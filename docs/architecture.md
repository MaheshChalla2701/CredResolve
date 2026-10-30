# SmartDialer Architecture

SmartDialer is a prototype outbound dialer for collection campaigns. It runs as
one FastAPI process with an in-process background runtime, a relational
database (SQLite for local use, PostgreSQL for concurrency guarantees) and two
**mock** telephony providers. No real calls are ever placed.

## 1. Components

```mermaid
flowchart LR
    Client -->|POST /campaigns/{id}/dial| Dial[run_dialing_cycle]
    Dial -->|lock campaign row| DB[(Database)]
    Dial --> Prog[ProgressiveDialer]
    Dial --> Pred[PredictiveDialer + PredictivePacingEngine]
    Prog --> Safety[SafetyController]
    Pred --> Safety
    Safety -->|approved count, audited| Alloc[CallAllocator]
    Alloc -->|agent, borrower, call, dispatch outbox| DB
    subgraph Runtime[Background runtime — same process]
        Worker[DispatchWorker per provider]
        Events[Event consumer per provider]
        Maint[Maintenance: reconciliation + early-event retry]
    end
    DB --> Worker -->|initiate_call, outside any transaction| Providers[Mock provider A / B]
    Providers -->|in-memory event queue| Events --> Proc[EventProcessor] --> DB
    Webhook[POST /provider-events, HMAC-signed] --> Proc
    Maint --> DB
```

| Component | File | Responsibility |
|---|---|---|
| Dialing entry point | `app/services/dialing.py` | Only path that creates calls. Locks the campaign row, picks the dialer by `campaign.dialing_mode`. |
| Progressive dialer | `app/services/progressive_dialer.py` | Proposes `min(available agents, eligible borrowers)`. |
| Predictive dialer / pacer | `app/services/predictive_dialer.py`, `predictive_pacer.py` | Pacer proposes a count from the observed answer rate, provider health and free agents; advisory only. |
| Safety Controller | `app/services/safety_controller.py` | Approves, reduces or rejects every proposal from fresh database state; every decision is written to `safety_decisions`. |
| Call allocator | `app/services/call_allocator.py` | Reserves agent + borrower, creates the call and its dispatch outbox record in one transaction; re-checks campaign ACTIVE and `max_concurrent_calls`. |
| Dispatch worker | `app/workers/dispatch_worker.py` | Drains the outbox; commits `SENT` + call `INITIATED` before contacting the provider; records the outcome. |
| Event processor | `app/services/event_processor.py` | Idempotent inbox; applies provider events to call, agent and borrower. |
| Call outcomes | `app/services/call_outcomes.py` | Single place that ends a call and settles its agent and borrower. |
| Reconciliation | `app/services/reconciliation.py` | Repairs stale reservations, lost dispatches, ambiguous outcomes, leaked agents, wrap-up timeouts. |
| Provider health | `app/services/provider_health.py` | Circuit breaker (CLOSED / OPEN / HALF_OPEN) persisted in `provider_health`. |
| Runtime | `app/workers/runtime.py` | Starts/stops the dispatch, event and maintenance loops with the API (FastAPI lifespan). |
| Webhook auth | `app/api/webhook_auth.py` | Per-provider HMAC-SHA256 signatures for `POST /provider-events`. |

## 2. A call, end to end

1. `POST /api/v1/campaigns/{id}/dial` → `run_dialing_cycle()` writes the
   campaign row first (PostgreSQL row lock / SQLite write lock), so dialing
   cycles for one campaign run one after another.
2. The dialer proposes a count (never more than eligible borrowers). The
   Safety Controller decides: campaign ACTIVE, concurrency headroom, available
   agents (strict 1:1 mode by default), circuit breaker (OPEN → reject,
   HALF_OPEN → at most `circuit_breaker_half_open_probes`). The decision is
   persisted; if it cannot be persisted the request is rejected.
3. The allocator reserves up to the approved count. Each reservation: campaign
   still ACTIVE and below `max_concurrent_calls` → claim an `AVAILABLE` agent
   and an eligible `QUEUED` borrower (`FOR UPDATE SKIP LOCKED`) → call
   `RESERVED`, agent `RESERVED`, borrower `ALLOCATED` (+1 attempt), dispatch
   record `PENDING`. The allocated count (and any shortfall reason) is written
   back onto the safety decision.
4. `/dial` commits and returns `call_state: RESERVED`, `dispatch_status: PENDING`.
5. The dispatch worker claims the record (lease), commits dispatch `SENT` and
   call `INITIATED`, then calls the provider outside any transaction. Accepted →
   `provider_call_id` stored, dispatch `SUCCEEDED`. Rejected → call `FAILED`,
   agent freed, borrower requeued. Timeout/unknown → `AMBIGUOUS`, resolved by
   reconciliation by asking the provider about the idempotency key.
6. Provider events (`ringing`, `answered`, `connected`, `completed`,
   `no_answer`, `busy`, `failed`, `cancelled`) arrive on the mock providers'
   in-memory queues (or the webhook) and drive the call to a final state.
7. Final state settles the agent (`WRAP_UP` if the borrower was reached, else
   `AVAILABLE`) and the borrower (`COMPLETED`, or `QUEUED` after the retry
   cooldown, or `EXHAUSTED` at the attempt limit). Reconciliation releases
   `WRAP_UP` agents after the campaign's `wrap_up_seconds`.

## 3. Transactions and locking

* Every write path locks rows in one global order:
  **campaign → call → dispatch record → agent → borrower → provider health**
  (documented in `app/persistence/retry.py`). Allocation starts at the campaign
  row and claims agents/borrowers with `SKIP LOCKED`, so it never waits on them.
  Agent-only repairs (wrap-up release, leaked agents, availability changes) lock
  only the agent.
* No transaction is held across a provider call: the dispatch worker commits
  before `initiate_call()`, and reconciliation performs provider lookups with
  no transaction open.
* Reconciliation repairs one item per short transaction; a failure affects
  only that item.
* Deadlocks (`40P01`) and serialization failures (`40001`) roll back and are
  retried as a whole (`run_transaction`, up to 3 attempts) in reconciliation,
  the dispatch worker and the webhook; the event consumer retries each event up
  to `EVENT_MAX_RETRIES`. Other errors roll back and are logged or returned —
  never ignored.
* The Safety Controller reads provider health without a lock and locks the row
  only to move an expired OPEN circuit to HALF_OPEN.

## 4. Data model (tables)

`campaigns`, `agents`, `borrowers`, `calls` (unique `idempotency_key`,
`provider_call_id`), `dispatch_records` (outbox: status, lease, worker),
`provider_event_inbox` (unique `provider_id + provider_event_id`),
`provider_health`, `safety_decisions` (requested / approved, reasons, capacity
snapshot incl. `allocated_count`), `audit_events`, `simulation_results`.
SQLite schemas are created by `scripts/init_db.py`; PostgreSQL by
`alembic upgrade head` (migrations `001`, `002`).

## 5. Security

* Provider webhooks: per-provider HMAC-SHA256 over timestamp, method, path,
  query and body; 5-minute timestamp window; replays inside the window are
  absorbed by the idempotent inbox. Without secrets, webhooks are accepted only
  outside production (see README, "Provider webhooks").
* The rest of the API has **no authentication** — it is a prototype meant to
  run locally. Phone numbers are synthetic and masked.

## 6. Known limitations

* Workers run inside the API process; several API processes against one
  PostgreSQL database are supported by the locking design but were only tested
  as concurrent sessions, not separate processes.
* Allocation is serialized per campaign (milliseconds per cycle).
* Predictive mode never over-dials: every call reserves an available agent.
  Call durations and setup latency are not used by the pacer.
* Agents cannot be paused or taken offline while reserved or on a call (the
  request is refused with 400).
