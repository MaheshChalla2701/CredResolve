# SmartDialer

A prototype outbound dialer for collection campaigns. It reserves an agent and a
borrower for every call, asks an independent **Safety Controller** before any
call is created, and places calls through **mock telephony providers** (no real
calls are ever made). Provider events drive each call, agent and borrower to a
final state.

- **Progressive dialing** — one call per available agent.
- **Predictive pacing** — a rule-based engine (EWMA answer rate, provider health,
  in-flight calls) proposes a dial count; the Safety Controller decides.
- **Safety Controller** — approves, reduces or rejects every dialing request
  using fresh database state, campaign limits and the provider circuit breaker;
  every decision is audited with the number of calls actually allocated.
- **Atomic allocation** — agent + borrower + call + dispatch-outbox record in one
  transaction (`SELECT … FOR UPDATE SKIP LOCKED` on PostgreSQL), under a
  per-campaign lock that enforces `max_concurrent_calls`.
- **Dispatch worker** — drains the outbox, calls the mock provider outside any
  transaction, never re-sends a request whose outcome is unknown.
- **Idempotent event processing** — duplicate, out-of-order and early events.
- **Reconciliation** — wrap-up release, stale reservations, ambiguous dispatches,
  worker crashes, leaked agents.
- **Two mock providers** — A (fast, ordered) and B (slow, duplicate and
  out-of-order events, timeouts, failure bursts).
- **Simulation** of both modes, **tests**, and a basic allocation **load test**.

## How a call flows

```
POST /campaigns/{id}/dial
  └─ run_dialing_cycle()            locks the campaign row, then campaign.dialing_mode selects the dialer
       ├─ progressive: propose = min(available agents, eligible borrowers)
       └─ predictive:  PredictivePacingEngine proposes (advisory), capped by eligible borrowers
            └─ SafetyController.evaluate()      APPROVE / REDUCE / REJECT (audited)
                 └─ CallAllocator               re-checks ACTIVE + max_concurrent_calls, then
                                                agent RESERVED, borrower ALLOCATED,
                                                call RESERVED, dispatch PENDING
                 └─ record_allocation()         allocated count (+ shortfall reason) on the audit record
Background runtime (inside the API process, app/workers/runtime.py)
  ├─ DispatchWorker   PENDING → SENT, call INITIATED (committed first),
  │                   then MockProvider.initiate_call(), then SUCCEEDED/FAILED/AMBIGUOUS
  ├─ event consumer   provider events → EventProcessor → call/agent/borrower states
  └─ maintenance      every RECONCILIATION_INTERVAL_SECONDS: reconciliation
                      (WRAP_UP → AVAILABLE, stale/ambiguous work) + early-event retry
```

Final states (`app/services/call_outcomes.py`):

| Provider outcome | Call | Agent | Borrower |
|---|---|---|---|
| `completed` | `COMPLETED` | `WRAP_UP` → `AVAILABLE` after `wrap_up_seconds` | `COMPLETED` |
| `no_answer` / `busy` / `failed` | `FAILED` (`failure_reason` = outcome) | `AVAILABLE` immediately | `QUEUED` after `retry_cooldown_seconds`, or `EXHAUSTED` at the attempt limit |
| provider rejects the request | `FAILED` (`failure_reason` = error code) | `AVAILABLE` | retried as above |
| timeout / unknown outcome | stays `INITIATED`, `is_ambiguous` | held until resolved | held until resolved |

## Requirements

- macOS with Python **3.13** (`brew install python@3.13`)
- Optional: Docker Desktop, only for the PostgreSQL setup

## Quick start (macOS, SQLite)

```bash
cd CredResolve
python3.13 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]" -c constraints.txt   # constraints.txt pins the verified versions

cp .env.example .env              # local settings: SQLite file ./smartdialer.db
python scripts/init_db.py         # create the schema in that database
python scripts/seed_demo.py       # optional: two demo campaigns with agents/borrowers

python -m uvicorn app.main:app --port 8000
```

The API, `init_db.py` and `seed_demo.py` all read `DATABASE_URL` from `.env`, so
they use the same database. On startup the log shows `dialer_runtime_started`;
if the schema is missing it logs `database_not_ready` and does not start the
workers. Interactive docs: http://localhost:8000/docs

### See a mock call end to end

With the server running, in a second terminal (`source .venv/bin/activate`):

```bash
python scripts/demo_api_workflow.py                                   # progressive, mock provider A
python scripts/demo_api_workflow.py --mode predictive --provider mock_provider_b
python scripts/demo_api_workflow.py --rounds 3 --borrowers 4          # agent reuse and retries
```

It creates a campaign with two agents and synthetic borrowers, calls
`POST /campaigns/{id}/dial`, and prints each call's states as observed via
`GET /calls/{id}`, e.g. `RESERVED/PENDING → INITIATED/SUCCEEDED → RINGING → … → COMPLETED`.

The same with curl:

```bash
curl -s -X POST localhost:8000/api/v1/campaigns -H 'Content-Type: application/json' \
  -d '{"name":"Demo","dialing_mode":"progressive","wrap_up_seconds":2,"retry_cooldown_seconds":0}'
curl -s -X POST localhost:8000/api/v1/agents -H 'Content-Type: application/json' \
  -d '{"campaign_id":"<campaign_id>","name":"Agent-1"}'
curl -s -X POST localhost:8000/api/v1/agents/<agent_id>/availability -H 'Content-Type: application/json' \
  -d '{"state":"AVAILABLE"}'
curl -s -X POST localhost:8000/api/v1/borrowers -H 'Content-Type: application/json' \
  -d '{"campaign_id":"<campaign_id>","synthetic_name":"Test Borrower","masked_phone":"***-***-0001"}'
curl -s -X POST localhost:8000/api/v1/campaigns/<campaign_id>/start
curl -s -X POST "localhost:8000/api/v1/campaigns/<campaign_id>/dial?provider_id=mock_provider_a"
curl -s localhost:8000/api/v1/calls/<call_id>          # state, dispatch_status, provider_call_id
```

`/dial` returns `call_state: RESERVED` and `dispatch_status: PENDING`: the call
is queued, not yet placed. The dispatch worker sends it to the mock provider
within `DISPATCH_POLL_INTERVAL_SECONDS` (it is also woken immediately).

Useful settings in `.env`: `MOCK_PROVIDER_DELAY_SCALE=0.2` makes mock calls
finish ~5× faster; `RECONCILIATION_INTERVAL_SECONDS` controls how quickly
wrap-up agents are released; `BACKGROUND_WORKERS_ENABLED=false` runs the API
without workers.

## PostgreSQL (for concurrency guarantees)

SQLite is enough to run the prototype, but it ignores `FOR UPDATE SKIP LOCKED`.
Correctness on SQLite relies on conditional (compare-and-set) updates and
SQLite's single writer; the row-locking design for many concurrent workers is
only meaningful on PostgreSQL.

```bash
open -a Docker                                   # start Docker Desktop
docker compose up -d postgres
# in .env, switch DATABASE_URL / DATABASE_URL_ASYNC to the PostgreSQL lines
alembic upgrade head
python -m uvicorn app.main:app --port 8000
```

PostgreSQL concurrency tests and load test, on the throwaway `postgres_test`
service (tmpfs storage, port 5433):

```bash
docker compose up -d postgres_test
export PG_TEST=postgresql+asyncpg://smartdialer_test:smartdialer_test@localhost:5433/smartdialer_test
DATABASE_URL=postgresql+psycopg://smartdialer_test:smartdialer_test@localhost:5433/smartdialer_test alembic upgrade head
SMARTDIALER_TEST_POSTGRES_URL=$PG_TEST pytest tests/postgres -v
python scripts/run_load_test.py --mode dial --agents 20 --max-concurrent 5 --database-url $PG_TEST
docker compose rm -sf postgres_test
```

`tests/postgres` (skipped without `SMARTDIALER_TEST_POSTGRES_URL`) checks the
schema is at the Alembic head and matches the models, and runs truly parallel
dialing cycles (campaign cap), allocations (no double booking), dispatch claims
(`SKIP LOCKED`, disjoint), first use of a provider's health row, a forced
deadlock (retried), reconciliation racing event processing on the same calls,
and an agent paused by another transaction mid-dial.

## Tests

```bash
pytest                       # whole suite
pytest tests/unit -q         # fast unit tests
```

Every test uses its own temporary SQLite database file, created in a temporary
directory that is removed when the run ends, and only mock or in-memory
providers. The tests ignore your `.env` for `APP_ENV` and webhook secrets. Because the suite runs on SQLite, the
concurrency tests exercise the compare-and-set guards and the campaign lock but
**not** PostgreSQL row locking — see `tests/postgres` above for that.

## Provider webhooks

The mock providers deliver events in-process, so the demo does not need the
webhook. `POST /api/v1/provider-events` is the entry point a real provider
would call, and it can be authenticated with per-provider shared secrets:

```bash
# .env
PROVIDER_WEBHOOK_SECRETS={"mock_provider_a":"change-me-a","mock_provider_b":"change-me-b"}

# send a signed event by hand (call id from GET /api/v1/calls/{id})
PROVIDER_WEBHOOK_SECRET=change-me-a python scripts/send_provider_event.py \
  --provider mock_provider_a --provider-call-id pA-xxxxxxxx --event-type completed
```

Each request carries `X-SmartDialer-Timestamp` (unix seconds) and
`X-SmartDialer-Signature: v1=<hex>`, an HMAC-SHA256 with the provider's secret
over `timestamp \n METHOD \n path \n query string \n body`
(`app/api/webhook_auth.py`).

| Configuration | Behaviour |
|---|---|
| secrets set | unsigned, wrongly signed, other provider's secret, or timestamp older than `PROVIDER_WEBHOOK_MAX_SKEW_SECONDS` (300) → 401 |
| no secrets, `APP_ENV=development`/`test` | accepted unauthenticated; a warning is logged at startup |
| no secrets, `APP_ENV=production` | refused (503) |

Threat model: stops anyone who can reach the API from forging or altering
provider events (which change call, agent and borrower state — a forged
`completed` would mark a borrower as contacted), and stops one provider from
signing another's events. Replays inside the time window are absorbed by the
idempotent inbox. Not covered: transport security (run behind TLS), secret
rotation (change both sides together), and authentication of the rest of the
API, which this prototype does not have.

## Simulation

```bash
python scripts/run_simulation.py --scenario medium_answer_rate --mode both --agents 5 --borrowers 20
python scripts/run_simulation.py --delay-scale 0.2          # all 7 scenarios, both modes, faster
```

Each run uses a fresh SQLite file (`--db-path`, default `simulation.db`, deleted
afterwards), the same dialing cycle, dispatch worker, event processing and
reconciliation as the API, and its own mock provider instance. After the dialing
cycles it waits until every call is final (bounded by `--drain-timeout`). The
exit code is non-zero if any run times out, leaves calls unresolved or breaks an
invariant (agent with two calls, agent held by a finished call, borrower stuck
`ALLOCATED`). Utilisation is sampled talk time (`CONNECTED` agents ÷ staffed,
non-offline agents).

## Load test

```bash
python scripts/run_load_test.py --agents 100 --duration 15                       # direct allocation
python scripts/run_load_test.py --mode dial --agents 20 --max-concurrent 5       # full dialing cycles
```

`allocator` mode hammers `CallAllocator` directly; `dial` mode runs full
dialing cycles (Safety Controller + allocator) from many workers while a
finisher ends calls, keeping the campaign cap under pressure. Both check: no
agent or borrower double-booked, no duplicate idempotency key, active calls
never above `max_concurrent_calls` (sampled during the run), no decision with
more calls allocated than approved. No dispatch or provider events are
involved. The default backend is a temporary SQLite file — its numbers validate
the guards, not PostgreSQL throughput; pass `--database-url` for PostgreSQL.

## API overview

| Method & path | Purpose |
|---|---|
| `POST /api/v1/campaigns`, `GET /api/v1/campaigns/{id}` | create / read a campaign (`dialing_mode`: `progressive` or `predictive`) |
| `POST /api/v1/campaigns/{id}/start` / `pause` | lifecycle |
| `POST /api/v1/campaigns/{id}/dial?provider_id=` | run one dialing cycle in the campaign's mode |
| `GET /api/v1/campaigns/{id}/safety-decisions` | Safety Controller audit trail (requested / approved / allocated) |
| `POST /api/v1/agents`, `POST /api/v1/agents/{id}/availability` | register an agent, set `AVAILABLE` / `PAUSED` / `OFFLINE` |
| `POST /api/v1/borrowers`, `GET /api/v1/campaigns/{id}/borrowers` | synthetic borrowers |
| `GET /api/v1/calls/{id}`, `GET /api/v1/campaigns/{id}/calls` | call state, provider call id, dispatch status |
| `POST /api/v1/provider-events` | provider webhook (idempotent; HMAC-signed when `PROVIDER_WEBHOOK_SECRETS` is set) |
| `POST /api/v1/reconcile` | run reconciliation now (it also runs periodically) |
| `POST /api/v1/simulation/provider-failure?provider_id=&failure_count=` | inject failures into a provider's circuit breaker (opens at the threshold) |
| `GET /health`, `GET /ready`, `GET /metrics` | health and Prometheus metrics |

## Design notes

- **Workers run inside the API process.** One command starts everything, and
  the in-memory mock providers deliver events to the process that dispatched
  the call. In production the dispatch worker would be a separate process (the
  outbox and leases already support several) and real providers would call
  `POST /api/v1/provider-events`.
- **Short transactions, one lock order.** Every write path locks rows in the
  order campaign → call → dispatch record → agent → borrower → provider health
  (`app/persistence/retry.py`). Reconciliation repairs one item per
  transaction and never holds a transaction during a provider lookup.
  Deadlocks and serialization failures are rolled back and retried.
- **No duplicate provider calls.** A worker commits `SENT` + call `INITIATED`
  *before* contacting the provider. Only records still `CLAIMED` (provably never
  sent) are re-claimed after a lease expires. A `SENT` record whose worker died
  is flagged ambiguous; reconciliation asks the provider whether it accepted the
  idempotency key and then fails or completes the bookkeeping.
- **Campaign cap is enforced in the allocating transaction.** Every dialing
  cycle (and every direct `CallAllocator` call) first writes the campaign row:
  on PostgreSQL that is a row lock, on SQLite the write lock. Concurrent cycles
  for one campaign therefore run one after another, and the active-call count
  the Safety Controller and allocator read cannot change before commit.
  Trade-off: allocation is serialized per campaign (a cycle holds the lock for
  milliseconds); a counter column would avoid that but must be decremented
  exactly on every path that ends a call.
- **Safety decisions match allocations.** Dialers never propose more calls than
  there are eligible borrowers; after allocating they record the allocated
  count on the decision (`capacity_snapshot.allocated_count`, plus
  `ALLOCATION_SHORTFALL:<reason>` if short). A decision that cannot be written
  to the audit table becomes a `REJECT`. Cycles with nothing to propose (no
  agents, no borrowers, pacer says 0) never reach the Safety Controller and
  are not audited (`audited: false` in the `/dial` response).
- **Safety boundary.** `run_dialing_cycle()` is the only entry point that
  creates calls; a test fails if anything else in `app/` imports the
  allocator. The allocator itself refuses inactive campaigns and the
  concurrency cap, so even a direct call cannot break those. Circuit-breaker
  checks stay in the Safety Controller (the allocator does not depend on
  provider health).
- **Predictive mode cannot over-dial.** Every call reserves an `AVAILABLE`
  agent, so the pacer chooses how many of the free agents to use: all of them
  when few calls answer, fewer when most answer (utilization target), half at
  cold start (low-confidence estimate), scaled down when the provider is
  degraded, none when its circuit is open. Calls that already hold their own
  agent are not counted against free agents. Call durations and setup latency
  are not used: they only matter when dialing ahead for agents that are about
  to free up, which needs calls without a reserved agent — this design (and the
  strict Safety Controller) deliberately does not allow that.
- **Out-of-order events.** A skip-ahead event is applied through its implied
  steps (`connected` while `RINGING` → `ANSWERED` → `CONNECTED`); a regressive
  event is recorded and ignored; a late `answered`/`connected` on a finished call
  only fills the missing timestamp; an event that arrives before the call's
  `provider_call_id` is stored is kept and retried.

## Documentation

- [Architecture](docs/architecture.md)
- [State Machines](docs/state_machines.md)
- [Architecture Decisions](docs/architecture_decisions.md)
- [Failure Scenarios](docs/failure_scenarios.md)
- [Reviewer Guide](docs/interview_guide.md) — how to run and review the project
