# Architecture Decision Records

Each record states what the code does today, why, and what it costs.

## ADR-001: The pacer advises, the Safety Controller decides

**Decision.** `PredictivePacingEngine` only proposes a dial count. Every
proposal — progressive or predictive — goes through `SafetyController.evaluate()`,
which reads fresh database state and returns APPROVE / REDUCE / REJECT. Only
`run_dialing_cycle()` creates calls, and only up to the approved count.

**Why.** A buggy or drifting estimator must not be able to exceed campaign
limits or dial through a failing provider. A test (`test_runaway_pacer_cannot_exceed_safety_limits`)
feeds a pacer that recommends 1,000 calls; the allocation stays within the cap.

**Consequences.** Every decision is persisted in `safety_decisions` with the
allocated count; an approval that cannot be audited is rejected. In the default
strict mode each call needs its own available agent, so predictive dialing
chooses how many free agents to use but never over-dials.

## ADR-002: Database locks, not a distributed lock service

**Decision.** Concurrency is controlled inside the database transaction:

* a write to the **campaign row** at the start of every dialing cycle and
  every allocation, serializing allocation per campaign so
  `max_concurrent_calls` is checked and enforced in the same transaction;
* `SELECT … FOR UPDATE SKIP LOCKED` to claim agents, borrowers and outbox
  records without blocking on rows another transaction holds;
* conditional (compare-and-set) updates on every state change.

**Alternatives.** Redis/Redlock or an in-process asyncio lock. Rejected: an
asyncio lock protects one process only; Redis adds infrastructure and a second
source of truth whose lock can expire while the database transaction is still
running.

**Consequences.** Allocation for one campaign is sequential (milliseconds per
cycle); different campaigns proceed in parallel. On SQLite, `FOR UPDATE` is
ignored and the single writer plus compare-and-set updates provide the same
outcomes; PostgreSQL-specific behaviour is covered by `tests/postgres`.

## ADR-003: Transactional outbox; commit "sent" before calling the provider

**Decision.** The allocator writes a `dispatch_records` row in the same
transaction as the call. The dispatch worker claims it with a lease, commits
dispatch `SENT` and call `INITIATED`, and only then calls the provider, outside
any transaction.

**Why.** A crash can then never cause a duplicate call: a record still
`CLAIMED` provably never reached the provider and may be re-claimed; a `SENT`
record with an expired lease is marked ambiguous and resolved by asking the
provider whether it accepted the idempotency key — never re-sent.

**Consequences.** A call can sit in `INITIATED` + `is_ambiguous` until
reconciliation resolves it (after `DISPATCH_LEASE_SECONDS`).

## ADR-004: Idempotent event inbox

**Decision.** Every provider event is inserted into `provider_event_inbox`
(unique `provider_id + provider_event_id`) in the same transaction that applies
it. Duplicates are ignored, skip-ahead events are fast-forwarded through valid
transitions, regressive events and events on terminal calls change nothing,
and events that arrive before their `provider_call_id` is stored are retried.

**Consequences.** Event delivery can be at-least-once and unordered (mock
provider B duplicates and reorders deliberately).

## ADR-005: Background runtime inside the API process

**Decision.** `DialerRuntime` (dispatch loop, one event consumer per provider,
maintenance loop) starts and stops with the FastAPI lifespan.

**Why.** One command runs the whole prototype, and the in-memory mock providers
deliver events to the process that placed the call.

**Consequences.** In production the dispatch worker would be a separate process
(the outbox leases already allow several) and real providers would call the
signed webhook. Not tested as separate processes.

## ADR-006: Short transactions, one lock order, retry on deadlock

**Decision.** All write paths lock rows in the order campaign → call →
dispatch → agent → borrower → provider health. Reconciliation repairs one item
per transaction and never holds a transaction during a provider lookup.
Deadlocks (`40P01`) and serialization failures (`40001`) are rolled back and
retried (`app/persistence/retry.py`).

**Why.** Earlier, reconciliation ran all seven steps in one transaction,
holding locks across provider calls; on PostgreSQL a single failed statement
aborted the transaction and every later step. The dispatch worker also locked
dispatch-before-call in one step and call-before-dispatch in another.

**Consequences.** `tests/postgres` forces a real deadlock (one victim, retried
once) and races reconciliation against event processing on the same calls;
the SQLite suite runs the same race scenario.

## ADR-007: Two mock providers with failure injection and a circuit breaker

**Decision.** Provider A is fast and ordered; provider B is slow, times out,
duplicates and reorders events, and has failure bursts. Dispatch outcomes feed a
per-provider circuit breaker persisted in `provider_health`, read by the Safety
Controller and the pacer. `POST /simulation/provider-failure` injects failures.

## ADR-008: Shared-secret HMAC for provider webhooks

**Decision.** `POST /provider-events` requires an HMAC-SHA256 signature with a
per-provider secret when `PROVIDER_WEBHOOK_SECRETS` is set; without secrets it
is open in development/test and refused (503) in production.

**Why.** Webhook events change call, agent and borrower state; a forged
`completed` would mark a borrower contacted. A per-provider secret also stops
one provider from forging another's events. No extra infrastructure needed.

**Consequences.** TLS, secret rotation and the rest of the API's
authentication are out of scope for this prototype.

## ADR-009: SQLite by default, PostgreSQL for concurrency

**Decision.** Local setup and the test suite use SQLite files; PostgreSQL is
optional (`docker compose`, Alembic migrations). Locking-dependent behaviour is
tested separately against PostgreSQL.

**Consequences.** `scripts/init_db.py` creates SQLite schemas with
`create_all` (straight from the models); PostgreSQL uses `alembic upgrade
head`, and `tests/postgres` checks the migrated schema matches the models.
