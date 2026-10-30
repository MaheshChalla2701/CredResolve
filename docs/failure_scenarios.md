# Failure Scenarios

What happens in each failure, which code handles it, and which test covers it.
All tests use mock or in-memory providers only.

| # | Failure | Handling | Covered by |
|---|---|---|---|
| 1 | Provider rejects the call | Dispatch `FAILED`; call `FAILED`; agent `AVAILABLE`; borrower requeued or `EXHAUSTED` | `tests/failure_injection/test_dispatch_recovery.py` |
| 2 | Provider times out (may or may not have accepted) | Dispatch `AMBIGUOUS`, call flagged; reconciliation asks the provider by idempotency key: not placed → `FAILED` and resources released; accepted → `provider_call_id` recorded | `test_dispatch_recovery.py` (timeout, timeout-after-accept) |
| 3 | Worker dies after claiming, before sending | Record stays `CLAIMED`; re-claimed after the lease expires and sent exactly once | `test_dispatch_recovery.py::test_expired_claim_is_redispatched_exactly_once` |
| 4 | Worker dies after `SENT`, before recording the outcome | Never re-sent; lease expiry → `AMBIGUOUS` → resolved as in #2 | `test_dispatch_recovery.py::test_worker_crash_after_send_is_not_redispatched` |
| 5 | Provider outage / repeated failures | Circuit breaker opens at the threshold; Safety Controller rejects, pacer proposes 0; HALF_OPEN after the cooldown allows a few probe calls | `tests/unit/test_dialing_modes.py`, `test_provider_failure_endpoint.py`, simulation `provider_degradation` |
| 6 | Duplicate provider events | Unique inbox row; second delivery changes nothing | `tests/failure_injection/test_event_duplicates.py`, webhook replay test |
| 7 | Out-of-order events | Skip-ahead applied through implied steps; regressive and post-terminal events change nothing (late timestamps backfilled) | `tests/unit/test_call_outcomes.py` (fast-forward, late backfill, newer call untouched), `test_event_duplicates.py` |
| 8 | Event arrives before its `provider_call_id` is stored | Kept pending in the inbox, retried by the maintenance loop | `tests/unit/test_call_outcomes.py::test_event_arriving_before_provider_call_id_is_retried` |
| 9 | Outcome never arrives | After `STALE_CALL_SECONDS` reconciliation probes the provider; unknown call → finished locally | `test_reconcile_and_availability_races.py` (scenario helper) |
| 10 | Agent leaked (call finished, agent still reserved) | Reconciliation releases it | `tests/unit/test_reconciliation.py` |
| 11 | Agent heartbeat lost | `OFFLINE` if no active call; kept if on a call | `tests/unit/test_reconciliation.py` |
| 12 | Concurrent `/dial` requests on one campaign | Campaign-row lock; cap re-checked in the allocating transaction | `tests/concurrency/test_campaign_concurrency_cap.py`, `tests/postgres` |
| 13 | Agent paused/offline between approval and allocation | Not claimed; allocation falls short and the shortfall is recorded on the decision; no orphaned reservation | `test_reconcile_and_availability_races.py`, `tests/postgres` |
| 14 | Pausing an agent that is reserved or on a call | Refused (400); agent unchanged | `test_reconcile_and_availability_races.py` |
| 15 | Reconciliation and events finish the same call at once | Exactly one wins (row lock + conditional updates); agent and borrower match the winning outcome | `test_reconcile_and_availability_races.py`, `tests/postgres` |
| 16 | Deadlock / serialization failure (PostgreSQL) | Transaction rolled back and retried (up to 3 attempts) | `tests/unit/test_db_retry.py`, `tests/postgres::test_real_deadlock_is_detected_and_retried` |
| 17 | Database error during a safety decision | Fail closed: REJECT with `INTERNAL_ERROR`; audit write failure → REJECT with `AUDIT_PERSIST_FAILED` | `tests/unit/test_safety_controller.py`, `test_safety_accuracy.py` |
| 18 | Forged or replayed webhook | 401 without a valid per-provider HMAC signature and fresh timestamp; replays inside the window deduplicated | `tests/integration/test_webhook_auth.py` |
| 19 | Database schema missing at startup | API starts, logs `database_not_ready`, does not start workers | `tests/integration/test_startup.py` |

## Notes on the trickier cases

**Ambiguous dispatch (#2, #4).** The worker commits `SENT` before calling the
provider, so "was it sent?" is always answerable from the database. When the
provider's answer is unknown, the system never re-sends; it asks the provider
(`find_call_by_idempotency_key`) after `DISPATCH_LEASE_SECONDS`, with no
transaction open during the lookup.

**Reconciliation vs events (#15).** Both paths lock the call first, then the
dispatch record, agent and borrower. The second path finds the call terminal
and does nothing; the borrower settle is conditional on `ALLOCATED`, so it
happens once. A late event on a call already failed by reconciliation does
not touch the agent if the agent has moved on to another call.

**Agent availability (#13, #14).** On PostgreSQL the dialing cycle holds only
the campaign lock while deciding, so a pause commits immediately and the
allocator's `SKIP LOCKED` claim no longer sees that agent. If the allocator
already claimed the agent, the pause waits for the dial to commit and is then
refused because the agent is `RESERVED`.

## Not covered

* Real carrier behaviour (only mocks exist).
* Several API processes against one database (tested as concurrent sessions in
  one process).
* Network partitions between the API and PostgreSQL beyond what a rolled-back
  transaction covers.
