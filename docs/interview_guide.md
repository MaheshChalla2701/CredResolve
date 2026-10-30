# SmartDialer Reviewer Guide

A short tour for reviewing the project: how to run it, what to look at, and
the questions it is designed to answer. Setup details are in the README.

## 1. Commands (macOS, from the repository root)

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]" -c constraints.txt
cp .env.example .env && python scripts/init_db.py

pytest                                                    # SQLite suite (PostgreSQL tests skip)
python -m uvicorn app.main:app --port 8000                # API + background workers
python scripts/demo_api_workflow.py                       # second terminal: one mock call end to end
python scripts/demo_api_workflow.py --mode predictive --provider mock_provider_b --rounds 3
python scripts/run_simulation.py --scenario medium_answer_rate --mode both --agents 5 --borrowers 20
python scripts/run_simulation.py --delay-scale 0.2        # all 7 scenarios, both modes
python scripts/run_load_test.py --mode dial --agents 20 --max-concurrent 5
```

PostgreSQL checks (Docker): see README, "PostgreSQL".

## 2. Five things worth reading

1. **The safety boundary** — `app/services/dialing.py` → dialers →
   `SafetyController.evaluate()` → `CallAllocator`. The pacer only proposes; the
   allocator re-checks campaign ACTIVE and `max_concurrent_calls` under the
   campaign lock; every decision is audited with the allocated count.
2. **No duplicate provider calls** — `app/workers/dispatch_worker.py`: `SENT`
   is committed before `initiate_call()`; only `CLAIMED` records are re-claimed;
   unknown outcomes are resolved by idempotency-key lookup.
3. **One place for call outcomes** — `app/services/call_outcomes.py`:
   contacted → agent `WRAP_UP`, borrower `COMPLETED`; not contacted → agent
   `AVAILABLE`, borrower requeued with cooldown or `EXHAUSTED`.
4. **Event handling** — `app/services/event_processor.py`: idempotent inbox,
   skip-ahead fast-forward, regressive/late events ignored, early events retried.
5. **Transactions** — `app/persistence/retry.py` (lock order and deadlock
   retry) and `app/services/reconciliation.py` (one repair per transaction, no
   transaction during provider lookups).

## 3. Likely questions

**Why database locks instead of Redis or an in-process lock?**
The invariants (one call per agent, cap per campaign, one settle per borrower)
live in the database rows being changed, so they are enforced in the same
transaction: a campaign-row lock for the cap, `FOR UPDATE SKIP LOCKED` to claim
agents/borrowers/outbox records, conditional updates for every state change.
An asyncio lock only covers one process; Redis would add a second source of
truth whose lock can expire mid-transaction.

**Can predictive mode cause abandoned calls?**
No. Every call reserves an available agent before it is dialed, so a borrower
who answers always has an agent. The trade-off is that predictive mode cannot
over-dial: it chooses how many free agents to use (all of them when few calls
answer, fewer near the utilization target, half at cold start, fewer when the
provider is degraded, none when its circuit is open). There is no abandonment
metric to violate because abandonment cannot occur in this design.

**The provider accepts a call and the server crashes before saving the response?**
The worker already committed `SENT` and `INITIATED`. After the lease expires,
reconciliation marks the call ambiguous and asks the provider whether it knows
the idempotency key: yes → the `provider_call_id` is recorded and events
continue; no → the call fails and the agent and borrower are released. The
request is never re-sent automatically.

**Two `/dial` requests for the same campaign at the same time?**
The second waits on the campaign row until the first commits, then sees the
first one's calls. Tests on SQLite and PostgreSQL show the cap holds; with the
lock disabled, they fail (12 calls against a cap of 4).

**An agent is paused right after the safety approval?**
The allocator's claim skips the paused agent; fewer calls are allocated and the
safety decision records `ALLOCATION_SHORTFALL:NO_AVAILABLE_AGENT`. An agent
that is already reserved or on a call cannot be paused (400).

**Deadlocks?**
All write paths lock in the same order (call → dispatch → agent → borrower →
provider health). If PostgreSQL still aborts a transaction as a deadlock victim
or for a serialization failure, the unit of work is rolled back and retried.

**How are webhooks protected?**
Per-provider HMAC-SHA256 signatures with a 5-minute timestamp window;
replays inside the window are absorbed by the idempotent inbox. Without
configured secrets the endpoint is open only outside production.

## 4. Honest limitations

* Mock providers only; workers run inside the API process.
* PostgreSQL concurrency is tested with concurrent sessions in one process, not
  several API processes.
* The rest of the API is unauthenticated (prototype).
* The pacer does not use call durations or setup latency (not usable without
  dialing ahead of agent availability, which this design forbids).
* One simulation run in earlier testing took about 6 minutes instead of about
  22 seconds; it has not recurred in more than 50 instrumented runs and its
  cause is unknown.

## 5. If this had to scale

Run dispatch workers as separate processes against PostgreSQL (the outbox
leases already allow several); put the API behind TLS with real provider
webhooks; add partitioning or a counter column if per-campaign allocation
serialization became a bottleneck. A message broker (e.g. Kafka) or Redis
would only be worth adding for a measured need — for example webhook ingestion
outpacing database writes — which this prototype does not show.
