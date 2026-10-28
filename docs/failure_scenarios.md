# SmartDialer Failure Scenarios & Resilience Matrix

This document provides a comprehensive analysis of 10 real-world failure scenarios in distributed outbound dialing systems, detailing how SmartDialer detects, isolates, mitigates, and recovers from each failure.

---

## Resilience Summary Matrix

| # | Failure Scenario | Detection Mechanism | Blast Radius | System Behavior | Self-Healing / Recovery |
| :-: | :--- | :--- | :--- | :--- | :--- |
| **1** | **Provider Latency Spikes** | Request timer > 5.0s / timeout | Single call dispatch | Worker aborts, leaves outbox record for retry | Lease expires; retry with exponential backoff |
| **2** | **Provider Outage / Hard Failure** | Consecutive error count >= threshold | Campaign calls targeting provider | Circuit breaker trips to `OPEN`; Safety Gate rejects calls | Cooldown timer expires; `HALF_OPEN` probe test |
| **3** | **Duplicate Webhook Events** | Unique constraint on `provider_event_inbox` | None | Duplicate rejected at DB layer; HTTP 200 returned | Immediate idempotent suppression |
| **4** | **Out-of-Order Webhooks** | Terminal state check in `EventProcessor` | Single call | Delayed event dropped; terminal state untouched | Invariant preserved; no state corruption |
| **5** | **Dispatch Worker Crash Mid-Flight** | Expired `lease_expires_at` in outbox | Unacknowledged batch | Call marked `PENDING`; agent remains held safely | `ReconciliationService` reclaims lease & re-dispatches |
| **6** | **Event Processor Crash Mid-Ingestion** | Uncommitted database transaction | Single event payload | Transaction rolled back; carrier retries webhook | Carrier delivers webhook again; processed cleanly |
| **7** | **Mass Agent Disconnection / Logout** | Agent heartbeat missing > 90s | Idle agents | Safety gate immediately detects drop in $A_{\text{avail}}$ | Safety gate clamps pacing; stale agents marked `OFFLINE` |
| **8** | **High Concurrency Allocator Race** | `FOR UPDATE SKIP LOCKED` / DB Unique Key | Multi-worker instances | Workers grab disjoint rows; zero duplicate allocations | Zero contention; instantaneous parallel execution |
| **9** | **Sudden Answer Rate Spike (Burst)** | EWMA tracking + Safety Ratio Clamp | Active campaign | Safety gate caps over-dialing; ratio clamp prevents overflow | Abandonment kept below 3% SLA |
| **10** | **Database Failure / Pool Exhaustion** | DB connection exception in Safety Gate | Entire dialing cluster | **Fails Closed**: Approved count set to 0 | Automatic reconnection when DB recovers |

---

## Detailed Scenario Walkthroughs

### Scenario 1: Telecom Provider Latency Spikes & Timeouts
* **Failure Description**: The upstream carrier experiences network degradation, causing call initiation requests to hang for >5 seconds.
* **Detection**: `DispatchWorker` uses an explicit timeout (`asyncio.wait_for(provider.initiate_call(), timeout=5.0)`).
* **Blast Radius**: Isolated solely to the asynchronous `DispatchWorker`. The core database allocation transaction is already committed, meaning database connection pools are unaffected.
* **Mitigation**: The worker catches the timeout exception, records a failure in `dispatch_records`, and notifies the circuit breaker.
* **Recovery**: The lease expires after 30 seconds; the `ReconciliationService` resets the dispatch record for retry up to `max_retries=3`.
* **Verification**: `tests/failure_injection/test_provider_failures.py::TestProviderFailureInjection::test_provider_outage_handling`

---

### Scenario 2: Upstream Carrier Outage & Circuit Breaker Tripping
* **Failure Description**: Provider B undergoes an unannounced outage, returning HTTP 503 or network socket errors on all requests.
* **Detection**: `ProviderHealthService` sliding window accumulates consecutive failures. When threshold (5 failures) is crossed, the circuit transitions from `CLOSED` to `OPEN`.
* **Blast Radius**: All subsequent dialing requests targeting Provider B.
* **Mitigation**: `SafetyController` queries `circuit_breaker.get_snapshot()`. If `state == "OPEN"`, it immediately overrides the predictive recommendation and approves **0 calls**. No borrower accounts are burned, and no agents are stranded in `RESERVED`.
* **Recovery**: After 30 seconds cooldown, circuit moves to `HALF_OPEN`. A single probe call is permitted. If successful, circuit transitions back to `CLOSED`.
* **Verification**: `tests/failure_injection/test_provider_failures.py::TestProviderFailureInjection::test_circuit_breaker_tripping_on_repeated_failures`

---

### Scenario 3: Duplicate Webhook Events & Network Replays
* **Failure Description**: Due to mobile network handoffs or aggressive carrier webhook retry policies, the carrier delivers the exact same `ringing` or `answered` webhook 5 times concurrently.
* **Detection**: In `EventProcessor.process_raw()`, the query attempts to insert into `provider_event_inbox` with unique index `(provider_id, provider_event_id)`.
* **Blast Radius**: Zero.
* **Mitigation**: The first event succeeds and updates the call state. The subsequent 4 concurrent events encounter a database unique constraint violation, are caught, logged as duplicates, and returned as HTTP 200 to acknowledge the carrier.
* **Verification**: `tests/failure_injection/test_event_duplicates.py::TestEventDuplicatesAndOutOfOrder::test_concurrent_duplicate_events_only_one_processed`

---

### Scenario 4: Out-of-Order Webhook Delivery
* **Failure Description**: A borrower immediately rejects a call. The carrier sends `ringing`, followed by `failed`. Due to network packet re-routing, the `failed` webhook arrives at 12:00:01 and moves the call to `FAILED` and frees the agent. At 12:00:03, the delayed `ringing` webhook arrives.
* **Detection**: `EventProcessor` checks if `call.state` is in `TERMINAL_STATES = {"COMPLETED", "ABANDONED", "FAILED", "BUSY", "NO_ANSWER"}`.
* **Blast Radius**: Zero.
* **Mitigation**: The processor notes that the call is already terminal. The delayed `ringing` event is safely discarded. The agent remains in `AVAILABLE` for new calls.
* **Verification**: `tests/failure_injection/test_event_duplicates.py::TestEventDuplicatesAndOutOfOrder::test_event_out_of_order_skipped_without_corrupting_state`

---

### Scenario 5: Dispatch Worker Process Crash Mid-Flight
* **Failure Description**: A container host running the `DispatchWorker` runs out of memory (OOM) or is terminated by Kubernetes while dispatching an allocated batch of 10 calls.
* **Detection**: Each dispatch claim has a finite lease: `lease_expires_at = now() + 30s`.
* **Blast Radius**: The 10 unacknowledged calls in the batch.
* **Mitigation**: The database transactional outbox records preserve the state.
* **Recovery**: The background `ReconciliationService` scans for records where `state = 'DISPATCHING'` AND `lease_expires_at < now()`. It resets their state to `PENDING` and increments `retry_count`. A surviving dispatch worker claims the records and completes the dispatch.
* **Verification**: `tests/failure_injection/test_worker_crash.py::TestWorkerCrashRecovery::test_reclaims_stale_dispatch_lease`

---

### Scenario 6: Event Processor Crash During Webhook Ingestion
* **Failure Description**: The API server handling carrier webhooks is terminated during the execution of `EventProcessor.process_raw()`.
* **Detection**: Uncommitted database transaction.
* **Blast Radius**: The single uncommitted event.
* **Mitigation**: Standard relational ACID rollback. The carrier receives an HTTP 500 / TCP reset.
* **Recovery**: Standard carrier webhook retry protocols re-deliver the event within 10–60 seconds, which is then processed cleanly.

---

### Scenario 7: Sudden Mass Agent Logout / Disconnection
* **Failure Description**: A call center floor experiences an internet drop, causing 50 agents to suddenly go offline simultaneously while the predictive pacer had assumed high available staffing.
* **Detection**: `SafetyController` performs an **authoritative DB re-read** at the moment of evaluation. It checks `SELECT count(*) FROM agents WHERE campaign_id=:id AND state='AVAILABLE'`.
* **Blast Radius**: Zero excess calls initiated.
* **Mitigation**: Even if the statistical pacer had recommended 100 calls based on past staffing, the Safety Controller detects $A_{\text{avail}} = 0$ and clamps the approved count to **0**.
* **Self-Healing**: `ReconciliationService` sweeps agents whose `last_heartbeat_at` is older than 90 seconds and transitions them from `AVAILABLE` to `OFFLINE`.
* **Verification**: `tests/unit/test_safety_controller.py::TestSafetyController::test_strict_mode_rejects_with_no_available_agents`

---

### Scenario 8: Concurrent Call Allocator Race Condition
* **Failure Description**: Ten worker nodes trigger dialing cycles at the exact same millisecond for the same campaign.
* **Detection**: PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED` and unique database constraint on `calls.idempotency_key`.
* **Blast Radius**: None.
* **Mitigation**:
  1. Each worker selects a mutually exclusive slice of available agents and queued borrowers.
  2. If two workers somehow attempt to generate a call for the same borrower on the same attempt, the unique constraint `calls.idempotency_key` rejects the second insert atomically.
* **Verification**: `tests/concurrency/test_call_allocator_concurrency.py::TestCallAllocatorConcurrency::test_concurrent_workers_no_over_allocation`

---

### Scenario 9: Sudden Answer Rate Spike / Burst Answering
* **Failure Description**: Answer rate jumps suddenly from 20% to 80% (e.g. during lunch hour), risking a spike in call abandonment because more calls are answered than there are available agents.
* **Detection**: `PredictivePacingEngine` tracks incoming outcomes via EWMA.
* **Mitigation**:
  1. As soon as answered calls start connecting, available agent capacity drops to zero ($A_{\text{avail}} \to 0$).
  2. `SafetyController` enforces a hard ratio clamp:
     $$\text{Approved} \le A_{\text{avail}} \times R_{\max}$$
     When $A_{\text{avail}} = 0$, `Approved` is strictly forced to 0.
  3. Pacing pauses until agents finish wrap-up, keeping abandonment within regulatory limits ($\le 3\%$).
* **Verification**: Simulation Scenario `burst_answering` (`scripts/run_simulation.py --scenario burst_answering`).

---

### Scenario 10: Database Connection Pool Exhaustion / Fail-Closed Gate
* **Failure Description**: Under extreme database load or network partition between application pods and PostgreSQL, database queries time out or raise connection pool errors.
* **Detection**: `try / except Exception` in `SafetyController.evaluate()`.
* **Blast Radius**: Prevents all unvalidated outbound dialing.
* **Mitigation**: **Fails Closed**. The safety controller catches the exception, logs `safety_evaluation_failed_fail_closed`, and returns:
  ```python
  SafetyEvaluationResult(
      decision=SafetyDecision.REJECT,
      approved_count=0,
      reason="FAIL_CLOSED: Database error during safety check"
  )
  ```
  The dialer never assumes availability in the absence of authoritative confirmation.
* **Verification**: `tests/unit/test_safety_controller.py::TestSafetyController::test_fails_closed_on_db_error`
