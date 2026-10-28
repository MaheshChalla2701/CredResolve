# SmartDialer State Machines & Lifecycle Transitions

State machines in SmartDialer are modeled as explicit, deterministic transitions enforced at both the application domain layer (`app/domain/state_machines.py`) and the persistence layer. Any invalid or illegal state transition raises a structured `InvalidStateTransitionError` and is prevented from corrupting database state.

---

## 1. Call State Machine

A call represents a specific attempt to connect an available agent with a queued borrower through a telecom carrier.

### 1.1 State Diagram

```mermaid
stateDiagram-v2
    [*] --> PENDING: Allocation initiated
    PENDING --> RESERVED: Agent & Borrower locked
    RESERVED --> INITIATED: Dispatched to provider
    RESERVED --> FAILED: Dispatch failure / Lease expired
    
    INITIATED --> RINGING: Carrier signaling received
    INITIATED --> FAILED: Provider rejected / Timeout
    
    RINGING --> ANSWERED: Borrower picked up
    RINGING --> NO_ANSWER: Ring timeout
    RINGING --> BUSY: Line busy signal
    RINGING --> FAILED: Network error
    
    ANSWERED --> CONNECTED: Audio bridge established with agent
    ANSWERED --> ABANDONED: No agent available within 2 seconds
    
    CONNECTED --> COMPLETED: Call terminated normally
    CONNECTED --> FAILED: Mid-call line dropped
    
    COMPLETED --> [*]
    ABANDONED --> [*]
    FAILED --> [*]
    BUSY --> [*]
    NO_ANSWER --> [*]
```

### 1.2 Call State Transitions Reference

| From State | To State | Trigger | Preconditions | Side Effects |
| :--- | :--- | :--- | :--- | :--- |
| `PENDING` | `RESERVED` | `CallAllocator.allocate()` | Agent and Borrower both locked | Sets `agent_id`, `borrower_id`, `idempotency_key` |
| `RESERVED` | `INITIATED` | Provider accepts call | Provider returned `provider_call_id` | Stores `provider_call_id`, updates `dispatch_records` |
| `RESERVED` | `FAILED` | Carrier rejects or timeout | Outbox failure | Frees reserved agent to `AVAILABLE`, releases borrower |
| `INITIATED` | `RINGING` | Carrier `ringing` webhook | Call is currently `INITIATED` | Updates `telemetry` |
| `INITIATED` | `FAILED` | Carrier error webhook | Call is currently `INITIATED` | Releases agent and reschedules borrower |
| `RINGING` | `ANSWERED` | Carrier `answered` webhook | Call is currently `RINGING` | Starts 2-second SLA timer to establish agent audio bridge |
| `RINGING` | `NO_ANSWER` | Carrier `no_answer` event | Borrower did not pick up | Releases agent; puts borrower into retry cooldown |
| `RINGING` | `BUSY` | Carrier `busy` event | Number busy | Releases agent; puts borrower into retry cooldown |
| `RINGING` | `FAILED` | Carrier disconnect | Network drop | Releases agent; checks retry eligibility |
| `ANSWERED` | `CONNECTED` | Audio bridge established | Agent is reserved and ready | Agent moves to `TALKING`, `connected_at` timestamp set |
| `ANSWERED` | `ABANDONED` | SLA breached (>2s no agent) | No agent available to bridge | Invariant tracked: `calls_abandoned += 1`, borrower freed |
| `CONNECTED` | `COMPLETED` | Either party hangs up | Call was in conversation | Agent moves to `WRAP_UP`; borrower status marked `COMPLETED` |
| `CONNECTED` | `FAILED` | Abrupt network disconnect | Call in conversation | Agent moves to `WRAP_UP`; sets error code |

### 1.3 Terminal State Rule
* **States**: `COMPLETED`, `ABANDONED`, `FAILED`, `BUSY`, `NO_ANSWER`.
* **Invariant**: Once a call enters a terminal state, **no further transitions are allowed**. Any incoming delayed webhooks (e.g., an out-of-order `ringing` or delayed `answered` event arriving after `completed`) are dropped and flagged as stale by `EventProcessor`.

---

## 2. Agent State Machine

The Agent state machine models human collectors ready to receive calls.

### 2.1 State Diagram

```mermaid
stateDiagram-v2
    [*] --> OFFLINE: Agent registered
    OFFLINE --> AVAILABLE: Login / Resume
    AVAILABLE --> RESERVED: Call allocated by Allocator
    AVAILABLE --> PAUSED: Agent pause requested
    AVAILABLE --> OFFLINE: Agent logout / Heartbeat timeout
    
    RESERVED --> TALKING: Call connected (audio bridge established)
    RESERVED --> AVAILABLE: Call failed before connect / Canceled
    
    TALKING --> WRAP_UP: Call hung up (normal or error)
    
    WRAP_UP --> AVAILABLE: Wrap-up timer expires / Agent clicks done
    WRAP_UP --> PAUSED: Wrap-up completes with pause queued
    WRAP_UP --> OFFLINE: Wrap-up completes with logout queued
    
    PAUSED --> AVAILABLE: Unpause requested
    PAUSED --> OFFLINE: Logout requested
```

### 2.2 Agent State Transitions Reference

| From State | To State | Trigger | Description |
| :--- | :--- | :--- | :--- |
| `OFFLINE` | `AVAILABLE` | Agent Login API | Agent enters the pool and is eligible for call allocation. |
| `AVAILABLE` | `RESERVED` | Allocator locks agent | Agent is matched with a borrower and waiting for carrier connection. |
| `AVAILABLE` | `PAUSED` | Agent UI "Pause" | Agent takes a break. Excluded from dialing capacity calculations. |
| `AVAILABLE` | `OFFLINE` | Logout / Heartbeat missing | Agent logs out or missed 3 consecutive heartbeat cycles. |
| `RESERVED` | `TALKING` | Call `CONNECTED` | Agent is connected to the borrower and talking. |
| `RESERVED` | `AVAILABLE` | Call canceled / failed | Carrier failed to ring or answer; agent immediately returns to pool. |
| `TALKING` | `WRAP_UP` | Call `COMPLETED` | Call ends; agent enters post-call notes / disposition period. |
| `WRAP_UP` | `AVAILABLE` | Wrap-up timeout / Done | Wrap-up duration expires (reconciliation automatically cleans up). |
| `PAUSED` | `AVAILABLE` | Agent UI "Resume" | Agent returns from break. |

---

## 3. Borrower State Machine

Borrowers represent delinquent accounts scheduled for contact under regulatory limits.

### 3.1 State Diagram

```mermaid
stateDiagram-v2
    [*] --> QUEUED: Borrower imported into campaign
    QUEUED --> RESERVED: Locked by Call Allocator
    
    RESERVED --> IN_CALL: Provider accepted call
    RESERVED --> QUEUED: Allocation aborted before dispatch
    
    IN_CALL --> COMPLETED: Borrower contacted & settled / promise to pay
    IN_CALL --> COOLDOWN: No answer / Busy / Network failure (attempts < max)
    IN_CALL --> EXHAUSTED: Attempts reached max_attempts
    
    COOLDOWN --> QUEUED: Cooldown timer expires (re-eligible for dial)
    
    COMPLETED --> [*]
    EXHAUSTED --> [*]
```

### 3.2 Borrower State Invariants
* **Max Attempts**: Borrower cannot exceed `campaign.max_attempts_per_borrower` (default: 3). Once reached, borrower transitions to `EXHAUSTED` and will never be selected by `CallAllocator`.
* **Cooldown Period**: Unsuccessful attempts enter `COOLDOWN` with `next_attempt_after = now() + campaign.retry_cooldown_seconds`. `CallAllocator` filters for `status='QUEUED'` AND `next_attempt_after <= now()`.
* **Contact Success**: Once a borrower call is successfully completed (`status='COMPLETED'`), the borrower account is closed for the active campaign.

---

## 4. Transactional Outbox State Machine (`dispatch_records`)

The outbox guarantees zero loss of external call initiations across process crashes.

### 4.1 State Diagram

```mermaid
stateDiagram-v2
    [*] --> PENDING: Inserted in allocation transaction
    PENDING --> DISPATCHING: Claimed by Dispatch Worker (lease acquired)
    
    DISPATCHING --> DISPATCHED: Provider HTTP 200 / accepted
    DISPATCHING --> FAILED: Provider rejected / Max retries exceeded
    DISPATCHING --> PENDING: Lease expired without carrier ACK (reclaimed by sweeper)
    
    DISPATCHED --> [*]
    FAILED --> [*]
```

### 4.2 Outbox Invariants
* **Lease Expiration**: When a worker claims a record, it sets `lease_expires_at = now() + 30s`. If the worker crashes, the `ReconciliationService` resets expired records to `PENDING` with incremented retry count.
* **Deterministic Deduplication**: The outbox record includes `idempotency_key` which is sent to the telecom provider. Re-dispatches do not cause duplicate dials at the telecom carrier.

---

## 5. Circuit Breaker State Machine (`provider_health`)

Protects the dialing engine against carrier degradation, high failure rates, and upstream outages.

### 5.1 State Diagram

```mermaid
stateDiagram-v2
    [*] --> CLOSED: Normal operation
    CLOSED --> OPEN: Failure threshold breached (e.g. 5 consecutive failures)
    
    OPEN --> HALF_OPEN: Cooldown timer elapsed (e.g. 30s)
    
    HALF_OPEN --> CLOSED: Probe request succeeds
    HALF_OPEN --> OPEN: Probe request fails
```

### 5.2 Circuit Breaker Rules
* **CLOSED**: Normal state. All call allocation requests for this provider are evaluated normally by `SafetyController`.
* **OPEN**: Carrier is failing. `SafetyController` immediately rejects all dialing requests targeting this provider (`approved_count = 0`), protecting borrower accounts and agent capacity.
* **HALF_OPEN**: Allows a single test request through. If successful, state resets to `CLOSED`; if unsuccessful, immediately re-trips to `OPEN`.
