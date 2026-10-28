# Architecture Decision Records (ADRs)

This document records the architectural choices, design trade-offs, and rationale behind the implementation of SmartDialer.

---

## ADR-001: Separation of Pacing Logic (Advisory) from Safety Controller (Authoritative)

### Status
Accepted & Implemented

### Context
In outbound dialing platforms, predictive algorithms estimate answer rates, agent talk durations, and queue dynamics to determine how many calls to place simultaneously. However, mathematical models can experience sudden estimation drift due to external anomalies (e.g. carrier routing changes, voicemail spikes, or agent churn). In high-volume collections, over-dialing can cause illegal call abandonment (violating the FTC/FCC 3% abandonment rule) or duplicate customer harassment.

### Decision
We strictly enforce a dual-gate architecture:
1. **Predictive Pacer is purely advisory**: It takes statistical estimates (EWMA answer rate, historical talk durations) and outputs an *unvalidated recommendation* for number of calls. It has **no write access** to the database and cannot initiate calls with telecom carriers.
2. **Safety Controller is authoritative**: It executes a fresh, isolated database read of real agent availability, active campaign concurrency, and circuit breaker health. It has the final, absolute authority to approve, reduce, or reject the recommendation.

### Alternatives Considered
* *Integrated Pacing & Execution*: The dialer calculates the pacing and directly allocates agents.
  * *Rejected*: Pacing bugs directly result in regulatory violations, double allocations, or unhandled concurrency spikes. Separation creates a clean blast-radius wall.
* *Rule-based Pacer without Safety Gate*: Adding max caps inside the pacer itself.
  * *Rejected*: Pacer uses cached or in-memory estimates that can diverge from the authoritative database state under multi-worker concurrency.

### Consequences
* **Positive**: The system cannot over-dial or violate campaign limits even if the predictive algorithm is buggy or outputs extreme values ($1000\times$).
* **Positive**: Tests can verify the safety gate in total isolation from statistical pacing models.
* **Trade-off**: Requires an additional lightweight database read per dialing cycle before allocation.

---

## ADR-002: Row-Level Locking (`FOR UPDATE SKIP LOCKED`) vs Distributed Locks

### Status
Accepted & Implemented

### Context
When multiple workers or cron tasks run concurrently to allocate calls, two workers might attempt to claim the same idle agent or select the same high-priority borrower at the same sub-second instant. We need a concurrency control mechanism that guarantees:
1. No two calls are assigned to the same agent.
2. No two calls are initiated to the same borrower.
3. High throughput without lock contention or deadlocks.

### Decision
We use PostgreSQL's native row-level pessimistic locking via `SELECT ... WHERE state='AVAILABLE' FOR UPDATE SKIP LOCKED LIMIT K` for both agents and borrowers. In SQLite (used for lightweight standalone simulations and test suites), we utilize serialized transactional boundaries with short backoff retries.

### Alternatives Considered
* *Distributed Locks (e.g. Redis Redlock)*:
  * *Rejected*: Distributed locks introduce an external infrastructure dependency, split-brain failure modes during network partitions, and clock drift vulnerabilities. Furthermore, locking individual borrower IDs in Redis requires round-trips for every candidate borrower.
* *Optimistic Concurrency Control (Version / Timestamp column)*:
  * *Rejected*: Under high worker concurrency, optimistic concurrency suffers from massive retry storms and high transaction abort rates when multiple workers compete for the top-priority borrowers.

### Consequences
* **Positive**: `SKIP LOCKED` completely eliminates worker contention. If Worker 1 is claiming Agent 1, Worker 2 seamlessly skips Agent 1 and claims Agent 2 with zero wait time and zero deadlocks.
* **Positive**: The reservation of the agent, the reservation of the borrower, the insertion of the call record, and the insertion of the outbox dispatch record occur in a single atomic ACID transaction.
* **Positive**: If a worker node crashes mid-transaction, PostgreSQL immediately rolls back the lock, releasing the agent and borrower back to the pool without orphaned locks.

---

## ADR-003: Transactional Outbox Pattern for Telecom Provider Dispatches

### Status
Accepted & Implemented

### Context
When a call is allocated, the system must notify the external telecom provider (SIP trunk / WebRTC gateway / Twilio / Plivo) to initiate the phone call. If the application makes an HTTP/RPC network request directly inside the database transaction:
1. Network timeouts hold database connection pool slots and locks open for seconds.
2. If the database crashes after the telecom provider accepted the call, the call exists in the carrier network but is lost in the database (ghost call).
3. If the network call times out, the application cannot know if the carrier actually initiated the call or not (dual-write dilemma).

### Decision
We implement the **Transactional Outbox Pattern**:
1. When `CallAllocator` reserves an agent and borrower, it inserts a `DispatchRecord` row into the `dispatch_records` table with state `PENDING` within the *same* database transaction.
2. An asynchronous background `DispatchWorker` polls pending dispatch records using lease-based claiming (`UPDATE ... SET state='DISPATCHING', lease_expires_at=now()+30s`).
3. The worker communicates with the telecom provider outside of any database transaction.
4. Upon confirmation from the carrier, the worker updates the outbox state to `DISPATCHED` and the call state to `INITIATED`.

### Alternatives Considered
* *Direct Synchronous Call within Allocation Request*:
  * *Rejected*: Carrier network latency directly degrades allocation latency and exhausts database connection pools.

### Consequences
* **Positive**: Guarantees zero lost call dispatches.
* **Positive**: If the application crashes during dispatch, the lease expires and another worker reclaims the dispatch record.
* **Positive**: Total isolation between database transactions and carrier latency.
* **Trade-off**: Introduces a negligible asynchronous dispatch latency (typically 5–20ms).

---

## ADR-004: Idempotent Event Inbox (`provider_event_inbox`) with Terminal State Protection

### Status
Accepted & Implemented

### Context
Telecom carriers deliver asynchronous webhooks as calls progress through their lifecycle (`ringing`, `answered`, `connected`, `completed`, `failed`). Real-world telecom carriers frequently exhibit two failure behaviors:
1. **Duplicate Webhook Deliveries**: The carrier retries webhooks on high network latency.
2. **Out-of-Order Deliveries**: An event notifying `completed` or `failed` may arrive *before* the `ringing` or `answered` event due to network routing anomalies.

### Decision
We implement a two-tier defense:
1. **Idempotent Event Inbox Table (`provider_event_inbox`)**:
   * Every incoming webhook is recorded with a unique constraint on `(provider_id, provider_event_id)`.
   * If a duplicate event arrives, the database rejects the duplicate insert immediately. The event processor returns HTTP 200 without re-triggering business logic.
2. **Terminal State Protection**:
   * A call that reaches a terminal state (`COMPLETED`, `ABANDONED`, `FAILED`, `BUSY`, `NO_ANSWER`) is locked against further transitions.
   * If a delayed event arrives for a call that is already terminal, the state machine logs and drops the event without modifying agent or borrower status.

### Alternatives Considered
* *In-Memory Deduplication (e.g. LRU Cache / Redis SET)*:
  * *Rejected*: If the API worker restarts or scales horizontally, the cache is lost or split across instances. Relational database constraints provide durable deduplication.

### Consequences
* **Positive**: Fully immune to duplicate carrier webhooks and network retransmissions.
* **Positive**: Out-of-order events cannot resurrect finished calls or overwrite completed borrower dispositions.

---

## ADR-005: Dual Telecom Provider Simulation with Chaos Injection and Circuit Breakers

### Status
Accepted & Implemented

### Context
Testing telephony systems in development environments cannot rely on live phone numbers or paid telecom carriers. Furthermore, unit tests that only test the "happy path" fail to catch production concurrency, out-of-order events, carrier degradation, and sudden outages.

### Decision
We built two distinct mock telecom providers:
1. **Provider A (`MockProviderA`)**: Represents a high-grade tier-1 carrier (low latency 10–50ms, 99.5% success rate, ordered event sequencing).
2. **Provider B (`MockProviderB`)**: Represents a chaotic carrier for stress and failure testing:
   * High latency (100–500ms).
   * Configurable failure bursts (e.g. 10 consecutive failures).
   * Configurable probability of duplicate event emission.
   * Configurable probability of out-of-order event delivery (e.g. `answered` delivered before `ringing`).
3. **Provider Health Service & Circuit Breaker**:
   * Tracks failure rates across a sliding window.
   * Tripping mechanism (`CLOSED` -> `OPEN` -> `HALF_OPEN`) automatically stops sending traffic to an unhealthy provider.

### Alternatives Considered
* *Third-Party Mock Server (e.g. WireMock / Mockoon)*:
  * *Rejected*: External mock servers cannot easily simulate dynamic in-flight call state, asynchronous WebRTC audio bridging, or custom chaos injections without complex scripting.

### Consequences
* **Positive**: Full test suite and simulation engine run entirely locally without external API keys or cloud services.
* **Positive**: Evaluator can inject failure bursts, duplicate events, and network latency at runtime to verify self-healing behavior.
