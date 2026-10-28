# SmartDialer

A complete prototype of a SmartDialer system for outbound call campaigns, implementing:

- **Progressive Dialer** — one agent = one call, deterministic baseline
- **Predictive Pacing Engine** — statistical dial-rate estimation with EWMA answer-rate smoothing
- **Safety Controller** — non-bypassable hard limits on every allocation
- **Atomic Call Allocator** — PostgreSQL-backed atomic agent+borrower+call reservation
- **Two behaviorally different Mock Providers** — A (fast/reliable) and B (slow/unreliable/duplicate events)
- **Durable event processing** — idempotent, handles duplicates, out-of-order, and delayed events
- **Worker recovery & reconciliation** — crash-safe lease-based dispatch
- **Full simulation engine** — 7 scenarios comparing Progressive vs Predictive modes
- **Load testing** — asyncio-based harness measuring allocation throughput
- **Complete test suite** — unit, integration, concurrency, failure-injection, property tests

## Architecture

```
Campaign Manager
  → Dialing Policy
      → Progressive Dialer OR Predictive Pacing Engine
          → Safety Controller (non-bypassable)
              → Atomic Call Allocator
                  → Provider Adapter
                      → Mock Provider A / Mock Provider B

Provider events → Durable Inbox → Idempotent Event Processor → Call State Machine
```

**Safety boundary**: The predictive engine proposes a dial count. It cannot call providers directly. Only the Safety Controller + Allocator path may initiate calls, and only after re-reading authoritative database state.

## Requirements

- Python 3.13+
- Optional: Docker + Docker Compose (for PostgreSQL in production mode; test suite and simulation scripts run out-of-the-box using the built-in SQLite fallback)

## Quick Start

```bash
# 1. Setup virtual environment & dependencies
py -3.13 -m venv .venv
.\.venv\Scripts\pip install -e ".[dev]"

# 2. Seed realistic demo data (runs on SQLite or PostgreSQL automatically)
.\.venv\Scripts\python scripts/seed_demo.py --db-path smartdialer.db

# 3. Start API server
.\.venv\Scripts\python -m uvicorn app.main:app --reload --port 8000

# 4. View interactive API documentation
# Open http://localhost:8000/docs
```

## Running Tests

```bash
# All tests
py -3.13 -m pytest tests/ -v

# Unit tests only (no DB needed)
py -3.13 -m pytest tests/unit/ -v

# Integration + concurrency tests (requires running PostgreSQL)
py -3.13 -m pytest tests/integration/ tests/concurrency/ -v

# Failure injection scenarios
py -3.13 -m pytest tests/failure_injection/ -v -m "not slow"

# With coverage
py -3.13 -m pytest tests/ --cov=app --cov-report=term-missing
```

## Simulation

```bash
# Run all scenarios, compare Progressive vs Predictive
py -3.13 scripts/run_simulation.py

# Run a specific scenario
py -3.13 scripts/run_simulation.py --scenario low_answer_rate --mode progressive

# With a fixed seed for reproducibility
py -3.13 scripts/run_simulation.py --seed 42
```

## Load Test

```bash
# Quick smoke test (100 agents)
py -3.13 scripts/run_load_test.py --agents 100 --duration 30

# Larger test (1000 agents)
py -3.13 scripts/run_load_test.py --agents 1000 --duration 60
```

## API Examples

See [docs/architecture.md](docs/architecture.md) for full curl examples.

```bash
# Health check
curl http://localhost:8000/health

# Metrics (Prometheus format)
curl http://localhost:8000/metrics

# Create campaign
curl -X POST http://localhost:8000/api/v1/campaigns \
  -H "Content-Type: application/json" \
  -d '{"name": "Test Campaign", "dialing_mode": "progressive", "max_concurrent_calls": 10}'

# Register an agent
curl -X POST http://localhost:8000/api/v1/agents \
  -H "Content-Type: application/json" \
  -d '{"campaign_id": "<id>", "name": "Agent-001"}'

# Run one dialing cycle
curl -X POST http://localhost:8000/api/v1/campaigns/<id>/dial
```

## Troubleshooting

- **Port 5432 in use**: Change the port in `docker-compose.yml` and `.env`
- **Migration fails**: Ensure the DB container is healthy (`docker compose ps`)
- **Tests fail with connection error**: Run `docker compose up -d postgres_test` first
- **Windows path issues**: Use forward slashes or raw strings in config

## Documentation

- [Architecture](docs/architecture.md)
- [State Machines](docs/state_machines.md)
- [Architecture Decisions](docs/architecture_decisions.md)
- [Failure Scenarios](docs/failure_scenarios.md)
- [Interview Guide](docs/interview_guide.md)
