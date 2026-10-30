"""Demonstrate the full mock-calling workflow against a running API server.

Creates a campaign with two agents and a few synthetic borrowers, submits
dialing cycles through POST /campaigns/{id}/dial, and follows every call until
the mock provider's events have driven it to a final state. Only the mock
providers exist in this application — no real calls are ever placed.

Usage (server already running on localhost:8000):
  python scripts/demo_api_workflow.py
  python scripts/demo_api_workflow.py --mode predictive --provider mock_provider_b
  python scripts/demo_api_workflow.py --rounds 3 --borrowers 4
"""
from __future__ import annotations

import argparse
import sys
import time

import httpx

TERMINAL = ("COMPLETED", "FAILED", "CANCELLED")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/api/v1")
    parser.add_argument("--mode", default="progressive", choices=["progressive", "predictive"])
    parser.add_argument("--provider", default="mock_provider_a", choices=["mock_provider_a", "mock_provider_b"])
    parser.add_argument("--agents", type=int, default=2)
    parser.add_argument("--borrowers", type=int, default=3)
    parser.add_argument("--rounds", type=int, default=1, help="Dialing cycles (agents are reused between rounds)")
    parser.add_argument("--timeout", type=float, default=90.0, help="Seconds to wait for calls per round")
    args = parser.parse_args()

    client = httpx.Client(base_url=args.base_url, timeout=10)

    def api(method: str, path: str, **kwargs) -> dict:
        resp = client.request(method, path, **kwargs)
        if resp.status_code >= 400:
            raise SystemExit(f"{method} {path} -> HTTP {resp.status_code}: {resp.text}")
        return resp.json()

    campaign = api("POST", "/campaigns", json={
        "name": f"Demo ({args.mode})",
        "dialing_mode": args.mode,
        "max_concurrent_calls": 10,
        "max_attempts_per_borrower": 2,
        "retry_cooldown_seconds": 0,
        "wrap_up_seconds": 2,
    })
    cid = campaign["id"]
    agent_ids = []
    for i in range(args.agents):
        agent = api("POST", "/agents", json={"campaign_id": cid, "name": f"Agent-{i + 1}"})
        api("POST", f"/agents/{agent['id']}/availability", json={"state": "AVAILABLE"})
        agent_ids.append(agent["id"])
    for i in range(args.borrowers):
        api("POST", "/borrowers", json={
            "campaign_id": cid, "synthetic_name": f"Synthetic Borrower {i + 1}", "masked_phone": f"***-***-{i + 1:04d}",
        })
    api("POST", f"/campaigns/{cid}/start")
    print(f"Campaign {cid} ({args.mode}) with {args.agents} agents and {args.borrowers} borrowers is ACTIVE\n")

    for rnd in range(1, args.rounds + 1):
        dial = api("POST", f"/campaigns/{cid}/dial", params={"provider_id": args.provider})
        decision = dial["safety_decision"]
        print(f"Round {rnd}: mode={dial['dialing_mode']} safety={decision['decision']} "
              f"(requested {decision['requested_count']}, approved {decision['approved_count']}) "
              f"→ {dial['calls_allocated']} call(s) reserved, dispatch {dial['dispatch_status']}, "
              f"worker running={dial['dispatch_worker_running']}")
        if dial["pacing"]:
            p = dial["pacing"]
            print(f"  pacing: recommended {p['recommended_count']} "
                  f"(answer-rate {p['answer_rate_estimate']} {p['answer_rate_confidence']}, "
                  f"health factor {p['provider_health_factor']})")
        if not dial["call_ids"]:
            print("  nothing to dial")
            continue

        history: dict[str, list[str]] = {call_id: [] for call_id in dial["call_ids"]}
        deadline = time.time() + args.timeout
        while time.time() < deadline:
            calls = [api("GET", f"/calls/{call_id}") for call_id in dial["call_ids"]]
            for call in calls:
                step = f"{call['state']}/{call['dispatch_status']}"
                if not history[call["id"]] or history[call["id"]][-1] != step:
                    history[call["id"]].append(step)
            if all(call["state"] in TERMINAL for call in calls):
                break
            time.sleep(0.2)
        for call in calls:
            print(f"  call {call['id'][:8]}: {' → '.join(history[call['id']])}")
            print(f"    provider_call_id={call['provider_call_id']} result={call['failure_reason'] or 'connected & completed'}")
        if not all(call["state"] in TERMINAL for call in calls):
            print("  some calls did not finish within the timeout")
            return 1

        deadline = time.time() + 30
        while time.time() < deadline:
            states = [api("GET", f"/agents/{a}")["state"] for a in agent_ids]
            if all(s == "AVAILABLE" for s in states):
                break
            time.sleep(0.5)
        print(f"  agents: {states}\n")

    borrowers = api("GET", f"/campaigns/{cid}/borrowers")
    print("Borrowers:", ", ".join(f"{b['synthetic_name']}={b['status']}(attempts {b['attempt_count']})" for b in borrowers))
    return 0


if __name__ == "__main__":
    sys.exit(main())
