"""Send one provider webhook event to a running API, signed like a real provider.

Usage (server on localhost:8000):
  python scripts/send_provider_event.py --provider-call-id pA-1234 --event-type completed
  PROVIDER_WEBHOOK_SECRET=change-me-a python scripts/send_provider_event.py \
      --provider mock_provider_a --provider-call-id pA-1234 --event-type answered

Without a secret the request is sent unsigned (accepted only when the server
has no PROVIDER_WEBHOOK_SECRETS and is not in production).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from urllib.parse import urlencode, urlsplit

import httpx

from app.api.webhook_auth import SIGNATURE_HEADER, TIMESTAMP_HEADER, sign


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://127.0.0.1:8000/api/v1/provider-events")
    parser.add_argument("--provider", default="mock_provider_a")
    parser.add_argument("--provider-call-id", required=True)
    parser.add_argument("--event-type", required=True,
                        help="ringing, answered, connected, completed, no_answer, busy, failed, ...")
    parser.add_argument("--event-id", default=None, help="Provider event id (default: random)")
    parser.add_argument("--payload", default=None, help="JSON object sent as the request body")
    parser.add_argument("--secret", default=os.environ.get("PROVIDER_WEBHOOK_SECRET"),
                        help="Shared secret for --provider (default: $PROVIDER_WEBHOOK_SECRET)")
    args = parser.parse_args()

    query = urlencode({
        "provider_id": args.provider,
        "provider_event_id": args.event_id or f"manual-{uuid.uuid4().hex[:12]}",
        "provider_call_id": args.provider_call_id,
        "event_type": args.event_type,
    })
    body = json.dumps(json.loads(args.payload)).encode() if args.payload else b""
    headers = {"Content-Type": "application/json"} if body else {}
    if args.secret:
        timestamp = str(int(time.time()))
        headers[TIMESTAMP_HEADER] = timestamp
        headers[SIGNATURE_HEADER] = sign(args.secret, timestamp, "POST", urlsplit(args.url).path, query, body)

    resp = httpx.post(f"{args.url}?{query}", content=body, headers=headers, timeout=10)
    print(resp.status_code, resp.text)
    return 0 if resp.status_code < 400 else 1


if __name__ == "__main__":
    sys.exit(main())
