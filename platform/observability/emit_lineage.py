"""Send one OpenLineage START + COMPLETE event pair by hand (stdlib only) to see the format.

python emit_lineage.py --job coffee.ingest --input postgres://coffee_pos/pos_transactions --output s3://lake/silver/pos_transactions
"""

from __future__ import annotations

import argparse
import json
import urllib.request
import uuid
from datetime import UTC, datetime

PRODUCER = "https://github.com/sophie-nguyenthuthuy/data-engineer-a-z"
SCHEMA_URL = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"


def dataset(uri: str) -> dict:
    namespace, _, name = uri.rpartition("/")
    return {"namespace": namespace, "name": name}


def event(
    kind: str, run_id: str, job: str, inputs: list[str], outputs: list[str], namespace: str
) -> dict:
    return {
        "eventType": kind,
        "eventTime": datetime.now(UTC).isoformat(),
        "run": {"runId": run_id},
        "job": {"namespace": namespace, "name": job},
        "inputs": [dataset(u) for u in inputs],
        "outputs": [dataset(u) for u in outputs],
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
    }


def post(url: str, payload: dict) -> int:
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310 — local Marquez
        return resp.status


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://localhost:5050/api/v1/lineage")
    p.add_argument("--namespace", default="dea2z-manual")
    p.add_argument("--job", required=True)
    p.add_argument("--input", action="append", default=[])
    p.add_argument("--output", action="append", default=[])
    a = p.parse_args()
    run_id = str(uuid.uuid4())
    for kind in ("START", "COMPLETE"):
        status = post(a.url, event(kind, run_id, a.job, a.input, a.output, a.namespace))
        print(f"{kind} → HTTP {status}")
    print("Mở http://localhost:3000 → namespace", a.namespace)


if __name__ == "__main__":
    main()
