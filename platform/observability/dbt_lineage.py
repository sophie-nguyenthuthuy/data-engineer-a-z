"""Gửi lineage cấp model của dbt lên Marquez từ `target/manifest.json` + `target/run_results.json` (stdlib only).

    dbt build --profiles-dir . --target dev            # sinh manifest.json + run_results.json
    python platform/observability/dbt_lineage.py --target-dir platform/dbt/target \
        --url http://localhost:5050/api/v1/lineage --namespace dea2z-dbt

Vì sao không dùng `dbt-ol` (openlineage-dbt)? Nó chỉ hỗ trợ adapter bigquery/snowflake/redshift/spark/postgres/
databricks/sqlserver/dremio — không có duckdb (bản 1.18). Nhưng mọi thứ nó cần đều nằm trong 2 file JSON dbt
đã ghi: đồ thị phụ thuộc (`depends_on.nodes`), bảng đích (`schema.alias`), trạng thái và thời gian chạy.
60 dòng dưới đây là phần lõi của dbt-ol: mỗi model = một job OpenLineage, inputs = model/source nó đọc,
output = chính nó. Chạy lại sau mỗi `dbt build` → Marquez có run history.
"""

from __future__ import annotations

import argparse
import json
import urllib.request
import uuid
from datetime import datetime, timedelta
from pathlib import Path

PRODUCER = "https://github.com/sophie-nguyenthuthuy/data-engineer-a-z"
SCHEMA_URL = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"


def dataset_of(node: dict, ds_namespace: str) -> dict:
    """Model → <schema>.<alias>; source → <schema>.<identifier>. Namespace là nơi dữ liệu nằm, không phải job."""
    if node["resource_type"] == "source":
        return {"namespace": ds_namespace, "name": f"{node['schema']}.{node['identifier']}"}
    return {
        "namespace": ds_namespace,
        "name": f"{node['schema']}.{node.get('alias') or node['name']}",
    }


def events_for_run(manifest: dict, results: dict, job_ns: str, ds_ns: str) -> list[dict]:
    all_nodes = {**manifest["nodes"], **manifest["sources"]}
    by_id = {r["unique_id"]: r for r in results["results"]}
    generated = datetime.fromisoformat(results["metadata"]["generated_at"].replace("Z", "+00:00"))
    events: list[dict] = []
    for uid, node in manifest["nodes"].items():
        if node["resource_type"] not in ("model", "snapshot") or uid not in by_id:
            continue
        res = by_id[uid]
        run_id = str(uuid.uuid4())
        end = generated
        start = end - timedelta(seconds=res.get("execution_time", 0))
        inputs = [
            dataset_of(all_nodes[d], ds_ns) for d in node["depends_on"]["nodes"] if d in all_nodes
        ]
        output = dataset_of(node, ds_ns)
        output["facets"] = {
            "dataSource": {
                "_producer": PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
                "name": ds_ns,
                "uri": ds_ns,
            }
        }
        job = {
            "namespace": job_ns,
            "name": f"{manifest['metadata']['project_name']}.{node['name']}",
            "facets": {
                "sql": {
                    "_producer": PRODUCER,
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SqlJobFacet.json",
                    "query": (node.get("compiled_code") or node.get("raw_code") or "")[:4000],
                }
            },
        }
        base = {"run": {"runId": run_id}, "job": job, "producer": PRODUCER, "schemaURL": SCHEMA_URL}
        events.append(
            {
                **base,
                "eventType": "START",
                "eventTime": start.isoformat(),
                "inputs": inputs,
                "outputs": [],
            }
        )
        kind = "COMPLETE" if res["status"] in ("success", "pass") else "FAIL"
        events.append(
            {
                **base,
                "eventType": kind,
                "eventTime": end.isoformat(),
                "inputs": inputs,
                "outputs": [output],
            }
        )
    return events


def post(url: str, payload: dict) -> int:
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310 — local Marquez
        return resp.status


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--target-dir", default="platform/dbt/target")
    p.add_argument("--url", default="http://localhost:5050/api/v1/lineage")
    p.add_argument("--namespace", default="dea2z-dbt", help="job namespace")
    p.add_argument("--dataset-namespace", default="duckdb://coffee_gold", help="nơi bảng dbt nằm")
    p.add_argument("--dry-run", action="store_true", help="in JSON thay vì gửi")
    a = p.parse_args()
    target = Path(a.target_dir)
    manifest = json.loads((target / "manifest.json").read_text())
    results = json.loads((target / "run_results.json").read_text())
    events = events_for_run(manifest, results, a.namespace, a.dataset_namespace)
    if a.dry_run:
        print(json.dumps(events[:2], indent=2, ensure_ascii=False))
        print(f"... {len(events)} events ({len(events) // 2} job)")
        return
    ok = 0
    for ev in events:
        ok += post(a.url, ev) in (200, 201)
    jobs = sorted({e["job"]["name"] for e in events})
    print(f"sent {ok}/{len(events)} events → {len(jobs)} job trong namespace {a.namespace}:")
    for j in jobs:
        print("  ", j)
    print("Mở http://localhost:3000 → namespace", a.namespace)


if __name__ == "__main__":
    main()
