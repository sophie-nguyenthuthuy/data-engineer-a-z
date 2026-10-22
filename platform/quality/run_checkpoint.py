"""Run the Great Expectations checkpoint on one silver partition. Exit 1 when any expectation fails.

python run_checkpoint.py --date 2026-02-15 --lake ./lake-local
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import great_expectations as gx

sys.path.insert(0, str(Path(__file__).parent))
from contract_check import read_parquet_any  # noqa: E402
from suites.pos_transactions_suite import build_suite  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--lake", default="./lake-local")
    a = p.parse_args()

    path = f"{a.lake.rstrip('/')}/silver/pos_transactions/dt={a.date}/*.parquet"
    df = read_parquet_any(path).to_pandas()

    context = gx.get_context(
        mode="ephemeral"
    )  # không ghi gì ra disk; Data Docs sẽ ở video #20 phần 2
    ds = context.data_sources.add_pandas("silver")
    asset = ds.add_dataframe_asset("pos_transactions")
    batch_def = asset.add_batch_definition_whole_dataframe("partition")
    suite = context.suites.add(build_suite())
    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name="pos_txn", data=batch_def, suite=suite)
    )
    result = vd.run(batch_parameters={"dataframe": df})

    print(
        f"{'✅' if result.success else '❌'} {sum(r.success for r in result.results)}/{len(result.results)} expectations passed for dt={a.date}"
    )
    for r in result.results:
        if not r.success:
            print(f"  - FAIL {r.expectation_config.type} {r.expectation_config.kwargs}")
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
