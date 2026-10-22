"""Check a Parquet dataset against a YAML data contract. Exit 1 on any violation.

    python contract_check.py --contract contracts/pos_transactions.yaml --parquet "lake-local/silver/pos_transactions/dt=2026-02-15/*.parquet"

Cố ý viết đơn giản (polars + PyYAML) để bạn đọc hết trong 5 phút — đây là "Great Expectations phiên bản 1 trang".
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import polars as pl
import yaml

TYPE_MAP = {
    "string": {pl.Utf8, pl.String},
    "integer": {pl.Int64, pl.Int32, pl.Int16, pl.Int8, pl.UInt64, pl.UInt32},
    "float": {pl.Float64, pl.Float32},
    "boolean": {pl.Boolean},
    "timestamp": None,  # any Datetime
    "date": {pl.Date},
}


def read_parquet_any(path: str) -> pl.DataFrame:
    """Đọc Parquet local hoặc `s3://` (MinIO qua S3_ENDPOINT).

    Polars đọc S3 bằng object_store, KHÔNG đọc biến S3_ENDPOINT như pyarrow trong coffee_ingestion → phải truyền
    storage_options. Thiếu `aws_allow_http` là nó âm thầm gọi https://s3.us-east-1.amazonaws.com → InvalidAccessKeyId.
    """
    if not path.startswith("s3://"):
        return pl.read_parquet(path)
    endpoint = os.environ.get("S3_ENDPOINT")
    opts = None
    if endpoint:  # MinIO; không có S3_ENDPOINT = AWS thật, object_store tự lấy credentials
        opts = {
            "aws_endpoint_url": endpoint,
            "aws_allow_http": "true" if endpoint.startswith("http://") else "false",
            "aws_virtual_hosted_style_request": "false",
            "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "aws_region": os.environ.get("AWS_REGION", "us-east-1"),
        }
    return pl.read_parquet(path, storage_options=opts)


def check(df: pl.DataFrame, contract: dict, run_date: str | None = None) -> list[str]:
    errors: list[str] = []
    for col in contract["columns"]:
        name = col["name"]
        if name not in df.columns:
            errors.append(f"missing column {name}")
            continue
        dtype = df.schema[name]
        allowed_types = TYPE_MAP[col["type"]]
        if allowed_types is not None and dtype not in allowed_types:
            errors.append(f"{name}: type {dtype} not {col['type']}")
        if col["type"] == "timestamp" and not isinstance(dtype, pl.Datetime):
            errors.append(f"{name}: type {dtype} not timestamp")
        s = df[name]
        if col.get("required") and s.null_count():
            errors.append(f"{name}: {s.null_count()} nulls but required")
        if col.get("unique") and s.n_unique() != df.height:
            errors.append(f"{name}: {df.height - s.n_unique()} duplicate values")
        if "allowed" in col:
            bad = set(s.drop_nulls().unique().to_list()) - set(col["allowed"])
            if bad:
                errors.append(f"{name}: values not allowed {sorted(bad)[:5]}")
        if "pattern" in col:
            n_bad = df.filter(
                pl.col(name).is_not_null() & ~pl.col(name).str.contains(col["pattern"])
            ).height
            if n_bad:
                errors.append(f"{name}: {n_bad} values do not match {col['pattern']}")
        if "min" in col:
            n_bad = df.filter(pl.col(name) < col["min"]).height
            if n_bad:
                errors.append(f"{name}: {n_bad} values < {col['min']}")
    sla = contract.get("sla", {})
    low_volume_ok = run_date in set(contract.get("exceptions", {}).get("low_volume_dates", []))
    if "min_rows_per_day" in sla and df.height < sla["min_rows_per_day"] and not low_volume_ok:
        errors.append(f"only {df.height} rows < min_rows_per_day {sla['min_rows_per_day']}")
    return errors


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--contract", required=True)
    p.add_argument("--parquet", required=True, help="glob or s3 path")
    p.add_argument("--date", default=None, help="business date, used for low-volume exceptions")
    a = p.parse_args(argv)
    contract = yaml.safe_load(Path(a.contract).read_text(encoding="utf-8"))
    df = read_parquet_any(a.parquet)
    run_date = a.date or (a.parquet.split("dt=")[1][:10] if "dt=" in a.parquet else None)
    errors = check(df, contract, run_date)
    print(
        f"contract {contract['contract']} v{contract['version']} — {df.height} rows — {'OK ✅' if not errors else 'FAILED ❌'}"
    )
    for e in errors:
        print(f"  - {e}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
