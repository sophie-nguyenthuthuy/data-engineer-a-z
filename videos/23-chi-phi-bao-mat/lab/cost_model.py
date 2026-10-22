"""Lab #23 — ước lượng chi phí S3 + Athena cho 90 ngày dữ liệu Phin Phố, CSV vs Parquet, có/không partition pruning.

    uv run python videos/23-chi-phi-bao-mat/lab/cost_model.py
    uv run python videos/23-chi-phi-bao-mat/lab/cost_model.py --multiplier 100   # "nếu có 3 000 cửa hàng"

Phần đo kích thước từ samples đã viết sẵn (measure_sources). Bạn điền 3 TODO: công thức S3, công thức Athena,
và các con số tổng trong estimate().

Kích thước đo từ data/samples (sinh ở scale 0.08) rồi nhân tỉ lệ. Giá public ap-southeast-1:
S3 Standard ~0.025 USD/GB-tháng, Athena 5 USD/TB quét (tối thiểu 10 MB/query).
"""

from __future__ import annotations

import argparse
import io
from dataclasses import dataclass
from pathlib import Path

import polars as pl

REPO = Path(__file__).resolve().parents[3]
SAMPLES = REPO / "data" / "samples"

# Giá ap-southeast-1 kiểm qua AWS Price List API ngày 2026-10-22 (xem README bước 3). Giá đổi theo thời gian.
S3_USD_PER_GB_MONTH = 0.025
ATHENA_USD_PER_TB = 5.0
ATHENA_MIN_BYTES_PER_QUERY = 10 * 1024**2  # Athena tính tối thiểu 10 MB mỗi query

SAMPLE_SCALE = 0.08  # make generate dùng --scale 0.08
IOT_SAMPLE_HOURS = 0.75  # coffeegen iot --hours 0.75
IOT_OPERATING_HOURS = 16  # máy pha chạy ~16 h/ngày
GB = 1024**3
TB = 1024**4


@dataclass(frozen=True)
class Source:
    name: str
    raw_format: str  # csv | jsonl
    rows_per_day: float  # ở quy mô thật (30 cửa hàng, scale 1.0)
    bytes_per_row_raw: float
    bytes_per_row_parquet: float

    def rows(self, days: int, multiplier: float = 1.0) -> float:
        return self.rows_per_day * days * multiplier

    def raw_bytes(self, days: int, multiplier: float = 1.0) -> float:
        return self.rows(days, multiplier) * self.bytes_per_row_raw

    def parquet_bytes(self, days: int, multiplier: float = 1.0) -> float:
        return self.rows(days, multiplier) * self.bytes_per_row_parquet


def _bytes_per_row(df: pl.DataFrame, raw_bytes: int) -> tuple[float, float]:
    buf = io.BytesIO()
    df.write_parquet(buf, compression="zstd")
    return raw_bytes / df.height, len(buf.getvalue()) / df.height


def _normal_day_rows(df: pl.DataFrame, ts_col: str = "ts") -> float:
    """Trung bình dòng/ngày của những ngày 'bình thường': bỏ ngày Tết (< 50% ngày cao nhất)."""
    per_day = df.group_by(pl.col(ts_col).str.slice(0, 10)).len()["len"].to_list()
    normal = [n for n in per_day if n >= 0.5 * max(per_day)]
    return sum(normal) / len(normal)


def measure_sources(samples: Path = SAMPLES) -> list[Source]:
    """Đo bytes/row CSV vs Parquet và rows/ngày từ samples; quy về scale 1.0."""
    txn = pl.read_csv(samples / "pos_transactions.csv", infer_schema=False, null_values=[""])
    raw, pq = _bytes_per_row(txn, len(txn.write_csv().encode()))
    txn_per_day = _normal_day_rows(txn) / SAMPLE_SCALE

    lines = pl.read_csv(samples / "pos_line_items.csv", infer_schema=False, null_values=[""])
    raw_l, pq_l = _bytes_per_row(lines, len(lines.write_csv().encode()))
    lines_per_txn = lines.height / txn.height

    events = pl.read_ndjson(samples / "app_events.jsonl")
    raw_e, pq_e = _bytes_per_row(events, (samples / "app_events.jsonl").stat().st_size)
    events_per_day = _normal_day_rows(events) / SAMPLE_SCALE

    iot = pl.read_csv(samples / "iot_readings.csv", infer_schema=False, null_values=[""])
    raw_i, pq_i = _bytes_per_row(iot, len(iot.write_csv().encode()))
    iot_per_day = (
        iot.height / IOT_SAMPLE_HOURS * IOT_OPERATING_HOURS
    )  # 60 máy không phụ thuộc scale

    return [
        Source("pos_transactions", "csv", txn_per_day, raw, pq),
        Source("pos_line_items", "csv", txn_per_day * lines_per_txn, raw_l, pq_l),
        Source("app_events", "jsonl", events_per_day, raw_e, pq_e),
        Source("iot_readings", "csv", iot_per_day, raw_i, pq_i),
    ]


def s3_monthly_cost(total_bytes: float, usd_per_gb_month: float = S3_USD_PER_GB_MONTH) -> float:
    # TODO: bytes → GB (chia GB) × giá/GB-tháng
    raise NotImplementedError


def athena_cost(
    bytes_scanned_per_query: float, n_queries: int, usd_per_tb: float = ATHENA_USD_PER_TB
) -> float:
    # TODO: mỗi query tính tối thiểu ATHENA_MIN_BYTES_PER_QUERY; tổng bytes → TB (chia TB) × giá/TB
    raise NotImplementedError


def estimate(
    sources: list[Source],
    days: int = 90,
    multiplier: float = 1.0,
    queries_per_day: int = 30,
) -> dict:
    # TODO: raw_total / pq_total = tổng bytes raw / parquet của mọi source cho `days` ngày × multiplier
    raw_total = 0.0
    pq_total = 0.0
    n_queries = queries_per_day * 30  # một tháng truy vấn
    # TODO: pruned = bytes một query phải quét khi CHỈ đọc 1 partition dt= trong `days` ngày
    pruned = pq_total
    return {
        "days": days,
        "multiplier": multiplier,
        "per_source": [
            {
                "name": s.name,
                "rows": round(s.rows(days, multiplier)),
                "raw_gb": s.raw_bytes(days, multiplier) / GB,
                "parquet_gb": s.parquet_bytes(days, multiplier) / GB,
                "ratio": s.bytes_per_row_raw / s.bytes_per_row_parquet,
            }
            for s in sources
        ],
        "raw_gb": raw_total / GB,
        "parquet_gb": pq_total / GB,
        "s3_month_raw_usd": s3_monthly_cost(raw_total),
        "s3_month_parquet_usd": s3_monthly_cost(pq_total),
        "athena_month_raw_usd": athena_cost(raw_total, n_queries),
        "athena_month_parquet_usd": athena_cost(pq_total, n_queries),
        "athena_month_pruned_usd": athena_cost(pruned, n_queries),
        "n_queries": n_queries,
    }


def format_report(r: dict) -> str:
    out = [
        f"Ước lượng {r['days']} ngày dữ liệu × {r['multiplier']:g} (30 cửa hàng = 1.0)",
        "",
        f"{'source':<18}{'rows':>12}{'raw MB':>10}{'parquet MB':>12}{'ratio':>7}",
    ]
    for s in r["per_source"]:
        out.append(
            f"{s['name']:<18}{s['rows']:>12,}{s['raw_gb'] * 1024:>10.1f}"
            f"{s['parquet_gb'] * 1024:>12.1f}{s['ratio']:>6.1f}x"
        )
    out += [
        "",
        f"Tổng: raw {r['raw_gb']:.3f} GB vs Parquet {r['parquet_gb']:.3f} GB "
        f"(giảm {(1 - r['parquet_gb'] / r['raw_gb']):.0%})",
        f"S3/tháng       : raw {r['s3_month_raw_usd']:.4f} USD | Parquet {r['s3_month_parquet_usd']:.4f} USD",
        f"Athena/tháng ({r['n_queries']} query):",
        f"  full scan raw      : {r['athena_month_raw_usd']:.3f} USD",
        f"  full scan Parquet  : {r['athena_month_parquet_usd']:.3f} USD",
        f"  Parquet + pruning  : {r['athena_month_pruned_usd']:.3f} USD  (1 partition dt=/query, tối thiểu 10 MB)",
    ]
    return "\n".join(out)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--multiplier", type=float, default=1.0, help="100 = 3 000 cửa hàng")
    p.add_argument("--queries-per-day", type=int, default=30)
    a = p.parse_args()
    print(format_report(estimate(measure_sources(), a.days, a.multiplier, a.queries_per_day)))


if __name__ == "__main__":
    main()
