"""Silver = typed, de-duplicated, conformed tables. One function per table so each rule is easy to read.

Những lỗi cố ý trong dữ liệu nguồn và cách xử lý (xem data/generator/SCHEMA.md):
  duplicate     → giữ bản ghi đầu tiên theo (transaction_id), ưu tiên dòng có store_id
  null_store    → đưa sang bảng quarantine, không vào silver
  tz_drift      → ts thiếu offset được hiểu là giờ Việt Nam (+07:00) rồi đổi về UTC
  negative_qty  → line item sang quarantine
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

VN_OFFSET = "+07:00"


@dataclass
class SilverResult:
    table: str
    rows_in: int
    rows_out: int
    quarantined: int
    reasons: dict[str, int] = field(default_factory=dict)


def parse_vn_timestamp(col: str) -> pl.Expr:
    """Parse ISO strings that may or may not carry an offset. Missing offset ⇒ Vietnam local time.

    Kết quả: cột Datetime(UTC). Lưu UTC trong lake, hiển thị giờ VN ở BI — quy ước của cả khoá.
    """
    has_offset = pl.col(col).str.contains(r"[+-]\d{2}:\d{2}$")
    fixed = (
        pl.when(has_offset)
        .then(pl.col(col))
        .otherwise(pl.col(col).str.replace(" ", "T") + VN_OFFSET)
    )
    return fixed.str.to_datetime(time_zone="UTC", strict=False)


def clean_transactions(raw: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, SilverResult]:
    """Return (silver_df, quarantine_df, stats)."""
    rows_in = raw.height
    typed = raw.with_columns(
        parse_vn_timestamp("ts").alias("ts"),
        pl.col("ts").alias("ts_raw"),
        pl.col("total_amount_vnd").cast(pl.Int64),
        pl.col("discount_vnd").cast(pl.Int64),
        pl.col("is_rainy_day").str.to_lowercase().eq("true"),
        (~pl.col("ts").str.contains(r"[+-]\d{2}:\d{2}$")).alias("_tz_fixed"),
    )

    bad_store = typed.filter(pl.col("store_id").is_null()).with_columns(
        pl.lit("null_store").alias("_reason")
    )
    good = typed.filter(pl.col("store_id").is_not_null())

    # dedup: keep the earliest row per transaction_id
    deduped = (
        good.sort(["transaction_id", "ts"])
        .with_columns(pl.int_range(pl.len()).over("transaction_id").alias("_rn"))
        .filter(pl.col("_rn") == 0)
        .drop("_rn")
    )
    n_dups = good.height - deduped.height

    silver = deduped.select(
        "transaction_id",
        "store_id",
        "ts",
        "channel",
        "payment_method",
        "customer_id",
        "staff_id",
        "total_amount_vnd",
        "discount_vnd",
        "is_rainy_day",
        "_tz_fixed",
    ).sort("ts")
    quarantine = bad_store.select("transaction_id", "ts_raw", "store_id", "_reason")
    stats = SilverResult(
        "pos_transactions",
        rows_in,
        silver.height,
        quarantine.height,
        {
            "null_store": quarantine.height,
            "duplicate": n_dups,
            "tz_drift": int(silver["_tz_fixed"].sum()),
        },
    )
    return silver, quarantine, stats


def clean_line_items(
    raw: pl.DataFrame, valid_txn_ids: pl.Series
) -> tuple[pl.DataFrame, pl.DataFrame, SilverResult]:
    rows_in = raw.height
    typed = raw.with_columns(
        pl.col("quantity").cast(pl.Int64),
        pl.col("unit_price_vnd").cast(pl.Int64),
        pl.col("line_total_vnd").cast(pl.Int64),
    )
    neg = typed.filter(pl.col("quantity") <= 0).with_columns(
        pl.lit("negative_qty").alias("_reason")
    )
    orphan = typed.filter(
        (pl.col("quantity") > 0) & ~pl.col("transaction_id").is_in(valid_txn_ids)
    ).with_columns(pl.lit("orphan_txn").alias("_reason"))
    good = typed.filter(
        (pl.col("quantity") > 0) & pl.col("transaction_id").is_in(valid_txn_ids)
    ).unique(subset=["line_id"], keep="first")
    quarantine = pl.concat([neg, orphan]).select("line_id", "transaction_id", "quantity", "_reason")
    stats = SilverResult(
        "pos_line_items",
        rows_in,
        good.height,
        quarantine.height,
        {"negative_qty": neg.height, "orphan_txn": orphan.height},
    )
    return good.sort("line_id"), quarantine, stats


def clean_dimension(
    raw: pl.DataFrame, key: str, casts: dict[str, pl.DataType] | None = None
) -> pl.DataFrame:
    df = raw.unique(subset=[key], keep="first")
    if casts:
        # Polars ≥ 2 không cho cast thẳng string → Date, nên dùng str.to_date cho cột ngày.
        exprs = [
            pl.col(c).str.to_date() if t == pl.Date else pl.col(c).cast(t) for c, t in casts.items()
        ]
        df = df.with_columns(exprs)
    return df.sort(key)
