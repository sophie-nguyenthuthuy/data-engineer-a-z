"""Lab #07 — thêm MỘT rule silver của bạn, bên cạnh các rule có sẵn trong coffee_ingestion.silver.

Chạy:
    uv run python videos/07-etl-vs-elt/lab/extra_rule.py --date 2026-02-15

Ý tưởng: pipeline platform/ingestion đã xử lý duplicate / null_store / tz_drift. Nhưng còn những dòng "đáng ngờ"
mà schema không cấm, ví dụ:
  - discount_gt_total : giảm giá lớn hơn số tiền khách trả (discount_vnd > total_amount_vnd) — đối soát lại
  - app_cash          : đơn đặt qua app nhưng thanh toán tiền mặt — SCHEMA.md nói app không bao giờ cash

Quy tắc của khoá: không XOÁ, đưa sang quarantine kèm cột `_reason` để team nguồn nhìn thấy.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl

from coffee_ingestion.silver import clean_transactions
from coffee_ingestion.sources import read_folder

REPO = Path(__file__).resolve().parents[3]
SAMPLES = REPO / "data" / "samples"


def apply_extra_rules(silver: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, int]]:
    """Nhận silver (đã typed, đã dedup) → trả (silver_sạch, quarantine, số dòng theo từng reason).

    quarantine có đúng các cột: transaction_id, store_id, channel, payment_method,
    total_amount_vnd, discount_vnd, _reason.
    """
    # TODO 1: tạo expression boolean cho rule discount_gt_total
    #   gợi ý: pl.col("discount_vnd") > pl.col("total_amount_vnd")
    discount_gt_total = pl.lit(False)

    # TODO 2: tạo expression boolean cho rule app_cash
    #   gợi ý: (pl.col("channel") == "app") & (pl.col("payment_method") == "cash")
    app_cash = pl.lit(False)

    # TODO 3: gắn _reason cho từng dòng vi phạm (ưu tiên discount_gt_total nếu vi phạm cả hai)
    #   gợi ý: pl.when(discount_gt_total).then(pl.lit("discount_gt_total"))
    #            .when(app_cash).then(pl.lit("app_cash")).otherwise(None)
    #   (placeholder dưới đây gộp 2 rule → tách thành 2 nhánh when/then, mỗi rule một _reason)
    flagged = silver.with_columns(
        pl.when(discount_gt_total | app_cash)
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("_reason")
    )

    # TODO 4: tách 2 DataFrame: dòng có _reason → quarantine, còn lại → silver (drop cột _reason)
    quarantine = flagged.filter(pl.col("_reason").is_not_null()).select(
        "transaction_id",
        "store_id",
        "channel",
        "payment_method",
        "total_amount_vnd",
        "discount_vnd",
        "_reason",
    )
    clean = flagged.filter(pl.col("_reason").is_null()).drop("_reason")

    # TODO 5: đếm số dòng theo reason — LUÔN có đủ 2 key kể cả khi = 0 (để summary ổn định)
    reasons = {"discount_gt_total": 0, "app_cash": 0}
    return clean, quarantine, reasons


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", default=str(SAMPLES))
    p.add_argument("--date", default="2026-02-15", help="YYYY-MM-DD; 'all' = không lọc")
    a = p.parse_args()

    raw = read_folder(Path(a.source), "pos_transactions")
    if a.date != "all":
        raw = raw.filter(pl.col("ts").str.slice(0, 10) == a.date)  # cùng cách lọc với pipeline.py
    silver, _q_builtin, stats = clean_transactions(raw)
    clean, quarantine, reasons = apply_extra_rules(silver)

    print(
        json.dumps(
            {
                "date": a.date,
                "builtin": stats.reasons,
                "rows_in": silver.height,
                "rows_out": clean.height,
                "extra_quarantined": quarantine.height,
                "extra_reasons": reasons,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if quarantine.height:
        print(quarantine)


if __name__ == "__main__":
    main()
