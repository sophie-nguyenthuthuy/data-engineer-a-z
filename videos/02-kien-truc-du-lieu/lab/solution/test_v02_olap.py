"""Test lời giải #02 — chạy offline trên data/samples, < 2 s."""

from __future__ import annotations

from pathlib import Path

from oltp_vs_olap import csv_to_parquet, revenue_by_day_store

REPO = Path(__file__).resolve().parents[4]
CSV = REPO / "data" / "samples" / "pos_transactions.csv"


def test_csv_and_parquet_give_same_answer(tmp_path: Path):
    pq = tmp_path / "pos_transactions.parquet"
    csv_to_parquet(CSV, pq)
    rows_csv = revenue_by_day_store(CSV)
    rows_pq = revenue_by_day_store(pq)
    assert rows_csv == rows_pq
    # 30 cửa hàng × 6 ngày (14→19/02/2026), bỏ 8 hoá đơn store_id NULL
    assert len(rows_csv) == 180
    assert sum(r[2] for r in rows_csv) == 238_299_000
    # Parquet nén tốt hơn CSV rõ rệt ngay cả với file nhỏ
    assert pq.stat().st_size < CSV.stat().st_size / 3
