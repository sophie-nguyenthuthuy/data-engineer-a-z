"""Test lab #23 (FinOps) — đo từ data/samples rồi kiểm tra mô hình chi phí. Offline, < 3 s."""

from __future__ import annotations

import pytest
from cost_model import (
    ATHENA_MIN_BYTES_PER_QUERY,
    GB,
    TB,
    Source,
    athena_cost,
    estimate,
    format_report,
    measure_sources,
    s3_monthly_cost,
)


@pytest.fixture(scope="module")
def sources() -> list[Source]:
    return measure_sources()


def test_measured_sources_cover_four_feeds_and_parquet_is_smaller(sources: list[Source]):
    assert [s.name for s in sources] == [
        "pos_transactions",
        "pos_line_items",
        "app_events",
        "iot_readings",
    ]
    for s in sources:
        assert s.bytes_per_row_parquet < s.bytes_per_row_raw, s.name
        assert 4 < s.bytes_per_row_raw / s.bytes_per_row_parquet < 12, s.name  # đo được 6–10x
    txn = sources[0]
    assert 9_000 < txn.rows_per_day < 10_500  # ~767 dòng/ngày ở scale 0.08 → ÷0.08
    assert 85 < txn.bytes_per_row_raw < 95  # CSV ~90 B/dòng


def test_pricing_formulas():
    assert s3_monthly_cost(GB) == pytest.approx(0.025)
    assert athena_cost(TB, 1) == pytest.approx(5.0)
    # query nhỏ hơn 10 MB vẫn bị tính 10 MB
    assert athena_cost(1, 1) == athena_cost(ATHENA_MIN_BYTES_PER_QUERY, 1)
    assert athena_cost(1, 900) == pytest.approx(900 * 10 * 1024**2 / TB * 5.0)


def test_estimate_90_days(sources: list[Source]):
    r = estimate(sources, days=90)
    assert 1.2 < r["raw_gb"] < 1.8  # ~1.47 GB raw
    assert 0.12 < r["parquet_gb"] < 0.25  # ~0.18 GB Parquet
    assert r["parquet_gb"] / r["raw_gb"] < 0.15  # Parquet tiết kiệm ≥ 85%
    assert (
        r["s3_month_parquet_usd"] < r["s3_month_raw_usd"] < 0.10
    )  # S3 gần như miễn phí ở quy mô này
    assert r["athena_month_pruned_usd"] < r["athena_month_parquet_usd"] < r["athena_month_raw_usd"]
    assert 5 < r["athena_month_raw_usd"] < 8  # ~6.4 USD/tháng nếu quét CSV full mỗi query
    # pruning: 900 query × 10 MB tối thiểu = 0.043 USD — chạm sàn 10 MB
    assert r["athena_month_pruned_usd"] == pytest.approx(900 * 10 * 1024**2 / TB * 5.0)


def test_multiplier_scales_linearly(sources: list[Source]):
    base = estimate(sources, days=90)
    big = estimate(sources, days=90, multiplier=100)
    assert big["raw_gb"] == pytest.approx(base["raw_gb"] * 100)
    assert big["athena_month_raw_usd"] == pytest.approx(base["athena_month_raw_usd"] * 100)
    assert (
        big["athena_month_raw_usd"] > 400
    )  # ~500 USD/tháng — lúc này format + partition mới "có giá"


def test_report_mentions_every_source(sources: list[Source]):
    text = format_report(estimate(sources))
    for s in sources:
        assert s.name in text
    assert "Parquet + pruning" in text
