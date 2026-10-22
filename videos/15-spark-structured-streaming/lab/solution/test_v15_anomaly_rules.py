"""Test offline cho #15: luật anomaly chạy trên data/samples/iot_readings.csv (2 713 dòng, 60 máy, 06:00–06:44)."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import polars as pl
from anomaly_rules import alert_windows, classify_reading, window_start

SAMPLES = Path(__file__).resolve().parents[4] / "data" / "samples" / "iot_readings.csv"


def _rows() -> list[dict]:
    return pl.read_csv(SAMPLES).to_dicts()


def test_classify_matches_generator_status() -> None:
    rows = _rows()
    assert len(rows) == 2713
    assert Counter(r["status"] for r in rows) == {"ok": 371, "warn": 2338, "error": 4}

    mine = Counter(classify_reading(r["boiler_temp_c"], r["pump_pressure_bar"]) for r in rows)
    assert mine == {"warn": 2332, "ok": 373, "unknown": 8}  # 8 = boiler_temp_c NULL (glitch cố ý)

    # status=ok ⇒ luật nói ok; luật nói warn ⇒ status là warn hoặc error (error ghi đè warn)
    for r in rows:
        kind = classify_reading(r["boiler_temp_c"], r["pump_pressure_bar"])
        if r["status"] == "ok":
            assert kind == "ok"
        if kind == "warn":
            assert r["status"] in {"warn", "error"}

    # 2 dòng status=warn nhưng temp đúng 95.50: generator so sánh TRƯỚC khi round(2). Biên là nơi bug sống.
    edge = [
        r
        for r in rows
        if r["status"] == "warn"
        and classify_reading(r["boiler_temp_c"], r["pump_pressure_bar"]) == "ok"
    ]
    assert len(edge) == 2 and all(r["boiler_temp_c"] == 95.5 for r in edge)


def test_window_start_is_utc_tumbling_minute() -> None:
    w = window_start("2026-02-16T06:04:59.401200+07:00")
    assert w.isoformat() == "2026-02-15T23:04:00+00:00"
    # thiếu offset → hiểu là UTC (lệch 7 giờ) — đúng hành vi Spark, và đúng là lỗi tz_drift
    assert window_start("2026-02-16 06:04:59").isoformat() == "2026-02-16T06:04:00+00:00"


def test_alert_windows_per_machine_minute() -> None:
    rows = _rows()
    alerts = alert_windows(rows)
    all_windows = {(r["machine_id"], window_start(r["ts"])) for r in rows}
    assert len(all_windows) == 2640  # 60 máy × 44 phút
    assert len(alerts) == 2319  # samples là 45 phút cao điểm sáng, máy nào cũng nóng
    assert len({m for m, _ in alerts}) == 60
    assert max(alerts.values()) == 103.13

    # 13 duplicate reading_id trong samples không đổi kết quả vì max là idempotent
    unique = list({r["reading_id"]: r for r in rows}.values())
    assert len(unique) == 2700
    assert alert_windows(unique) == alerts
