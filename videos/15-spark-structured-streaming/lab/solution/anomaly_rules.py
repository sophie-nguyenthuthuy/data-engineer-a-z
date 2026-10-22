"""Luật phát hiện bất thường cho máy pha espresso — thuần Python, không Spark.

Cùng ngưỡng với generator (data/generator/src/coffeegen/iot.py) và job Spark iot_anomaly_stream.py.
Tách riêng để test offline trên data/samples/iot_readings.csv; job Spark chỉ là bản "chạy mãi" của hàm
`alert_windows` dưới đây.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

BOILER_TEMP_MAX_C = 95.5
PUMP_PRESSURE_MIN_BAR = 8.3
WINDOW = timedelta(minutes=1)


def classify_reading(boiler_temp_c: float | None, pump_pressure_bar: float | None) -> str:
    """'ok' | 'warn' | 'unknown' (thiếu cảm biến)."""
    if boiler_temp_c is None or pump_pressure_bar is None:
        return "unknown"
    if boiler_temp_c > BOILER_TEMP_MAX_C or pump_pressure_bar < PUMP_PRESSURE_MIN_BAR:
        return "warn"
    return "ok"


def window_start(ts: str | datetime, size: timedelta = WINDOW) -> datetime:
    """Đầu cửa sổ tumbling chứa `ts`, theo UTC — giống `F.window()` của Spark với session tz UTC.

    Timestamp thiếu offset được hiểu là UTC, đúng như Spark sẽ hiểu (đó là lỗi tz_drift ở #15 bước 5).
    """
    dt = datetime.fromisoformat(ts) if isinstance(ts, str) else ts
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    epoch = dt.timestamp()
    size_s = size.total_seconds()
    return datetime.fromtimestamp(epoch - epoch % size_s, tz=UTC)


def alert_windows(
    readings: Iterable[dict],
    threshold: float = BOILER_TEMP_MAX_C,
    size: timedelta = WINDOW,
) -> dict[tuple[str, datetime], float]:
    """(machine_id, window_start) → max boiler_temp_c, chỉ giữ cửa sổ có max > threshold.

    `max` là phép idempotent: duplicate không đổi kết quả — một lý do streaming hay chọn max/min/last
    thay cho sum/count khi nguồn là at-least-once.
    """
    max_temp: dict[tuple[str, datetime], float] = {}
    for r in readings:
        temp = r.get("boiler_temp_c")
        if temp is None:
            continue
        key = (r["machine_id"], window_start(r["ts"], size))
        max_temp[key] = max(max_temp.get(key, float("-inf")), float(temp))
    return {k: v for k, v in max_temp.items() if v > threshold}
