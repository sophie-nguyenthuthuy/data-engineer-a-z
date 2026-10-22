"""Espresso-machine telemetry: 2 machines per store, readings at `hz` per second.

Signals drift with traffic (more shots at peak hours → boiler temp climbs, pressure dips).
~1% late readings (ingested minutes after event time) and ~0.5% duplicates, for the streaming labs.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from .calendar_effects import day_rng
from .config import HOURLY_PROFILE, VN_TZ, Store
from .dirty import format_ts

MACHINE_MODELS = ["LM Linea PB", "Nuova Simonelli Aurelia", "Victoria Arduino Eagle One"]

IOT_FIELDS = [
    "reading_id",
    "machine_id",
    "store_id",
    "ts",
    "ingested_at",
    "boiler_temp_c",
    "pump_pressure_bar",
    "water_flow_ml_s",
    "shots_total",
    "status",
    "firmware",
]


def machines(stores: list[Store], n_machines: int) -> list[dict]:
    out = []
    for i in range(n_machines):
        store = stores[i % len(stores)]
        out.append(
            {
                "machine_id": f"M{i + 1:03d}",
                "store_id": store.store_id,
                "model": MACHINE_MODELS[i % len(MACHINE_MODELS)],
                "installed_date": (store.opened_date + timedelta(days=(i * 31) % 400)).isoformat(),
            }
        )
    return out


def simulate_iot(
    stores: list[Store], n_machines: int, start: date, hours: float, hz: float, seed: int
):
    """Yield readings in ingestion order (mostly chronological, a few late)."""
    rng = day_rng(seed, start, f"iot:{n_machines}:{hz}")
    machs = machines(stores, n_machines)
    step = timedelta(seconds=1.0 / hz)
    n_steps = int(hours * 3600 * hz)
    t0 = datetime(
        start.year, start.month, start.day, 6, 0, 0, tzinfo=VN_TZ
    )  # machines power on at 06:00

    state = {
        m["machine_id"]: {
            "shots": rng.randrange(10_000, 80_000),
            "temp": 92.0,
            "fw": rng.choice(["2.4.1", "2.4.3", "2.5.0"]),
        }
        for m in machs
    }
    buffer: list[dict] = []
    seq = 0
    for k in range(n_steps):
        ts = t0 + step * k
        load = HOURLY_PROFILE[ts.hour] / max(HOURLY_PROFILE)  # 0..1
        for m in machs:
            st = state[m["machine_id"]]
            # shots pulled since last reading ~ Poisson-ish with load
            new_shots = sum(
                1 for _ in range(4) if rng.random() < load * 0.5 * step.total_seconds() / 30
            )
            st["shots"] += new_shots
            # temp climbs under load, relaxes toward 92
            st["temp"] += 0.6 * new_shots - 0.15 * (st["temp"] - 92.0) + rng.gauss(0, 0.15)
            pressure = 9.0 - 0.15 * new_shots + rng.gauss(0, 0.08)
            flow = 0.0 if new_shots == 0 else round(rng.uniform(1.6, 2.4), 2)
            status = "ok"
            if st["temp"] > 95.5 or pressure < 8.3:
                status = "warn"
            if rng.random() < 0.002:
                status = "error"
                st["temp"] += rng.uniform(3, 8)
            seq += 1
            late = rng.random() < 0.01
            ingested = ts + (
                timedelta(minutes=rng.randrange(1, 10))
                if late
                else timedelta(seconds=rng.randrange(0, 2))
            )
            row = {
                "reading_id": f"{m['machine_id']}-{start:%Y%m%d}-{seq:07d}",
                "machine_id": m["machine_id"],
                "store_id": m["store_id"],
                "ts": format_ts(ts),
                "ingested_at": format_ts(ingested),
                "boiler_temp_c": round(st["temp"], 2),
                "pump_pressure_bar": round(pressure, 2),
                "water_flow_ml_s": flow,
                "shots_total": st["shots"],
                "status": status,
                "firmware": st["fw"],
            }
            if rng.random() < 0.003:  # sensor glitch → null temperature
                row["boiler_temp_c"] = None
            buffer.append(row)
            if rng.random() < 0.005:
                buffer.append(dict(row))  # duplicate
    buffer.sort(key=lambda r: r["ingested_at"])
    yield from buffer
