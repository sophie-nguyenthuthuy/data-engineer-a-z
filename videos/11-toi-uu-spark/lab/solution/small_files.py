"""Lời giải lab #11 — bài toán "small files": 1 file vs N file nhỏ, cùng dữ liệu. Chạy offline (polars + pyarrow).

uv run python videos/11-toi-uu-spark/lab/solution/small_files.py --out /tmp/small_files --files 500
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

import polars as pl
import pyarrow.dataset as ds

REPO = Path(__file__).resolve().parents[4]
SAMPLES = REPO / "data" / "samples"


def load_source() -> pl.DataFrame:
    return pl.read_csv(SAMPLES / "pos_transactions.csv", null_values=[""]).with_columns(
        pl.col("total_amount_vnd", "discount_vnd").cast(pl.Int64)
    )


def write_one_file(df: pl.DataFrame, out: Path) -> Path:
    out.mkdir(parents=True, exist_ok=True)
    path = out / "part-000.parquet"
    df.write_parquet(path, compression="zstd")
    return path


def write_many_files(df: pl.DataFrame, out: Path, n_files: int) -> Path:
    """Chia df thành n_files mảnh ~ bằng nhau, mỗi mảnh một file — y như một job Spark có n_files task ghi ra."""
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    for i in range(n_files):
        start, end = (
            i * df.height // n_files,
            (i + 1) * df.height // n_files,
        )  # 2481 dòng / 500 file → 4–5 dòng/file
        df.slice(start, end - start).write_parquet(
            out / f"part-{i:05d}.parquet", compression="zstd"
        )
    return out


def dir_stats(folder: Path) -> dict:
    files = sorted(folder.glob("*.parquet"))
    return {"files": len(files), "bytes": sum(f.stat().st_size for f in files)}


def time_read(glob_pattern: str, repeat: int) -> dict[str, float]:
    """ms tốt nhất trong `repeat` lần cho 2 kiểu đọc: polars scan (collect) và pyarrow dataset (to_table)."""

    def polars_read() -> None:
        pl.scan_parquet(glob_pattern).select(pl.sum("total_amount_vnd")).collect()

    def arrow_read() -> None:
        ds.dataset(str(Path(glob_pattern).parent), format="parquet").to_table(
            columns=["total_amount_vnd"]
        )

    out = {}
    for name, fn in (("polars_ms", polars_read), ("pyarrow_ms", arrow_read)):
        best = float("inf")
        for _ in range(repeat):
            t0 = time.perf_counter()
            fn()
            best = min(best, (time.perf_counter() - t0) * 1000)
        out[name] = round(best, 2)
    return out


def compact(many_dir: Path, out: Path) -> Path:
    """Cách sửa: đọc hết file nhỏ, ghi lại thành 1 file — Spark là `.coalesce(1)` / `.repartition(k)` trước khi write."""
    out.mkdir(parents=True, exist_ok=True)
    path = out / "part-000.parquet"
    pl.scan_parquet(str(many_dir / "*.parquet")).collect().write_parquet(path, compression="zstd")
    return path


def run(out: Path, n_files: int, repeat: int = 3) -> dict:
    df = load_source()
    one = write_one_file(df, out / "one")
    many = write_many_files(df, out / "many", n_files)
    compacted = compact(many, out / "compacted")

    result = {
        "rows": df.height,
        "one_file": {**dir_stats(one.parent), **time_read(str(one), repeat)},
        "many_files": {**dir_stats(many), **time_read(str(many / "*.parquet"), repeat)},
        "compacted": {**dir_stats(compacted.parent), **time_read(str(compacted), repeat)},
    }
    result["bytes_overhead_x"] = round(
        result["many_files"]["bytes"] / result["one_file"]["bytes"], 1
    )
    result["polars_slowdown_x"] = round(
        result["many_files"]["polars_ms"] / result["one_file"]["polars_ms"], 1
    )
    return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default="/tmp/small_files")
    p.add_argument("--files", type=int, default=500)
    p.add_argument("--repeat", type=int, default=3)
    a = p.parse_args()
    print(json.dumps(run(Path(a.out), a.files, a.repeat), indent=2))


if __name__ == "__main__":
    main()
