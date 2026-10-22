"""Lab #08 — so sánh CSV / JSONL / Parquet (snappy, zstd) / ORC trên pos_transactions.

Chạy:
    uv run python videos/08-dinh-dang-file/lab/formats_bench.py --out /tmp/formats

Điền các TODO theo thứ tự. Mỗi hàm nhỏ, chạy được riêng lẻ trong notebook nếu bạn muốn thử.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[3]
SAMPLES = REPO / "data" / "samples"
ONE_COLUMN = "total_amount_vnd"


def load_source(scale: int = 1) -> pl.DataFrame:
    """Đọc samples với kiểu dữ liệu thật (không phải bronze toàn string) để so sánh công bằng.

    `scale` nhân bản dữ liệu (2 481 dòng × scale) để chênh lệch thời gian đọc đủ lớn mà đo được.
    """
    df = pl.read_csv(SAMPLES / "pos_transactions.csv", null_values=[""])
    df = pl.concat([df] * scale) if scale > 1 else df
    return df.with_columns(pl.col("total_amount_vnd", "discount_vnd").cast(pl.Int64))


def write_all(df: pl.DataFrame, out: Path) -> dict[str, Path]:
    out.mkdir(parents=True, exist_ok=True)
    files = {
        "csv": out / "pos_transactions.csv",
        "jsonl": out / "pos_transactions.jsonl",
        "parquet_snappy": out / "pos_transactions.snappy.parquet",
        "parquet_zstd": out / "pos_transactions.zstd.parquet",
    }
    df.write_csv(files["csv"])
    # TODO 1: ghi JSONL (gợi ý: df.write_ndjson), Parquet snappy và Parquet zstd
    #   (gợi ý: df.write_parquet(path, compression="snappy" | "zstd"))

    # TODO 2 (tuỳ chọn): ghi ORC bằng `from pyarrow import orc; orc.write_table(df.to_arrow(), path, compression="zstd")`
    #   bọc trong try/except ImportError vì không phải bản pyarrow nào cũng kèm ORC
    return files


def readers(path: Path) -> tuple[Callable[[], pl.DataFrame], Callable[[], pl.DataFrame]]:
    """(đọc cả bảng, đọc 1 cột) cho từng định dạng."""
    if path.suffix == ".csv":
        return (lambda: pl.read_csv(path), lambda: pl.read_csv(path, columns=[ONE_COLUMN]))
    if path.suffix == ".jsonl":
        return (lambda: pl.read_ndjson(path), lambda: pl.read_ndjson(path).select(ONE_COLUMN))
    if path.suffix == ".orc":
        from pyarrow import orc

        return (
            lambda: pl.from_arrow(orc.read_table(path)),
            lambda: pl.from_arrow(orc.read_table(path, columns=[ONE_COLUMN])),
        )
    # TODO 3: Parquet — đọc cả bảng và đọc đúng 1 cột (gợi ý: pl.read_parquet(path, columns=[...]))
    return (lambda: pl.DataFrame(), lambda: pl.DataFrame())


def timeit(fn: Callable[[], pl.DataFrame], repeat: int) -> float:
    """Thời gian tốt nhất (ms) trong `repeat` lần."""
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        best = min(best, (time.perf_counter() - t0) * 1000)
    return best


def bench(files: dict[str, Path], repeat: int = 5) -> list[dict]:
    rows = []
    for fmt, path in files.items():
        read_all, read_one = readers(path)
        rows.append(
            {
                "format": fmt,
                "bytes": path.stat().st_size,
                "read_all_ms": round(timeit(read_all, repeat), 2),
                "read_one_col_ms": round(timeit(read_one, repeat), 2),
            }
        )
    return rows


def parquet_metadata(path: Path) -> dict:
    """Footer Parquet: số row group, thống kê min/max từng cột — nền tảng của predicate pushdown."""
    pf = pq.ParquetFile(path)
    md = pf.metadata
    info: dict = {
        "num_rows": md.num_rows,
        "num_row_groups": md.num_row_groups,
        "num_columns": md.num_columns,
        "created_by": md.created_by,
        "row_groups": [],
    }
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        stats: dict = {}
        # TODO 4: duyệt rg.column(j) với j in range(rg.num_columns); nếu col.statistics.has_min_max
        #   thì lưu {"min": str(stat.min), "max": str(stat.max), "nulls": stat.null_count,
        #   "codec": col.compression} vào stats[col.path_in_schema]
        info["row_groups"].append(
            {"rows": rg.num_rows, "bytes": rg.total_byte_size, "stats": stats}
        )
    return info


def pushdown_demo(df: pl.DataFrame, out: Path) -> dict:
    """Ghi file sort theo store_id với row group nhỏ, rồi lọc 1 store: engine chỉ đọc row group chứa store đó."""
    path = out / "pos_transactions.by_store.parquet"
    df.sort("store_id").write_parquet(path, row_group_size=256, compression="zstd")
    pf = pq.ParquetFile(path)
    hits: list[int] = []
    # TODO 5: với mỗi row group i, lấy statistics của cột store_id
    #   (pf.metadata.row_group(i).column(<index của store_id>).statistics)
    #   nếu stats is not None and stats.has_min_max and stats.min <= "SG14" <= stats.max thì append i
    #   (row group toàn NULL store_id — lỗi null_store sort lên đầu — không có min/max)
    n = pl.scan_parquet(path).filter(pl.col("store_id") == "SG14").select(pl.len()).collect().item()
    return {
        "row_groups_total": pf.metadata.num_row_groups,
        "row_groups_needed_for_SG14": hits,
        "rows_SG14": n,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default="/tmp/formats", help="thư mục ghi file tạm")
    p.add_argument("--repeat", type=int, default=5)
    p.add_argument(
        "--scale", type=int, default=1, help="nhân bản dữ liệu N lần (thử 40 ≈ 100 k dòng)"
    )
    a = p.parse_args()
    out = Path(a.out)

    df = load_source(a.scale)
    files = write_all(df, out)
    results = bench(files, a.repeat)
    csv_bytes = next(r["bytes"] for r in results if r["format"] == "csv")

    print(f"{'format':16}{'bytes':>10}{'vs csv':>8}{'all ms':>10}{'1 col ms':>10}")
    for r in results:
        print(
            f"{r['format']:16}{r['bytes']:>10,}{r['bytes'] / csv_bytes:>8.2f}"
            f"{r['read_all_ms']:>10.2f}{r['read_one_col_ms']:>10.2f}"
        )

    md = parquet_metadata(files["parquet_zstd"])
    print(
        "\nParquet footer:",
        json.dumps({k: v for k, v in md.items() if k != "row_groups"}, indent=2),
    )
    for i, rg in enumerate(md["row_groups"]):
        print(f"  row group {i}: rows={rg['rows']} bytes={rg['bytes']:,}")
        for col in ("ts", "store_id", "total_amount_vnd"):
            if col in rg["stats"]:
                print(f"    {col:18} {rg['stats'][col]}")

    print("\nPredicate pushdown:", json.dumps(pushdown_demo(df, out)))


if __name__ == "__main__":
    main()
