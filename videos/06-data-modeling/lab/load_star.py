"""Chạy một file .sql (nhiều câu, phân cách bằng ;) trên DuckDB rồi in số dòng mỗi bảng/view. Dùng cho lab #06.

    uv run python videos/06-data-modeling/lab/load_star.py videos/06-data-modeling/lab/star_schema.sql
    uv run python videos/06-data-modeling/lab/load_star.py videos/06-data-modeling/lab/solution/star_schema.sql --db lake-local/star.duckdb

--db: ghi ra file .duckdb (đã gitignore) để mở lại bằng Python/DBeaver và tự query.
Đường dẫn trong SQL là tương đối so với root repo (data/samples/...), nên script chdir về đó.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[3]


def load(sql_path: Path, db: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Thực thi toàn bộ file SQL trên một connection mới và trả về connection đó."""
    os.chdir(REPO)
    con = duckdb.connect(db)
    con.execute(sql_path.read_text(encoding="utf-8"))
    return con


def table_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    names = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY 1"
        ).fetchall()
    ]
    return {n: con.execute(f'SELECT count(*) FROM "{n}"').fetchall()[0][0] for n in names}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("sql", type=Path)
    ap.add_argument("--db", default=":memory:")
    args = ap.parse_args()
    if args.db != ":memory:":
        Path(args.db).parent.mkdir(parents=True, exist_ok=True)
        Path(args.db).unlink(missing_ok=True)

    con = load(args.sql.resolve(), args.db)
    for name, n in table_counts(con).items():
        print(f"{name:<32} {n:>8,} dòng")
    if args.db != ":memory:":
        print(f"\nĐã ghi {args.db} — mở lại: duckdb.connect('{args.db}', read_only=True)")


if __name__ == "__main__":
    main()
