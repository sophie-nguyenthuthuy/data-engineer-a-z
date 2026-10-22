"""Chạy một file .sql trên DuckDB, từng câu một, in kết quả. Dùng cho lab #03 (và #06).

    uv run python videos/03-sql-cho-data-engineer/lab/run_sql.py videos/03-sql-cho-data-engineer/lab/queries.sql

Quy ước file .sql: mỗi câu bắt đầu bằng một dòng comment nhãn `-- Q1 ...`, `-- Q2b ...`.
Câu nào còn chữ TODO thì bỏ qua (in "chưa làm"). Câu lỗi thì in lỗi và chạy tiếp câu sau.
Đường dẫn trong SQL là tương đối so với root repo (data/samples/...), nên script chdir về đó.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[3]
LABEL_RE = re.compile(r"^--\s*(Q\d+\w*)\b.*$", re.MULTILINE)


def split_blocks(sql_text: str) -> list[tuple[str, str]]:
    """Trả về [(nhãn, sql)] — bỏ dòng comment, bỏ dấu ; cuối."""
    parts = LABEL_RE.split(sql_text)
    blocks: list[tuple[str, str]] = []
    # parts = [phần trước nhãn đầu, nhãn1, thân1, nhãn2, thân2, ...]
    for label, body in zip(parts[1::2], parts[2::2], strict=True):
        lines = [ln for ln in body.splitlines() if not ln.strip().startswith("--")]
        sql = "\n".join(lines).strip().rstrip(";").strip()
        blocks.append((label, sql))
    return blocks


def run_file(
    path: Path, con: duckdb.DuckDBPyConnection | None = None, verbose: bool = True
) -> dict[str, list[tuple]]:
    """Chạy mọi câu trong file, trả về {nhãn: rows}. Câu TODO hoặc lỗi không có trong kết quả."""
    con = con or duckdb.connect()
    os.chdir(REPO)
    results: dict[str, list[tuple]] = {}
    for label, sql in split_blocks(path.read_text(encoding="utf-8")):
        if not sql or "TODO" in sql:
            if verbose:
                print(f"\n== {label}: chưa làm (TODO)")
            continue
        try:
            rel = con.execute(sql)
            rows = rel.fetchall()
            cols = [d[0] for d in rel.description] if rel.description else []
        except duckdb.Error as e:  # in lỗi rồi đi tiếp để học viên sửa từng câu
            if verbose:
                print(f"\n== {label}: LỖI {type(e).__name__}: {e}")
            continue
        results[label] = rows
        if verbose:
            print(f"\n== {label}: {len(rows)} dòng")
            if cols:
                print(" | ".join(cols))
            for r in rows[:12]:
                print(" | ".join(str(v) for v in r))
            if len(rows) > 12:
                print(f"... ({len(rows) - 12} dòng nữa)")
    return results


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    run_file(Path(sys.argv[1]).resolve())
