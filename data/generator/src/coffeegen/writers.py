"""CSV / JSONL writers. Stdlib only; UTF-8 without BOM; '\n' line endings."""

from __future__ import annotations

import csv
import json
from collections.abc import Iterable
from pathlib import Path


def write_csv(path: Path, rows: Iterable[dict], fieldnames: list[str], append: bool = False) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append and path.exists() else "w"
    n = 0
    with path.open(mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        if mode == "w":
            w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fieldnames})
            n += 1
    return n


def write_jsonl(path: Path, rows: Iterable[dict], append: bool = False) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append and path.exists() else "w"
    n = 0
    with path.open(mode, encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
            n += 1
    return n
