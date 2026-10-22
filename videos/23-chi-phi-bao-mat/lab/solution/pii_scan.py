"""Lời giải lab #23 — quét CSV/Parquet tìm cột có vẻ chứa PII (số điện thoại VN, email, họ tên).

    uv run python videos/23-chi-phi-bao-mat/lab/solution/pii_scan.py data/samples
    uv run python videos/23-chi-phi-bao-mat/lab/solution/pii_scan.py lake-local/silver/customers

Cách làm cố ý đơn giản: với mỗi cột kiểu string, tính tỉ lệ giá trị khớp từng pattern.
Tỉ lệ ≥ THRESHOLD → báo. Scanner thương mại (Macie, DLP API) cũng làm vậy, chỉ nhiều pattern hơn.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import polars as pl

THRESHOLD = 0.5  # ≥ 50% giá trị không null khớp → coi là cột PII

PHONE_VN = re.compile(r"^0[3-9]\d{8}$")  # 10 số, đầu số di động VN
EMAIL = re.compile(r"^[\w.+-]+@[\w-]+(\.[\w-]+)+$")
# Họ tên VN: 2–5 từ viết hoa chữ đầu, từ đầu là họ phổ biến. Heuristic — đủ cho dữ liệu khoá học.
SURNAMES = {
    "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng",
    "Bùi", "Đỗ", "Hồ", "Ngô", "Dương", "Lý", "Đinh", "Trịnh", "Mai", "Lâm",
}  # fmt: skip


def looks_like_vn_name(value: str) -> bool:
    words = value.split()
    return 2 <= len(words) <= 5 and words[0] in SURNAMES and all(w[:1].isupper() for w in words)


DETECTORS: dict[str, callable] = {
    "phone_vn": lambda v: bool(PHONE_VN.match(v)),
    "email": lambda v: bool(EMAIL.match(v)),
    "person_name": looks_like_vn_name,
}


@dataclass(frozen=True)
class Finding:
    file: str
    column: str
    kind: str
    ratio: float  # tỉ lệ giá trị khớp
    sample: str  # một giá trị mẫu ĐÃ che — báo cáo không được in PII ra

    def line(self) -> str:
        return f"{self.file}\t{self.column}\t{self.kind}\t{self.ratio:.0%}\t{self.sample}"


def mask(value: str) -> str:
    """Giữ 3 ký tự cuối, phần còn lại thành * — giống macro mask_pii của dbt."""
    keep = value[-3:]
    return "*" * max(len(value) - 3, 0) + keep


def read_any(path: Path) -> pl.DataFrame:
    if path.suffix == ".csv":
        return pl.read_csv(path, infer_schema=False, null_values=[""])
    if path.suffix == ".parquet":
        return pl.read_parquet(path)
    if path.suffix in {".jsonl", ".ndjson"}:
        return pl.read_ndjson(path)
    raise ValueError(f"unsupported file type: {path}")


def scan_frame(df: pl.DataFrame, file_label: str, threshold: float = THRESHOLD) -> list[Finding]:
    findings: list[Finding] = []
    for name, dtype in df.schema.items():
        if dtype != pl.String:
            continue
        values = df[name].drop_nulls().to_list()
        if not values:
            continue
        for kind, detect in DETECTORS.items():
            hits = [v for v in values if detect(v)]
            ratio = len(hits) / len(values)
            if ratio >= threshold:
                findings.append(Finding(file_label, name, kind, ratio, mask(hits[0])))
    return findings


def scan_path(path: Path, threshold: float = THRESHOLD) -> list[Finding]:
    files = (
        [path]
        if path.is_file()
        else sorted(p for p in path.rglob("*") if p.suffix in {".csv", ".parquet", ".jsonl"})
    )
    findings: list[Finding] = []
    for f in files:
        try:
            df = read_any(f)
        except Exception as exc:  # noqa: BLE001 — file hỏng không được chặn cả báo cáo
            print(f"skip {f}: {exc}", file=sys.stderr)
            continue
        findings += scan_frame(df, str(f), threshold)
    return findings


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("path", help="file hoặc thư mục (quét đệ quy)")
    p.add_argument("--threshold", type=float, default=THRESHOLD)
    a = p.parse_args()
    findings = scan_path(Path(a.path), a.threshold)
    print("file\tcolumn\tkind\tmatch\tsample(masked)")
    for f in findings:
        print(f.line())
    print(f"\n{len(findings)} PII column(s) found", file=sys.stderr)
    return 1 if findings else 0  # exit 1 để dùng được làm CI gate


if __name__ == "__main__":
    sys.exit(main())
