"""Lab #23 — quét CSV/Parquet tìm cột có vẻ chứa PII (số điện thoại VN, email, họ tên).

Chạy:
    uv run python videos/23-chi-phi-bao-mat/lab/pii_scan.py data/samples
    uv run python videos/23-chi-phi-bao-mat/lab/pii_scan.py lake-local/silver/customers

Ý tưởng: với mỗi cột kiểu string, tính tỉ lệ giá trị khớp từng pattern. Tỉ lệ ≥ THRESHOLD → báo.
Báo cáo KHÔNG được in PII thật ra — chỉ in giá trị đã che (mask).
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import polars as pl

THRESHOLD = 0.5

# TODO: regex số di động VN — 10 chữ số, bắt đầu 03/05/07/08/09.  Gợi ý: ^0[3-9]\d{8}$
PHONE_VN = re.compile(r"^$")
# TODO: regex email đơn giản (local@domain.tld). Không cần chuẩn RFC.
EMAIL = re.compile(r"^$")
SURNAMES = {
    "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng",
    "Bùi", "Đỗ", "Hồ", "Ngô", "Dương", "Lý", "Đinh", "Trịnh", "Mai", "Lâm",
}  # fmt: skip


def looks_like_vn_name(value: str) -> bool:
    """Heuristic họ tên VN: 2–5 từ, từ đầu nằm trong SURNAMES, mọi từ viết hoa chữ đầu.

    Thử với 'Trần Minh Lan' (True) và 'Phin Phố Ba Đình' (False — tên cửa hàng, không phải người).
    """
    # TODO
    raise NotImplementedError


DETECTORS = {
    "phone_vn": lambda v: bool(PHONE_VN.match(v)),
    "email": lambda v: bool(EMAIL.match(v)),
    "person_name": looks_like_vn_name,
}


@dataclass(frozen=True)
class Finding:
    file: str
    column: str
    kind: str
    ratio: float
    sample: str  # đã mask

    def line(self) -> str:
        return f"{self.file}\t{self.column}\t{self.kind}\t{self.ratio:.0%}\t{self.sample}"


def mask(value: str) -> str:
    """Giữ 3 ký tự cuối, còn lại thành '*' — cùng quy ước với macro mask_pii trong platform/dbt."""
    # TODO
    raise NotImplementedError


def read_any(path: Path) -> pl.DataFrame:
    if path.suffix == ".csv":
        return pl.read_csv(path, infer_schema=False, null_values=[""])
    if path.suffix == ".parquet":
        return pl.read_parquet(path)
    if path.suffix in {".jsonl", ".ndjson"}:
        return pl.read_ndjson(path)
    raise ValueError(f"unsupported file type: {path}")


def scan_frame(df: pl.DataFrame, file_label: str, threshold: float = THRESHOLD) -> list[Finding]:
    """Với mỗi cột String: với mỗi detector, ratio = số giá trị khớp / số giá trị không null."""
    findings: list[Finding] = []
    # TODO: duyệt df.schema.items(); bỏ qua cột không phải pl.String; values = df[name].drop_nulls().to_list()
    #       với mỗi (kind, detect) trong DETECTORS: nếu ratio >= threshold → Finding(file_label, name, kind, ratio, mask(hit đầu tiên))
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
        except Exception as exc:
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
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
