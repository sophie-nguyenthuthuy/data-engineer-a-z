"""Test lab #23 (PII) — quét data/samples/customers.csv + tokenize. Offline, < 2 s."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from pii_scan import looks_like_vn_name, mask, scan_frame, scan_path
from tokenize_pii import brute_force_md5, md5_plain, tokenize

SAMPLES = Path(__file__).resolve().parents[4] / "data" / "samples"


def test_customers_csv_has_exactly_three_pii_columns():
    findings = scan_path(SAMPLES / "customers.csv")
    by_col = {f.column: f for f in findings}
    assert set(by_col) == {"full_name", "phone", "email"}
    assert by_col["phone"].kind == "phone_vn" and by_col["phone"].ratio == 1.0  # 2000/2000
    assert by_col["email"].kind == "email" and by_col["email"].ratio == 1.0  # 2000/2000
    assert by_col["full_name"].kind == "person_name" and by_col["full_name"].ratio >= 0.95
    # báo cáo không được lộ PII thật
    assert by_col["phone"].sample.startswith("*******") and len(by_col["phone"].sample) == 10
    assert by_col["email"].sample.endswith("com") and "@" not in by_col["email"].sample


def test_whole_samples_dir_only_flags_customers():
    findings = scan_path(SAMPLES)
    assert {Path(f.file).name for f in findings} == {"customers.csv"}
    # stores.csv có "Phin Phố Hoàn Kiếm" — không phải tên người; pos_transactions không có PII dạng text
    assert len(findings) == 3


def test_name_heuristic():
    assert looks_like_vn_name("Trần Minh Lan")
    assert looks_like_vn_name("Nguyễn Thị Hồng Nhung")
    assert not looks_like_vn_name("Phin Phố Ba Đình")
    assert not looks_like_vn_name("Cà phê sữa đá")
    assert not looks_like_vn_name("HN01")


def test_mask_keeps_last_three():
    assert mask("0959710580") == "*******580"
    assert mask("ab") == "ab"


def test_scan_frame_respects_threshold_and_skips_non_string():
    df = pl.DataFrame(
        {
            "mixed": ["0912345678", "hello", "x", "y"],  # 25% phone < 50%
            "amount": [1, 2, 3, 4],
        }
    )
    assert scan_frame(df, "mem") == []
    assert [f.kind for f in scan_frame(df, "mem", threshold=0.25)] == ["phone_vn"]


def test_tokenize_is_stable_and_salt_dependent():
    t1 = tokenize("C000001", "salt-a")
    assert t1 == tokenize("C000001", "salt-a") and len(t1) == 16
    assert t1 != tokenize("C000001", "salt-b")  # salt khác → không join chéo dataset được
    assert t1 != tokenize("C000002", "salt-a")
    assert "C000001" not in t1


def test_md5_without_salt_is_brute_forceable():
    """Số điện thoại chỉ có ~7×10^8 giá trị; ở đây thu nhỏ còn 10^4 để test chạy < 1 s."""
    secret_phone = "0959710580"
    recovered = brute_force_md5(md5_plain(secret_phone), prefix="095971", digits=4)
    assert recovered == secret_phone
