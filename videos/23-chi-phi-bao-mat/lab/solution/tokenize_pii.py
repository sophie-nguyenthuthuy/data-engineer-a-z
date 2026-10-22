"""Lời giải lab #23 — tokenize định danh bằng HMAC-SHA256 + salt; demo vì sao md5 trần vô dụng.

uv run python videos/23-chi-phi-bao-mat/lab/solution/tokenize_pii.py --value 0959710580 --salt "doi-thanh-bi-mat"
"""

from __future__ import annotations

import argparse
import hashlib
import hmac


def md5_plain(value: str) -> str:
    """Cách SAI — giữ lại để demo brute-force."""
    return hashlib.md5(value.encode()).hexdigest()  # noqa: S324


def tokenize(value: str, salt: str, length: int = 16) -> str:
    digest = hmac.new(salt.encode(), value.encode(), hashlib.sha256).hexdigest()
    return digest[:length]


def brute_force_md5(target_hex: str, prefix: str, digits: int) -> str | None:
    for n in range(10**digits):
        candidate = f"{prefix}{n:0{digits}d}"
        if md5_plain(candidate) == target_hex:
            return candidate
    return None


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--value", required=True)
    p.add_argument("--salt", required=True)
    a = p.parse_args()
    print("md5 plain :", md5_plain(a.value))
    print("token     :", tokenize(a.value, a.salt))


if __name__ == "__main__":
    main()
