"""Lab #23 — tokenize customer_id (và các định danh khác) trước khi đưa lên gold / chia sẻ ra ngoài.

Chạy:
    uv run python videos/23-chi-phi-bao-mat/lab/tokenize_pii.py --value 0959710580 --salt "doi-thanh-bi-mat"

Vì sao không dùng md5(value) trần?
  - Không gian số điện thoại VN chỉ ~7×10^8 giá trị → brute-force toàn bộ trong vài phút trên laptop,
    hoặc tra "rainbow table" sẵn có. md5 trần = mã hoá "giả".
  - Hash trần còn cho phép JOIN giữa hai dataset khác nhau cùng hash — rò rỉ qua liên kết.
Salt bí mật (giữ ngoài dữ liệu, ví dụ trong Secrets Manager) phá cả hai điểm trên.
Chuẩn đúng là HMAC-SHA256(key=salt, msg=value) — hmac có sẵn trong stdlib.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac


def md5_plain(value: str) -> str:
    """Cách SAI — giữ ở đây để demo brute-force trong test."""
    return hashlib.md5(value.encode()).hexdigest()  # noqa: S324


def tokenize(value: str, salt: str, length: int = 16) -> str:
    """Token ổn định (cùng value + salt → cùng token), không đảo ngược được nếu không biết salt.

    length=16 hex = 64 bit — đủ để không va chạm trong vài tỉ bản ghi, ngắn hơn để join nhanh.
    """
    # TODO: hmac.new(key=salt.encode(), msg=value.encode(), digestmod=hashlib.sha256).hexdigest()[:length]
    raise NotImplementedError


def brute_force_md5(target_hex: str, prefix: str, digits: int) -> str | None:
    """Thử mọi số `prefix + <digits chữ số>` tới khi md5 khớp. Minh hoạ md5 trần vô dụng với số điện thoại."""
    # TODO: for n in range(10**digits): candidate = f"{prefix}{n:0{digits}d}"; nếu md5_plain(candidate) == target_hex → return
    raise NotImplementedError


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--value", required=True)
    p.add_argument("--salt", required=True)
    a = p.parse_args()
    print("md5 plain :", md5_plain(a.value))
    print("token     :", tokenize(a.value, a.salt))


if __name__ == "__main__":
    main()
