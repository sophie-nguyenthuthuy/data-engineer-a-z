# data/samples

Bộ dữ liệu mẫu nhỏ (< 2 MB) để mọi lab chạy được ngay, không cần chạy generator.
Sinh bằng `make generate` (deterministic, seed 42) — **đừng sửa tay**, hãy sửa generator rồi chạy lại.

| File | Dòng | Khoảng thời gian | Ghi chú |
|------|------|------------------|---------|
| `stores.csv` | 30 | — | 15 quận Hà Nội + 15 quận TP.HCM |
| `menu_items.csv` | 23 | — | giá VND |
| `customers.csv` | 2 000 | — | có PII (phone, email) |
| `pos_transactions.csv` | ~3 000 | 14–19/02/2026 | **có Tết** (mùng 1 = 17/02) → doanh thu rơi ~90% |
| `pos_line_items.csv` | ~5 200 | 14–19/02/2026 | |
| `app_events.jsonl` | ~2 600 | 16–17/02/2026 | clickstream app, `order_id` khớp POS |
| `machines.csv` | 60 | — | 2 máy / cửa hàng |
| `iot_readings.csv` | ~2 700 | 16/02/2026 06:00–06:45 | mỗi 60 s / máy |

Dữ liệu có **lỗi cố ý ~1.5%** (duplicate, `store_id` NULL, timestamp thiếu offset, quantity âm, bản ghi tới muộn). Danh sách đầy đủ: [../generator/SCHEMA.md](../generator/SCHEMA.md).

`sql/` chứa schema + lệnh COPY mà Postgres chạy tự động lần đầu khởi động (`make up`). Nạp lại bằng `make seed`.

Muốn nhiều dữ liệu hơn (90 ngày, đủ 30 cửa hàng, ~500 k giao dịch):

```bash
uv run coffeegen pos --days 90 --out ./out
uv run coffeegen app-events --days 90 --out ./out
uv run coffeegen iot --hours 16 --out ./out
```
