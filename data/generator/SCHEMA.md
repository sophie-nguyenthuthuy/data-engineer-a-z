# Schema dữ liệu chuỗi cà phê "Phin Phố"

Ba nguồn dữ liệu, mô phỏng đúng những gì một chuỗi 30 cửa hàng thật có:

| Nguồn | Hệ thống thật tương ứng | Dạng | Lệnh sinh |
|-------|--------------------------|------|-----------|
| **POS** | Máy tính tiền tại quầy → database OLTP (Postgres) | CSV / bảng Postgres | `coffeegen pos` |
| **App events** | App đặt hàng gửi clickstream về backend | JSONL (1 event / dòng) | `coffeegen app-events` |
| **IoT** | Cảm biến máy pha espresso gửi telemetry qua MQTT/Kafka | CSV | `coffeegen iot` |

Timestamp luôn là ISO-8601 kèm offset `+07:00` (Asia/Ho_Chi_Minh, không có DST) — **trừ** các dòng bị lỗi `tz_drift` (xem cuối trang).
Tiền tệ: VND, số nguyên, không có phần thập phân.

---

## Dimension tables

### `stores` (30 dòng)
| Field | Type | Mô tả |
|-------|------|-------|
| `store_id` | text, PK | `HN01`…`HN15` (Hà Nội), `SG01`…`SG15` (TP.HCM) |
| `store_name` | text | "Phin Phố " + tên quận |
| `city` | text | `Hà Nội` \| `TP.HCM` |
| `district` | text | quận |
| `opened_date` | date | ngày khai trương |
| `seats` | int | số chỗ ngồi |
| `tier` | numeric | hệ số lưu lượng 0.6–1.5 (cửa hàng trung tâm đông hơn) |

### `menu_items` (23 dòng)
| Field | Type | Mô tả |
|-------|------|-------|
| `item_id` | text, PK | `CF01`… (coffee), `TE..` (tea), `MT..` (milk tea), `OD..` (other drink), `FD..` (food) |
| `item_name` | text | tiếng Việt: Cà phê sữa đá, Bạc xỉu, Trà đào cam sả… |
| `category` | text | `coffee` \| `tea` \| `milk_tea` \| `other_drink` \| `food` |
| `price_vnd` | int | giá size S |
| `cost_vnd` | int | giá vốn (để tính margin ở gold layer) |

Size phụ thu: `S` +0, `M` +5 000, `L` +10 000 (food luôn `S`).

### `customers` (2 000 dòng)
| Field | Type | Mô tả |
|-------|------|-------|
| `customer_id` | text, PK | `C000001`… |
| `full_name` | text | **PII** |
| `phone` | text | **PII** — 10 số bắt đầu 09 |
| `email` | text | **PII** — luôn domain example.com |
| `city` | text | |
| `joined_date` | date | |
| `tier` | text | `bronze` \| `silver` \| `gold` |

### `machines` (60 dòng, chỉ sinh cùng IoT)
| Field | Type | Mô tả |
|-------|------|-------|
| `machine_id` | text, PK | `M001`… — 2 máy / cửa hàng |
| `store_id` | text, FK | |
| `model` | text | |
| `installed_date` | date | |

---

## Fact: `pos_transactions`
Một dòng = một hoá đơn.

| Field | Type | Mô tả |
|-------|------|-------|
| `transaction_id` | text | `{store_id}-{YYYYMMDD}-{seq:05d}` — **không unique** (có duplicate cố ý) |
| `store_id` | text, nullable | **có NULL cố ý** |
| `ts` | timestamptz | thời điểm thanh toán, `+07:00` (một số dòng thiếu offset) |
| `channel` | text | `store` (tại quầy) \| `app` (đặt qua app) |
| `payment_method` | text | `cash` \| `momo` \| `zalopay` \| `vnpay` \| `card` \| `app_wallet` — app không bao giờ `cash` |
| `customer_id` | text, nullable | NULL với khách vãng lai; luôn có khi `channel = app` |
| `staff_id` | text | `{store_id}-S{1..6}` |
| `total_amount_vnd` | bigint | = Σ `line_total_vnd` − `discount_vnd` |
| `discount_vnd` | int | 0 hoặc 10k/15k/20k (chỉ app, ~25% đơn) |
| `is_rainy_day` | bool | ngày mưa tại thành phố đó — lưu ở đây cho tiện lab, thực tế sẽ là bảng weather riêng |

## Fact: `pos_line_items`
Một dòng = một món trong hoá đơn.

| Field | Type | Mô tả |
|-------|------|-------|
| `line_id` | text | `{transaction_id}-{n}` |
| `transaction_id` | text, FK | |
| `item_id` | text, FK | |
| `size` | text | `S` \| `M` \| `L` |
| `quantity` | int | **có số âm cố ý** |
| `unit_price_vnd` | int | = `price_vnd` + phụ thu size |
| `line_total_vnd` | int | = `quantity × unit_price_vnd` |

## Event: `app_events.jsonl`
Một dòng JSON = một event. Phễu: `app_open → view_menu → add_to_cart → checkout → order_placed | order_cancelled`.

```json
{"event_id":"uuid","ts":"2026-02-16T07:41:02+07:00","ingested_at":"2026-02-16T07:41:03+07:00",
 "user_id":"C001234","session_id":"uuid","event_type":"order_placed","store_id":"HN01",
 "platform":"android","app_version":"3.9.0",
 "props":{"order_id":"HN01-20260216-00042","amount_vnd":74000,"discount_vnd":0}}
```

| Field | Type | Mô tả |
|-------|------|-------|
| `event_id` | uuid | |
| `ts` | timestamptz | event time (lúc user bấm) |
| `ingested_at` | timestamptz | processing time (lúc backend nhận) — thường trễ 0–2 s, **~1% trễ 1–10 phút** |
| `user_id` | text | `C……` (đã đăng nhập) hoặc `anon-xxxxxxxx` |
| `session_id` | uuid | |
| `event_type` | text | 6 giá trị ở trên |
| `store_id` | text, nullable | NULL ở `app_open` |
| `platform` | text | `android` \| `ios` |
| `app_version` | text | |
| `props` | object | tuỳ `event_type`: `add_to_cart` → `item_id, qty, size`; `checkout` → `cart_items, payment_method`; `order_placed` → `order_id, amount_vnd, discount_vnd`; `order_cancelled` → `reason` |

`props.order_id` **join được** với `pos_transactions.transaction_id` khi `channel = app`, miễn là sinh cùng `--seed/--start/--stores/--scale`.

## Telemetry: `iot_readings.csv`
| Field | Type | Mô tả |
|-------|------|-------|
| `reading_id` | text | |
| `machine_id` | text, FK | |
| `store_id` | text, FK | |
| `ts` | timestamptz | event time (cảm biến đo) |
| `ingested_at` | timestamptz | processing time — **~1% trễ 1–10 phút** |
| `boiler_temp_c` | float, nullable | ~92 °C, tăng khi pha liên tục; **~0.3% NULL** (glitch) |
| `pump_pressure_bar` | float | ~9 bar |
| `water_flow_ml_s` | float | 0 khi máy nghỉ |
| `shots_total` | int | bộ đếm tăng dần (odometer) |
| `status` | text | `ok` \| `warn` (temp > 95.5 hoặc pressure < 8.3) \| `error` (~0.2%) |
| `firmware` | text | |

---

## Hiệu ứng theo thời gian (để bạn thấy trong biểu đồ)
- **Giờ cao điểm** 7–9 h và 14–16 h; đóng cửa 23–5 h.
- **Cuối tuần**: ×1.1 (Hà Nội), ×1.2 (TP.HCM).
- **Tết**: 7 ngày trước tăng dần tới ×1.5; mùng 1–3 còn ×0.15; mùng 4–6 ×0.5. Mùng 1 các năm: 2025-01-29, **2026-02-17**, 2027-02-06.
- **Ngày mưa**: tổng khách ×0.85, tỉ lệ đặt app ×1.3, ca chiều dịch muộn hơn. Xác suất mưa theo mùa (Hà Nội cao nhất tháng 7–8, TP.HCM tháng 6–10).

## Lỗi dữ liệu cố ý (~1.5% dòng)
| Mã | Ở đâu | Chuyện gì | Video dạy cách xử lý |
|----|-------|-----------|----------------------|
| `duplicate` | POS, app, IoT | cùng bản ghi xuất hiện 2 lần, cách nhau vài dòng (retry khi mạng chập chờn) | #03 (window function), #07, #14 |
| `null_store` | POS | `store_id` rỗng (máy POS mới chưa cấu hình) | #07, #20 |
| `tz_drift` | POS | `ts` ghi giờ local nhưng **không có** `+07:00` → load vào `timestamptz` sẽ lệch 7 giờ | #03, #07 |
| `negative_qty` | POS line items | `quantity < 0` (nhân viên hoàn hàng sai cách) | #07, #20 |
| `late_event` | app, IoT | `ingested_at` − `ts` = 1–10 phút | #15 (watermark), #18 |
| `null_reading` | IoT | `boiler_temp_c` rỗng | #15, #20 |

Tắt toàn bộ lỗi POS: `coffeegen pos --clean`.

## Tính deterministic
Mỗi (seed, ngày, store) có RNG riêng (`random.Random(f"{seed}:{date}:{store_id}")`), nên:
- cùng tham số → file giống nhau từng byte;
- đổi `--days` không làm thay đổi dữ liệu của những ngày đã có;
- `app-events` tái sinh POS nội bộ để `order_id` khớp mà không cần đọc file.
