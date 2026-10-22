# coffeegen

Bộ sinh dữ liệu giả lập cho chuỗi cà phê 30 cửa hàng — nguồn dữ liệu của toàn khoá học.
Chỉ dùng thư viện chuẩn Python, **deterministic theo `--seed`**: cùng tham số → cùng byte output.

```bash
coffeegen pos        --days 90 --stores 30 --seed 42 --out ./out   # stores, menu_items, customers, pos_transactions, pos_line_items
coffeegen app-events --days 90 --seed 42 --out ./out                # app_events.jsonl (khớp order_id với POS cùng seed/ngày)
coffeegen iot        --machines 60 --hz 0.2 --hours 24 --out ./out  # iot_readings.csv
coffeegen stream     --topic pos.transactions --bootstrap localhost:9092 --rate 5   # Kafka producer (video #13)
coffeegen stream     --source iot --dry-run                          # in ra stdout, không cần Kafka
```

Schema đầy đủ và danh sách lỗi dữ liệu cố ý (1–2%): [SCHEMA.md](SCHEMA.md).

Mẹo: `make generate` ở root repo tái tạo `data/samples/` với tham số nhỏ đã chốt.
