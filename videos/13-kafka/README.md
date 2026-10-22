# #13 — Kafka từ số 0

📺 Video: `TODO:youtube-link`

## Bạn sẽ học được gì
- Hiểu Kafka là gì bằng đúng 5 khái niệm: **topic, partition, offset, producer, consumer group** — và vì sao key của message quyết định thứ tự.
- Tạo topic, bắn 1 000 hoá đơn POS của Phin Phố vào Kafka bằng `coffeegen stream`, nhìn thấy chúng trên Kafka UI.
- Viết consumer Python (confluent-kafka) tính doanh thu luỹ kế theo cửa hàng, tự commit offset.
- Chạy 2 consumer cùng group và thấy Kafka chia partition cho chúng — nền tảng của mọi hệ thống streaming scale ngang.

## Yêu cầu trước khi xem
- Đã xem #05 (Docker) và #07 (ETL batch) — để so sánh "kéo file mỗi đêm" với "nhận từng bản ghi".
- `make up PROFILE=streaming` (thêm ~3 GB RAM: kafka, schema-registry, kafka-ui, debezium). Máy 8 GB: `make down` trước, rồi bật profile này.
- `make install` đã chạy (cần `coffeegen`). Thư viện `confluent-kafka` **không** có trong `.venv` — lab dùng `uv run --with confluent-kafka ...` (uv tải thêm đúng một gói, không sửa `pyproject.toml`), hoặc chạy trong terminal của Jupyter (đã cài sẵn).

## Lab: hoá đơn POS chạy qua Kafka

### Bước 1 — Bật Kafka và kiểm tra
```bash
make up PROFILE=streaming
make ps
```
`kafka`, `schema-registry`, `debezium` phải `healthy`, `kafka-ui` `running`. Kafka ở đây là **một broker, chế độ KRaft** (không có ZooKeeper) — đủ để học, không đủ để chạy production.

### Bước 2 — Tạo topic 3 partition
Kafka trong repo bật `auto.create.topics`, nhưng topic tự tạo chỉ có 1 partition. Mình tạo tay để thấy rõ partition:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 \
  --create --topic pos.transactions --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 \
  --describe --topic pos.transactions
```
Output `--describe` có 3 dòng `Partition: 0/1/2`, `Leader: 1`, `Replicas: 1` — một broker nên replication factor chỉ có thể là 1.

> Trong container, broker lắng nghe ở `localhost:29092`; từ máy bạn là `localhost:9092`. Đây là lỗi hay gặp nhất với Kafka trong Docker: *advertised listener* (xem `KAFKA_ADVERTISED_LISTENERS` trong `docker-compose.yml`).

### Bước 3 — Nhìn message trước khi gửi (`--dry-run`)
```bash
uv run coffeegen stream --dry-run --start 2026-02-15 --scale 0.1 --limit 3
```
Mỗi dòng là `key<TAB>value`: key là `store_id`, value là JSON một hoá đơn **kèm `items`** (nested, khác CSV ở #07 có 2 bảng tách). Ngày 15/02 ở `--scale 0.1` cho đúng **1 008 message**, trong đó 7 message có `store_id` null → key rỗng `""`.

### Bước 4 — Producer: bắn hoá đơn vào topic
```bash
uv run --with confluent-kafka coffeegen stream --topic pos.transactions \
  --start 2026-02-15 --scale 0.1 --rate 5
```
5 message/giây → ~3,5 phút cho cả ngày. Để terminal này chạy. Mở http://localhost:8080 → Topics → `pos.transactions` → tab **Messages**: cột Key là mã cửa hàng; cùng một key luôn rơi vào cùng một partition (Kafka hash key → partition), nên hoá đơn của `HN01` **giữ đúng thứ tự** với nhau.

Đọc code producer ở [`data/generator/src/coffeegen/stream.py`](../../data/generator/src/coffeegen/stream.py): chỉ `Producer({"bootstrap.servers": ...})`, `produce(topic, key, value)`, `poll(0)` và `flush()` cuối cùng.

### Bước 5 — Consumer "chay" trong container
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic pos.transactions --from-beginning --property print.key=true --property print.partition=true --max-messages 5
```
Thấy `Partition:…` và key đi kèm từng message. Consumer này **không có group id** nên không lưu offset — chạy lại vẫn đọc từ đầu.

### Bước 6 — Consumer Python tính doanh thu luỹ kế
Mở [`lab/consumer.py`](lab/consumer.py), điền 4 TODO (cấu hình group, parse JSON + cộng dồn, in kết quả, commit offset), rồi chạy:
```bash
uv run --with confluent-kafka python videos/13-kafka/lab/consumer.py --group lab13 --from-beginning
```
Mỗi message in một dòng `p0@42  HN01   doanh thu luỹ kế …`. Ctrl+C → consumer commit offset rồi thoát. Chạy lại **không** có `--from-beginning`: nó tiếp tục từ offset đã commit, không tính lại từ đầu — đó là ý nghĩa của consumer group.

### Bước 7 — Thí nghiệm 2 consumer cùng group
Mở terminal thứ hai, chạy cùng lệnh ở bước 6 (cùng `--group lab13`). Nhìn log `assigned:` ở cả hai terminal, rồi kiểm tra:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 \
  --describe --group lab13
```
3 partition chia cho 2 consumer (2 + 1). Tắt một consumer → vài giây sau consumer còn lại nhận cả 3 (**rebalance**). Chạy consumer thứ 4 → nó **đứng không**: số consumer hữu ích trong một group ≤ số partition.

### Bước 8 — Test offline (không cần Docker)
```bash
uv run pytest videos/13-kafka -q
```

## Kết quả mong đợi
- `--describe` topic: 3 partition, `ReplicationFactor: 1`.
- `--dry-run` ngày 2026-02-15, `--scale 0.1`: **1 008 message**, 31 key khác nhau (30 cửa hàng + `""`), 7 message key rỗng, 4 `transaction_id` xuất hiện 2 lần (duplicate cố ý, dùng ở #14). Test offline `test_v13_message_format.py` kiểm đúng các số này: `1 passed`.
- Consumer chạy hết 1 008 message (có `--from-beginning`): **tổng doanh thu luỹ kế = 97.494.000 VND**, cửa hàng cao nhất `SG14` = 6.495.000 VND, nhóm `(null)` = 730.000 VND (7 hoá đơn thiếu `store_id`). Con số 97.494.000 **cao hơn thực tế 530.000 VND** vì 4 hoá đơn bị gửi 2 lần — #14 xử lý.
- `kafka-consumer-groups --describe` với 2 consumer: cột `CONSUMER-ID` có 2 giá trị, `LAG` về 0 khi producer dừng.
<!-- TODO(thuy): dán output thật của --describe group và vài dòng consumer sau khi quay -->

## Câu hỏi phỏng vấn của video
> "Topic có 3 partition, team bạn chạy 5 consumer cùng group để 'nhanh hơn'. Chuyện gì xảy ra, và nếu cần thật sự nhanh hơn thì phải làm gì? Tăng partition về sau có rủi ro gì với thứ tự message theo `store_id`?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Sửa `consumer.py` để in thêm **doanh thu theo thành phố** (`HN*` → Hà Nội, `SG*` → TP.HCM). Cần join `stores` không? Vì sao key message giúp bạn trả lời được mà không cần join?
2. Chạy producer với `--source app-events --topic app.events` rồi viết consumer đếm số `order_placed` mỗi phút. Key của topic này là gì (đọc `stream.py`)? Event `app_open` rơi vào partition nào?
3. Tạo topic `pos.transactions.1p` chỉ 1 partition, bắn lại dữ liệu và so thời gian producer `--rate 0` (nhanh nhất có thể) giữa 1 và 3 partition. Có khác biệt không với một broker trên laptop? Giải thích dưới comment / mở Discussion.
