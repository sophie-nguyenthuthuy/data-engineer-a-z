# #14 — Exactly-once / at-least-once, DLQ, Schema Registry

📺 Video: `TODO:youtube-link` · 📑 Slide: [V14-Khong-mat-khong-trung.pdf](slides/V14-Khong-mat-khong-trung.pdf)

## Bạn sẽ học được gì
- Phân biệt **at-most-once / at-least-once / exactly-once** bằng một câu hỏi duy nhất: *commit offset trước hay sau khi xử lý?*
- Biết duplicate sinh ra ở đâu (producer retry, consumer crash trước commit) và cách rẻ nhất để sống chung: **consumer idempotent**.
- Hiểu idempotent producer và Kafka transactions làm được gì, và vì sao "exactly-once end-to-end" cuối cùng vẫn là chuyện của **sink**.
- Thiết kế **Dead Letter Queue**: message hỏng không làm sập pipeline, không bị xoá, có lý do đi kèm.
- Dùng **Schema Registry**: đăng ký Avro schema cho `pos.transactions`, thấy registry từ chối thay đổi không tương thích.

## Yêu cầu trước khi xem
- Đã làm lab #13 (topic `pos.transactions` 3 partition, consumer Python chạy được).
- `make up PROFILE=streaming` đang chạy. Lệnh Python dùng `uv run --with confluent-kafka ...` như #13.
- Không có `fastavro` trong `.venv` — phần schema validate offline chỉ dùng `json` chuẩn; serialize Avro thật làm trong Jupyter (có `fastavro`, `confluent-kafka`).

## Ba mức đảm bảo — nói ngắn
| Mức | Consumer làm gì | Khi crash giữa chừng | Dùng khi |
|-----|-----------------|----------------------|----------|
| at-most-once | commit offset **trước** khi xử lý | mất message | log/metric không quan trọng |
| at-least-once | xử lý **xong** rồi commit (lab #13) | xử lý lại → **duplicate** | hầu hết pipeline; sink phải chịu được dup |
| exactly-once | producer idempotent + transaction, consumer `read_committed`, **sink idempotent** | không mất, không dup | tiền, tồn kho, bất cứ gì cộng dồn |

Kafka cho bạn hai công cụ phía producer: `enable.idempotence=true` (broker nhận diện retry nhờ producer id + sequence number → không ghi 2 lần **trong một session**), và transactions (`transactional.id`, `begin/commit_transaction`) để consume-transform-produce **nguyên tử** giữa các topic Kafka. Nhưng khi sink là Postgres, Iceberg, DuckDB… Kafka không kiểm soát được, nên exactly-once thực tế = at-least-once + **ghi theo key (upsert)**. Đó là lý do #17 dùng `MERGE INTO`.

## Lab: sống chung với duplicate, message hỏng và schema

### Bước 1 — Đếm duplicate đang có trong topic
`coffeegen` cố ý gửi lại ~0,4% hoá đơn (giả lập POS retry khi mạng chập chờn). Ngày 15/02 `--scale 0.1`: 4 duplicate trong 1 008 message. Nếu topic đã trống (đã `make reset`), bắn lại:
```bash
uv run --with confluent-kafka coffeegen stream --topic pos.transactions --start 2026-02-15 --scale 0.1 --rate 0
```

### Bước 2 — Consumer idempotent
Mở [`lab/idempotent_consumer.py`](lab/idempotent_consumer.py): giống consumer #13 nhưng có một tập `seen` các `transaction_id` đã xử lý. Điền TODO rồi chạy với **group mới** để đọc lại từ đầu:
```bash
uv run --with confluent-kafka python videos/14-delivery-semantics-dlq-schema-registry/lab/idempotent_consumer.py \
  --group lab14-idem --from-beginning
```
Để producer chạy hết rồi Ctrl+C: chương trình in số message, số duplicate đã bỏ, doanh thu **có** và **không** dedup.

Câu hỏi để tự trả lời: tập `seen` nằm trong RAM — restart consumer thì sao? Spark (#15) giải quyết bằng state + watermark; với consumer tay bạn dùng SQLite/Redis có TTL (bài tập 1).

### Bước 3 — Dead Letter Queue
Mở [`lab/dlq_consumer.py`](lab/dlq_consumer.py). Nhiệm vụ: với mỗi message, `validate()` trả về event hợp lệ **hoặc** một lý do (`invalid_json`, `null_store`, `negative_amount`, `no_items`); message hỏng được gửi sang topic `pos.transactions.dlq` kèm **header** `reason`, `source_topic`, `source_partition`, `source_offset`. Chạy:
```bash
uv run --with confluent-kafka python videos/14-delivery-semantics-dlq-schema-registry/lab/dlq_consumer.py \
  --group lab14-dlq --from-beginning
```
Trong lúc nó chạy, bơm một message JSON hỏng (key `HN01`, phân cách `|` vì JSON có dấu `:`):
```bash
echo 'HN01|{"broken": true' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:29092 --topic pos.transactions --property parse.key=true --property key.separator='|'
```
Xem DLQ với header:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic pos.transactions.dlq --from-beginning --property print.key=true --property print.headers=true --timeout-ms 5000
```
Nguyên tắc DLQ: **không sửa** message gốc, **không xoá**, lý do đi kèm message, có người (hoặc job) đọc DLQ định kỳ. Bảng `quarantine_*` ở silver (#07) là cùng ý tưởng cho batch.

### Bước 4 — Viết Avro schema cho `pos.transactions`
Mở [`lab/pos_transaction.avsc`](lab/pos_transaction.avsc) — mình để sẵn 4 field, bạn bổ sung phần còn lại theo [SCHEMA.md](../../data/generator/SCHEMA.md) và message thật ở #13 bước 3. Quy tắc Avro cần nhớ: field nullable là union `["null", "string"]` **kèm** `"default": null`; `items` là `array` của `record`.

Kiểm tra offline (so với lời giải):
```bash
uv run pytest videos/14-delivery-semantics-dlq-schema-registry -q
```

### Bước 5 — Đăng ký schema và thử phá tương thích
```bash
curl -s localhost:8081/subjects            # [] — chưa có gì
curl -s localhost:8081/config              # {"compatibilityLevel":"BACKWARD"}

LAB=videos/14-delivery-semantics-dlq-schema-registry/lab/solution
bash $LAB/register_schema.sh $LAB/pos_transaction.avsc                 # → {"id":1}
bash $LAB/register_schema.sh $LAB/pos_transaction_v2_compatible.avsc   # thêm field có default → {"id":2}
bash $LAB/register_schema.sh $LAB/pos_transaction_v2_incompatible.avsc # đổi kiểu total_amount_vnd → 409
curl -s localhost:8081/subjects/pos.transactions-value/versions        # [1,2]
```
Chỉ **kiểm tra** mà không đăng ký: `CHECK=1 bash $LAB/register_schema.sh <file>` → `{"is_compatible":false}`.

Subject đặt theo quy ước `<topic>-value`. Message Avro thật trên Kafka có 5 byte đầu: magic byte `0` + schema id 4 byte — consumer đọc id, hỏi registry, decode. Kafka UI (:8080) đã nối với registry nên tự decode cho bạn.

> Thử nghiệm nhỏ: sửa `coffeegen/export.py` để `txn_event` gửi `"is_rainy_day": "false"` (chuỗi, giống CSV) thay cho boolean. Nếu producer serialize bằng Avro với schema này, nó **fail ngay lúc gửi** — đó chính là giá trị của registry: lỗi lộ ra ở producer, không phải 3 tuần sau trong dashboard.

## Kết quả mong đợi
- Bước 2 (1 008 message): `processed=1008 duplicates=4`, doanh thu không dedup **97.494.000 VND**, dedup **96.964.000 VND** (chênh 530.000 VND = 4 hoá đơn gửi 2 lần).
- Bước 3: 7 message `null_store` + 1 `invalid_json` bạn vừa bơm → topic `pos.transactions.dlq` có **8 message**, header hiển thị `reason:null_store,source_topic:pos.transactions,...`. Nếu đã bắn dữ liệu nhiều lần ở #13 thì số `null_store` là bội số của 7.
- Bước 5: id 1, id 2, rồi HTTP 409 `Schema being registered is incompatible with an earlier schema`.
- Test offline `test_v14_avro_schema.py`: `4 passed`.

Output thật (topic vừa tạo lại, 1 008 message):
```
$ idempotent_consumer.py --group lab14-idem --from-beginning        (Ctrl+C sau khi hết message)
dup  HN15-20260215-00007  p0@75
dup  HN12-20260215-00011  p2@188
dup  SG12-20260215-00011  p1@293
dup  HN06-20260215-00010  p1@358
^C
processed=1008 duplicates=4 distinct=1004
doanh thu không dedup: 97.494.000 VND
doanh thu dedup      : 96.964.000 VND

$ dlq_consumer.py --group lab14-dlq --from-beginning
→ DLQ  reason=null_store  p2@67
→ DLQ  reason=null_store  p2@87
...
→ DLQ  reason=null_store  p0@218
→ DLQ  reason=invalid_json  p0@345            ← message bạn vừa bơm
^C
 {'ok': 1001, 'null_store': 7, 'invalid_json': 1}

$ kafka-console-consumer.sh --topic pos.transactions.dlq --from-beginning --property print.headers=true ...
reason:null_store,source_topic:pos.transactions,source_partition:1,source_offset:24		{"transaction_id": "SG10-20260215-00017", "store_id": null, ...}
reason:invalid_json,source_topic:pos.transactions,source_partition:0,source_offset:345	HN01	{"broken": true
Processed a total of 8 messages

$ bash register_schema.sh pos_transaction.avsc                 → {"id":1}
$ bash register_schema.sh pos_transaction_v2_compatible.avsc   → {"id":2}
$ bash register_schema.sh pos_transaction_v2_incompatible.avsc → HTTP 409
{"error_code":409,"message":"Schema being registered is incompatible with an earlier schema for subject \"pos.transactions-value\", details: [{errorType:'TYPE_MISMATCH', description:'The type (path '/fields/7/type') of a field in the new schema does not match ...
$ CHECK=1 bash register_schema.sh pos_transaction_v2_incompatible.avsc → {"is_compatible":false}
$ curl -s localhost:8081/subjects/pos.transactions-value/versions → [1,2]
```
Để ý `source_offset` trong header: 7 message `null_store` có offset khác nhau ở 3 partition — đó là địa chỉ để quay lại message gốc trong topic nguồn khi cần replay. Registry chỉ ra *đúng field* bị đổi kiểu (`/fields/7/type` = `total_amount_vnd`). Nếu port 8081 trên máy bạn bị chiếm (`SCHEMA_REGISTRY_PORT` trong `.env`), truyền registry URL làm tham số thứ 3 cho `register_schema.sh`.

## Câu hỏi phỏng vấn của video
> "Pipeline của bạn đọc Kafka rồi ghi doanh thu vào Postgres. Sếp bảo 'phải exactly-once'. Bạn bật `enable.idempotence=true` và transactions là xong chưa? Nếu chưa, phần còn thiếu nằm ở đâu và bạn làm thế nào với bảng Postgres đó?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Làm `idempotent_consumer.py` sống qua restart: lưu tập `seen` vào SQLite (`transaction_id`, `first_seen_at`) và chỉ giữ 24 giờ gần nhất. Vì sao không giữ mãi?
2. Thêm luật validate thứ 5: `ts` thiếu offset `+07:00` (lỗi `tz_drift`, 5 message trong ngày 15/02). Theo bạn loại này nên vào DLQ hay nên **sửa** rồi cho đi tiếp? Lập luận dưới comment.
3. Viết `app_event.avsc` cho topic `app.events`. Field `props` thay đổi theo `event_type` — bạn chọn `map<string>`, union nhiều record, hay chuỗi JSON? Nêu được/mất của từng cách rồi mở Discussion.
