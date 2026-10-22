# #16 — CDC với Debezium

📺 Video: `TODO:youtube-link` · 📑 Slide: [V16-CDC-voi-Debezium.pdf](slides/V16-CDC-voi-Debezium.pdf)

## Bạn sẽ học được gì
- Hiểu **Change Data Capture** là gì và vì sao đọc WAL (log-based) thắng cả hai cách cũ: poll theo `updated_at` và trigger.
- Biết 4 thứ Postgres cần để CDC chạy: `wal_level=logical`, **publication**, **replication slot**, plugin `pgoutput` — và cái nào sẽ làm đầy đĩa nếu bạn quên.
- Đăng ký Debezium connector qua REST, đọc được **envelope** `before / after / op / ts_ms / source`.
- Viết consumer áp dụng CDC vào bảng đích (upsert / delete) một cách **idempotent**, có test offline.
- So sánh thật sự với batch full-load ở #07: khi nào 30 cửa hàng cần CDC, khi nào không.

## Yêu cầu trước khi xem
- Đã xem #07 (batch full-load từ Postgres bằng `coffee-ingest`) và làm lab #13.
- `make up PROFILE=streaming` (Debezium Connect ở :8083 đã nằm trong profile này; Postgres của repo đã bật `wal_level=logical` và tạo sẵn publication `dbz_publication` — xem [`01_schema.sql`](../../data/samples/sql/01_schema.sql)).
- `curl` trên máy. `jq` không bắt buộc (mình dùng `python -m json.tool`).

## CDC trong 5 phút
Mọi thay đổi vào Postgres đều được ghi vào **WAL** trước khi vào bảng. `wal_level=logical` làm WAL đủ chi tiết để một client bên ngoài đọc lại từng dòng thay đổi. Debezium là client đó: nó tạo một **replication slot** (Postgres hứa giữ WAL từ vị trí đó cho tới khi slot xác nhận đã đọc), đọc qua plugin **pgoutput** các bảng nằm trong **publication**, rồi gửi mỗi thay đổi thành một message Kafka vào topic `<prefix>.<schema>.<table>`.

| Cách lấy thay đổi | Thấy DELETE? | Thấy trạng thái trung gian? | Tải lên OLTP | Độ trễ |
|-------------------|--------------|-----------------------------|--------------|--------|
| Full-load mỗi đêm (#07) | không | không | 1 full scan / ngày | 24 h |
| Poll `WHERE updated_at > :last` | không | không | 1 query / phút, cần index | phút |
| Trigger ghi bảng audit | có | có | **mỗi** write chậm hơn | giây |
| Log-based (Debezium) | có | có | ~0 (đọc WAL) | giây |

Cái giá: slot. Nếu connector chết mà slot còn, Postgres **giữ WAL mãi** → đĩa đầy → POS ngừng bán hàng. Khối 4 trong `cdc_demo.sql` cho bạn câu query theo dõi.

## Lab: bắt từng thay đổi của bảng POS

### Bước 1 — Kiểm tra Connect và Postgres
```bash
curl -s localhost:8083/connectors                       # [] — chưa có connector
docker compose exec postgres psql -U coffee -d coffee_pos -c "SHOW wal_level;" -c "\dRp+"
```
`wal_level = logical`; publication `dbz_publication` có 5 bảng.

### Bước 2 — Replica identity
Bảng raw `pos_transactions` **cố ý không có primary key** (có duplicate). pgoutput từ chối UPDATE/DELETE trên bảng được publish mà không có replica identity, nên:
```bash
docker compose exec postgres psql -U coffee -d coffee_pos \
  -c "ALTER TABLE pos_transactions REPLICA IDENTITY FULL;" -c "ALTER TABLE pos_line_items REPLICA IDENTITY FULL;"
```
`FULL` = WAL ghi cả dòng cũ → envelope có `before` đầy đủ. Bảng có PK thì mặc định (`DEFAULT`) chỉ ghi khoá — đủ cho upsert, rẻ hơn.

### Bước 3 — Điền `register-connector.json` và đăng ký
Mở [`lab/register-connector.json`](lab/register-connector.json), điền 6 TODO (`database.hostname`, `plugin.name`, `slot.name`, `publication.name`, `table.include.list`, `topic.prefix`). Rồi:
```bash
curl -s -X POST -H "Content-Type: application/json" \
  --data @videos/16-cdc-debezium/lab/register-connector.json localhost:8083/connectors | python -m json.tool
curl -s localhost:8083/connectors/coffee-pos/status | python -m json.tool
```
`"state": "RUNNING"` ở cả connector và task. Nếu `FAILED`: `docker compose logs debezium --tail=50`, lỗi hay gặp là tên host sai (`localhost` thay vì `postgres`) hoặc slot name có ký tự lạ.

Kafka UI (:8080) → Topics: xuất hiện `coffee.public.pos_transactions` và `coffee.public.pos_line_items`. Đó là **snapshot ban đầu** (`snapshot.mode=initial`): toàn bộ bảng, mỗi dòng một envelope `op = "r"`.

### Bước 4 — INSERT / UPDATE / DELETE và nhìn envelope
Terminal 2 — console consumer bám topic:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic coffee.public.pos_transactions --property print.key=true
```
Terminal 1 — chạy từng khối trong [`lab/solution/cdc_demo.sql`](lab/solution/cdc_demo.sql) (hoặc cả file):
```bash
docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/16-cdc-debezium/lab/solution/cdc_demo.sql
```
Bạn thấy lần lượt 3 envelope + 1 tombstone:
```
{"transaction_id":"HN01-20260215-99001"}  {"before":null,"after":{...,"total_amount_vnd":45000,...},"source":{...,"lsn":...},"op":"c","ts_ms":...}
{"transaction_id":"HN01-20260215-99001"}  {"before":{...45000...},"after":{...50000...},"op":"u",...}
{"transaction_id":"HN01-20260215-99001"}  {"before":{...50000...},"after":null,"op":"d",...}
{"transaction_id":"HN01-20260215-99001"}  null
```
Đọc kỹ: `ts` của Postgres (`timestamptz`) được Debezium gửi thành chuỗi ISO **UTC** (`2026-02-15T03:00:00Z`) — một quy ước nữa cần biết khi ghép vào silver. Key là `transaction_id` nhờ `message.key.columns` (bảng không có PK thì Debezium không tự biết key). Message cuối `null` là **tombstone** để Kafka log compaction xoá key.

### Bước 5 — Áp dụng CDC vào DuckDB local
Mở [`lab/cdc_apply.py`](lab/cdc_apply.py), điền 3 TODO (`unwrap`, upsert cho `c/u/r`, delete cho `d`). Chạy:
```bash
uv run --with confluent-kafka python videos/16-cdc-debezium/lab/cdc_apply.py --db /tmp/cdc_pos.duckdb
```
Nó nuốt snapshot (~2 500 envelope `r`) rồi in từng `c/u/d` khi bạn chạy lại `cdc_demo.sql`. Ctrl+C rồi kiểm tra:
```bash
uv run python -c "import duckdb; c=duckdb.connect('/tmp/cdc_pos.duckdb'); print(c.sql('SELECT count(*) FROM pos_transactions')); print(c.sql(\"SELECT * FROM pos_transactions WHERE transaction_id LIKE '%-99001'\"))"
```

### Bước 6 — Test offline
```bash
uv run pytest videos/16-cdc-debezium -q
```

### Bước 7 — Dọn (quan trọng với slot!)
```bash
curl -s -X DELETE localhost:8083/connectors/coffee-pos
docker compose exec postgres psql -U coffee -d coffee_pos -c "SELECT slot_name, active FROM pg_replication_slots;"
```
Xoá connector **không** xoá slot (`slot.drop.on.stop` mặc định `false` — để restart đọc tiếp từ chỗ cũ). Slot `dbz_coffee` còn đó với `active = f` và Postgres vẫn giữ WAL cho nó. Xoá tay:
```bash
docker compose exec postgres psql -U coffee -d coffee_pos -c "SELECT pg_drop_replication_slot('dbz_coffee');"
```
Ở môi trường dev có thể đặt `"slot.drop.on.stop": "true"` trong config; production thì không bao giờ.

## Kết quả mong đợi
- Bước 3: status `RUNNING`; topic `coffee.public.pos_transactions` có **2 481 message** snapshot (bằng `SELECT count(*) FROM pos_transactions`), `coffee.public.pos_line_items` 4 328.
- Bước 4: 3 envelope `c → u → d` + tombstone đúng thứ tự, `before` có dữ liệu ở `u` và `d` (nhờ `REPLICA IDENTITY FULL`); `source.lsn` tăng dần.
- Bước 5: sau snapshot DuckDB có **2 476 dòng** (2 481 − 5 `transaction_id` trùng: upsert theo key tự dedup — hãy nghĩ xem đó là may hay rủi); sau `cdc_demo.sql` dòng `…-99001` **không còn** (đã delete), `_op` phân bố `r` ≈ 2 476.
- Bước 6: `3 passed` — trạng thái cuối sau `r,r,c,u,d,c` là 3 dòng, tổng 175 000 VND; áp dụng lại toàn bộ envelope không đổi kết quả.

Output thật (Debezium 2.7.3, Postgres 16.4):
```
$ curl -s localhost:8083/connectors/coffee-pos/status | python -m json.tool
{
    "name": "coffee-pos",
    "connector": { "state": "RUNNING", "worker_id": "172.25.0.7:8083" },
    "tasks": [ { "id": 0, "state": "RUNNING", "worker_id": "172.25.0.7:8083" } ],
    "type": "source"
}
$ kafka-topics.sh --list | grep coffee
__debezium-heartbeat.coffee
coffee.public.pos_line_items
coffee.public.pos_transactions
$ kafka-get-offsets.sh --topic coffee.public.pos_transactions   →  coffee.public.pos_transactions:0:2481
$ kafka-get-offsets.sh --topic coffee.public.pos_line_items     →  coffee.public.pos_line_items:0:4328
```
Bốn message của `cdc_demo.sql` trên console consumer (rút gọn `source`):
```
{"transaction_id":"HN01-20260215-99001"}  {"before":null,"after":{"transaction_id":"HN01-20260215-99001","store_id":"HN01","ts":"2026-02-15T03:00:00.000000Z","channel":"store","payment_method":"cash","customer_id":null,"staff_id":"HN01-S1","total_amount_vnd":45000,"discount_vnd":0,"is_rainy_day":false,"loaded_at":"2026-10-22T11:48:28.125231Z"},"source":{"version":"2.7.3.Final","connector":"postgresql","name":"coffee","db":"coffee_pos","schema":"public","table":"pos_transactions","txId":821,"lsn":33502096,...},"op":"c","ts_ms":1792669708154,...}
{"transaction_id":"HN01-20260215-99001"}  {"before":{...,"total_amount_vnd":45000,...},"after":{...,"total_amount_vnd":50000,...},"source":{...,"txId":822,"lsn":33519400,...},"op":"u",...}
{"transaction_id":"HN01-20260215-99001"}  {"before":{...,"total_amount_vnd":50000,...},"after":null,"source":{...,"txId":823,"lsn":33519712,...},"op":"d",...}
{"transaction_id":"HN01-20260215-99001"}  null
```
`lsn` 33502096 → 33519400 → 33519712 tăng dần, `txId` 821 → 822 → 823 — ba transaction Postgres riêng. Cột `ts` INSERT là `'2026-02-15 10:00:00+07'` nhưng envelope ghi `2026-02-15T03:00:00.000000Z`: Debezium luôn chuẩn hoá `timestamptz` về UTC. DuckDB sau `cdc_apply.py` + `cdc_demo.sql`: `count(*) = 2476`, dòng `…-99001` = 0 (đã delete), `_op` toàn `r` (2 476) — các dòng `c`/`u` của 99001 đã bị `d` xoá. Slot sau khi xoá connector: `dbz_coffee | active = t` thêm vài giây (walsender đang đóng) rồi `f`; `pg_drop_replication_slot` lúc còn `active` sẽ báo `replication slot "dbz_coffee" is active for PID …` — chờ rồi chạy lại.

## Câu hỏi phỏng vấn của video
> "Debezium connector của bạn chết lúc 2 giờ sáng, không ai biết tới 9 giờ. Chuyện gì xảy ra với Postgres của POS trong 7 tiếng đó, bạn kiểm tra bằng query gì, và thiết kế lại thế nào để lần sau nó tự kêu?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Mở rộng `cdc_apply.py` để xử lý thêm topic `coffee.public.pos_line_items` (key `line_id`) — một consumer subscribe 2 topic. Một hoá đơn và các dòng món của nó nằm ở 2 topic khác nhau: có lúc nào bảng đích thấy hoá đơn mà chưa thấy món không? Gợi ý: đọc về `provide.transaction.metadata`.
2. `UPDATE pos_transactions SET total_amount_vnd = total_amount_vnd WHERE store_id = 'HN01';` (không đổi giá trị) sinh bao nhiêu envelope? Batch full-load có "thấy" thay đổi này không? Rút ra điều gì về chi phí CDC khi app chạy job "touch" hàng loạt?
3. So sánh với #07: liệt kê 3 tình huống ở chuỗi 30 cửa hàng mà **full-load mỗi đêm là đủ** và 1 tình huống **bắt buộc** CDC. Trả lời dưới comment / mở Discussion.
