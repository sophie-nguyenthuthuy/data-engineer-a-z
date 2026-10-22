# #15 — Spark Structured Streaming: watermark, window

📺 Video: `TODO:youtube-link` · 📑 Slide: [V15-Stream-Processing.pdf](slides/V15-Stream-Processing.pdf)

## Bạn sẽ học được gì
- Hiểu Structured Streaming là "bảng vô hạn + micro-batch": cùng API DataFrame ở #10, chỉ khác `readStream`/`writeStream`.
- Phân biệt **event time** (`ts` trong message) và **processing time** (lúc Spark nhận), và vì sao chỉ event time mới cho kết quả đúng khi dữ liệu tới muộn.
- Dùng **watermark** để Spark biết chờ dữ liệu muộn bao lâu rồi "đóng" cửa sổ — và thấy tận mắt dữ liệu bị bỏ khi watermark quá ngắn.
- Chọn **output mode** (append / update / complete) đúng với sink.
- Viết job cảnh báo máy pha quá nóng theo cửa sổ 1 phút cho từng máy, có test offline cho phần luật.

## Yêu cầu trước khi xem
- Đã xem #10 (Spark DataFrame) và làm lab #13 (producer `coffeegen stream`, topic `pos.transactions`).
- Hai profile cùng chạy: `make up PROFILE=streaming` rồi `make up PROFILE=spark` (hoặc một lệnh `make up PROFILE=all` nếu máy ≥ 16 GB). Spark UI: http://localhost:8085 phải thấy 1 worker.
- Job Spark **chỉ chạy trong container** (image `dea2z/spark` có jar Kafka + Iceberg). Phần luật thuần Python test được trên máy bạn.

## Năm khái niệm cần nắm trước khi gõ lệnh
| Khái niệm | Một câu | Trong lab |
|-----------|---------|-----------|
| Micro-batch / trigger | Spark gom message tới trong ~vài giây rồi chạy như một batch nhỏ; mỗi lần là một `Batch: N` trên console | mặc định: batch mới ngay khi batch trước xong |
| Event time vs processing time | `ts` do POS/cảm biến ghi ≠ lúc Spark nhận. Replay dữ liệu cũ, lệch múi giờ, mạng chập chờn đều làm hai thứ này khác nhau | `to_timestamp("ts")` |
| Window | Chia event time thành cửa sổ (tumbling 1 phút ở đây; còn sliding, session) | `F.window("event_time", "1 minute")` |
| Watermark | "Max event time đã thấy − X": bản ghi có event time cũ hơn watermark bị **bỏ**; state của cửa sổ cũ hơn watermark được **xoá** | `withWatermark("event_time", "10 minutes")` |
| Output mode | `append`: chỉ in cửa sổ đã đóng (cần watermark); `update`: in cửa sổ vừa đổi; `complete`: in toàn bộ bảng kết quả mỗi batch | `--output-mode` |

Watermark là cái giá của streaming: không có nó, Spark phải giữ state **mãi mãi** để chờ bản ghi muộn có thể không bao giờ tới.

## Lab: doanh thu mỗi phút và cảnh báo máy pha

### Bước 1 — Khởi động Spark trước, producer sau
Job đọc `startingOffsets = latest`, nên bật job rồi mới bắn dữ liệu:
```bash
docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
  /opt/jobs/streaming_pos_agg.py --bootstrap kafka:29092
```
Chờ dòng `Batch: 0` (bảng rỗng). Đọc code [`platform/spark/streaming_pos_agg.py`](../../platform/spark/streaming_pos_agg.py) trong lúc chờ — 60 dòng, không có gì bạn chưa gặp ở #10 ngoài `readStream`, `withWatermark`, `window`, `writeStream`.

### Bước 2 — Bắn hoá đơn, quan sát console sink
Terminal 2:
```bash
uv run --with confluent-kafka coffeegen stream --topic pos.transactions --start 2026-02-15 --scale 0.1 --rate 20
```
Terminal 1 in bảng mỗi batch: `window{start,end}`, `store_id`, `orders`, `revenue_vnd`. Vì `outputMode("update")`, batch chỉ chứa cửa sổ **vừa thay đổi**. Replay 1 008 hoá đơn của một ngày trong ~50 giây → event time chạy nhanh gấp ~1 700 lần đồng hồ thật, rất tiện để thấy watermark "trôi".

Để ý `window.start` là **UTC** (job set session timezone UTC, như quy ước lake ở #09): hoá đơn 07:15 giờ VN nằm ở cửa sổ `00:15`.

### Bước 3 — Thí nghiệm: một dòng thiếu `+07:00` phá cả buổi sáng
Trong 1 008 message có **5 dòng `tz_drift`** (`ts` dạng `2026-02-15 06:17:22`, không offset). `to_timestamp` hiểu chúng là UTC → lệch **+7 giờ về tương lai** → watermark nhảy theo. Mọi hoá đơn thật trong ~7 giờ tiếp theo bỗng "cũ hơn watermark" và bị bỏ. Nhìn console sau khi producer qua mốc 06:17: các cửa sổ `23:xx` (= 06:xx VN) ngừng cập nhật một lúc dài rồi mới xuất hiện lại ở quãng trưa.

Sửa đúng không phải nới watermark mà là parse đúng: hàm `parse_vn_timestamp` trong [`bronze_to_silver_pos.py`](../../platform/spark/bronze_to_silver_pos.py) (thiếu offset ⇒ giờ VN). Bài tập 1 yêu cầu bạn ghép nó vào job streaming.

Đo thật trên cluster compose (producer `--rate 20`, 82 micro-batch, đọc từ `progress` JSON mà console sink in sau mỗi batch):

| batch | input rows | cửa sổ in ra | watermark sau batch | max event time trong batch |
|------:|-----------:|-------------:|---------------------|----------------------------|
| 1 | 1 | 1 | 1970-01-01 | 2026-02-14 **22:17:30** (05:17 VN, hoá đơn đầu ngày) |
| 2 | 39 | 20+ | 2026-02-14 22:07:30 | 2026-02-15 **06:17:22** ← dòng tz_drift `2026-02-15 06:17:22` bị hiểu là UTC |
| 3 | 24 | 20+ | **2026-02-15 06:07:22** | 00:09:34 |
| 4 → 11 | 118 | **0** | 06:07:22 | 00:19 → 01:22 |
| 12 | 11 | 2 | 06:07:22 | **08:27:49** ← tz_drift thứ hai, watermark nhảy tiếp |
| 13 → 53 | ~520 | **0** (trừ 2 batch có tz_drift) | 08:17 → **09:44:12** | 01:34 → 09:42 |
| 54 → 80 | ~330 | bình thường | trượt 10 phút sau max event time | 09:45 → 15:55 |

Kết quả: **~640 trong 1 008 hoá đơn (≈63 %) bị bỏ** — toàn bộ từ 07:09 đến 16:42 giờ VN, tức cả buổi sáng lẫn giờ cao điểm chiều — chỉ vì 5 dòng thiếu `+07:00`. Console chỉ còn in bảng trống (`numInputRows: 10`, 0 dòng kết quả) suốt batch 4–53; đến batch 54 event time thật mới vượt qua watermark 09:44:12 và doanh thu "sống lại". Không có lỗi, không có cảnh báo: Spark làm đúng việc được bảo. Đây là lý do `_tz_fixed` tồn tại ở silver.

### Bước 4 — Job cảnh báo IoT: điền TODO
Mở [`lab/iot_anomaly_stream.py`](lab/iot_anomaly_stream.py): 6 TODO (schema, cột `ingest_time`, watermark + dedup, groupBy/agg, filter ngưỡng, sink). Luật thuần Python và ngưỡng nằm ở [`lab/solution/anomaly_rules.py`](lab/solution/anomaly_rules.py) — job Spark chỉ là bản "chạy mãi" của `alert_windows()`.

Tạo topic **trước** khi bật job: `startingOffsets = latest` trên topic chưa tồn tại làm Kafka source lỗi `UnknownTopicOrPartitionException` lặp vô tận và không bao giờ ra `Batch: 0` (topic `pos.transactions` ở bước 1 đã có từ #13 nên không gặp).
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 \
  --create --topic iot.readings --partitions 3 --replication-factor 1
```
Chạy (file ngoài `/opt/jobs` nên copy vào container):
```bash
docker compose cp videos/15-spark-structured-streaming/lab/iot_anomaly_stream.py spark-master:/tmp/
docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
  --py-files /opt/jobs/common.py /tmp/iot_anomaly_stream.py --bootstrap kafka:29092 --watermark "2 minutes"
```
Terminal 2 (generator dựng 16 giờ telemetry trong RAM trước nên chờ ~5 giây mới có message đầu):
```bash
uv run --with confluent-kafka coffeegen stream --source iot --topic iot.readings --start 2026-02-16 --rate 100 --limit 20000
```
Console in các cửa sổ có `max_temp_c > 95.5` kèm `readings`, `hot_readings`, `max_lag_s` (bản ghi muộn nhất trong cửa sổ, giây).

### Bước 5 — Thí nghiệm watermark và output mode
Dừng job (Ctrl+C), chạy lại với tham số khác, bắn lại cùng producer, so sánh:
1. `--watermark "1 minute"` so với `"10 minutes"`: IoT có ~1% bản ghi tới muộn 1–10 phút (`max_lag_s` cho thấy). Với 1 phút, phần lớn chúng bị bỏ → tổng `readings` qua các cửa sổ **nhỏ hơn** số message producer báo `sent`. Với 10 phút: gần đủ, nhưng Spark phải giữ state lâu hơn 10× và cửa sổ "đóng" chậm hơn 10×.
2. `--output-mode append`: cửa sổ chỉ xuất hiện **sau** khi watermark vượt qua `window.end` — đây là mode duy nhất dùng được cho sink file/Parquet (#18). `complete`: in cả bảng mỗi batch — tiện xem, không dùng được khi bảng lớn.

### Bước 6 — Test offline phần luật
```bash
uv run pytest videos/15-spark-structured-streaming -q
```

## Kết quả mong đợi
- Bước 2: console có bảng `Batch: N` với cột `window`, `store_id`, `orders`, `revenue_vnd`. Thật: `Batch: 0` sau ~12 s khởi động, producer `--rate 20` bắn 1 008 message trong 55 s, job in **82 batch** (một batch ≈ 0,5–0,7 s, `numInputRows` 8–39). Batch đầu có dữ liệu:
  ```
  Batch: 3
  +------------------------------------------+--------+------+-----------+
  |window                                    |store_id|orders|revenue_vnd|
  +------------------------------------------+--------+------+-----------+
  |{2026-02-15 00:03:00, 2026-02-15 00:04:00}|HN10    |1     |35000      |
  |{2026-02-14 23:52:00, 2026-02-14 23:53:00}|SG14    |1     |94000      |
  |{2026-02-15 00:05:00, 2026-02-15 00:06:00}|HN13    |2     |240000     |
  ...
  only showing top 20 rows
  ```
  `window` là UTC: `00:03` = 07:03 sáng VN. Vì watermark + tz_drift (bước 3), tổng `orders` in ra **nhỏ hơn hẳn** 1 004 — xem bảng ở bước 3.
- Bước 3: console trống (0 dòng) từ batch 4 tới batch 53 dù mỗi batch vẫn nhận 8–22 message; ~63 % hoá đơn bị bỏ (bảng ở trên).
- Bước 4, đo thật (`--rate 100 --limit 20000`, producer chạy 240 s, job in **503 batch**, `numInputRows` tổng đúng 20 000): **279 batch có cảnh báo**, 37/60 máy xuất hiện trong bảng, `max_temp_c` cao nhất **101,18 °C**; `max_lag_s` = 0–1 ở hầu hết dòng, 4 dòng ≥ 60 s (bản ghi tới muộn cố ý của generator). Bảng đầu tiên xuất hiện ở `Batch: 2`:
  ```
  +------------------------------------------+----------+--------+----------+--------+------------+---------+
  |window                                    |machine_id|store_id|max_temp_c|readings|hot_readings|max_lag_s|
  +------------------------------------------+----------+--------+----------+--------+------------+---------+
  |{2026-02-15 23:00:00, 2026-02-15 23:01:00}|M006      |SG03    |95.89     |3       |1           |1        |
  |{2026-02-15 23:00:00, 2026-02-15 23:01:00}|M023      |HN12    |100.05    |6       |1           |1        |
  +------------------------------------------+----------+--------+----------+--------+------------+---------+
  ```
  Cùng cửa sổ `23:00` (06:00 VN) của M006 lặp lại ở batch 2, 3, 4 với `readings` 3 → 4 → 5: `update` mode in lại cửa sổ mỗi khi có bản ghi mới rơi vào — cho tới khi watermark 2 phút đóng nó. Nhiều batch trống xen kẽ: 100 msg/s nhưng event time 60 máy × 0,2 Hz chạy chậm hơn, nên phần lớn batch chỉ cập nhật cửa sổ đang mở mà chưa vượt ngưỡng.
- Bước 6 — số thật trên `data/samples/iot_readings.csv` (2 713 dòng, 60 máy, 06:00–06:44 ngày 16/02): luật phân loại cho **2 332 warn / 373 ok / 8 unknown** (8 dòng `boiler_temp_c` NULL); cột `status` của generator: 371 ok / 2 338 warn / 4 error; 2 dòng `status=warn` nhưng temp đúng 95.50 (generator so sánh trước khi làm tròn — bug ở biên). **2 319 / 2 640** cửa sổ máy-phút vượt ngưỡng, max 103,13 °C; 13 duplicate `reading_id` không đổi kết quả. `3 passed`.

## Câu hỏi phỏng vấn của video
> "Job streaming của bạn đặt watermark 10 phút. PM hỏi: 'Sao dashboard trễ 10 phút, giảm xuống 30 giây được không?' Bạn trả lời thế nào — và nếu buộc phải giảm, bạn làm gì với dữ liệu tới muộn hơn 30 giây để không mất doanh thu?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Ghép `parse_vn_timestamp` từ `bronze_to_silver_pos.py` vào `streaming_pos_agg.py` (bản copy trong `/tmp` của bạn) và chạy lại bước 3: khoảng trống còn không? Dán 2 ảnh console trước/sau.
2. Thêm cảnh báo thứ hai vào `iot_anomaly_stream.py`: `pump_pressure_bar < 8.3` **liên tục 3 cửa sổ** trên cùng máy. Gợi ý: đây là bài toán state giữa các cửa sổ — `flatMapGroupsWithState`/`applyInPandasWithState`, hoặc đơn giản hơn: sliding window 3 phút, bước 1 phút với `min(pressure)`. Giải thích vì sao cách đơn giản chấp nhận được.
3. Đổi sink IoT sang Parquet `s3a://lake/gold/rt_machine_alerts_1m` (xem nhánh `--sink parquet` trong `streaming_pos_agg.py`). Mode nào bắt buộc? Checkpoint để đâu để restart không ghi trùng? Trả lời dưới comment / mở Discussion.
