# #01 — Data Engineer là gì? Lộ trình từ số 0

📺 Video: `TODO:youtube-link`

## Bạn sẽ học được gì
- Nói được bằng một câu Data Engineer làm gì, và phân biệt với Data Analyst, Data Scientist, Analytics Engineer, Platform Engineer.
- Vẽ được sơ đồ luồng dữ liệu của Phin Phố: 3 nguồn → lake (bronze/silver/gold) → warehouse → dashboard, kèm tên công cụ ở từng mũi tên.
- Đọc một JD Data Engineer ở Việt Nam và chỉ ra video nào trong khoá dạy kỹ năng đó.
- Biết 3 nguồn dữ liệu mẫu trong repo trông như thế nào (mỗi nguồn vài dòng).

## Yêu cầu trước khi xem
- Đã xem [#00](../00-intro) và clone repo. **Không cần Docker** cho video này — lab chỉ dùng file text và lệnh shell cơ bản.
- Nếu muốn chạy bước 1, cần `make install` (để có `uv run`).

## Lab: vẽ luồng dữ liệu của Phin Phố

### Bước 1 — Nhìn 3 nguồn dữ liệu bằng mắt
```bash
head -3 data/samples/pos_transactions.csv
head -1 data/samples/app_events.jsonl
head -3 data/samples/iot_readings.csv
wc -l data/samples/*.csv data/samples/app_events.jsonl
```
Mỗi nguồn một "hình dạng": POS là bảng quan hệ (CSV/Postgres), app là JSON mỗi dòng một event, IoT là chuỗi đo theo thời gian. Data Engineer sống với việc 3 hình dạng này phải gặp nhau ở một chỗ.

### Bước 2 — Điền sơ đồ Mermaid
Mở [`lab/data_flow.md`](lab/data_flow.md). Mình để sẵn khung sơ đồ với các ô `TODO`. Bạn điền:
- tên và dạng dữ liệu của 3 nguồn (đọc [SCHEMA.md](../../data/generator/SCHEMA.md));
- ba layer của lake và mỗi layer làm gì (gợi ý: [platform/ingestion/src/coffee_ingestion/bronze.py](../../platform/ingestion/src/coffee_ingestion/bronze.py) và [silver.py](../../platform/ingestion/src/coffee_ingestion/silver.py) có docstring nói rõ);
- công cụ trên từng mũi tên và số video dạy công cụ đó (tra [videos/README.md](../README.md)).

Xem kết quả: dán nội dung khối ` ```mermaid ` vào https://mermaid.live, hoặc push lên GitHub (GitHub render Mermaid trong Markdown).

### Bước 3 — Map lộ trình sang JD thật
Mở [`lab/roadmap.md`](lab/roadmap.md): bảng 24 video × kỹ năng hay gặp trong JD Data Engineer ở Việt Nam (SQL, Python, Spark, Kafka, Airflow, cloud, dbt…). Việc của bạn:
1. Mở 3 JD Data Engineer đang tuyển (TopCV, ITviec, LinkedIn — bất kỳ).
2. Với mỗi yêu cầu trong JD, ghi vào cột `JD của bạn` số video tương ứng. Yêu cầu nào **không** có video nào dạy → ghi vào mục "Thiếu" cuối file.

### Bước 4 — So với lời giải
[`lab/solution/data_flow.md`](lab/solution/data_flow.md) là sơ đồ mình dùng xuyên suốt khoá. Sơ đồ của bạn không cần giống hệt, nhưng phải có đủ: 3 nguồn, 3 layer, 1 orchestrator, 1 nơi dashboard đọc.

## Kết quả mong đợi
- `wc -l` cho ra: `pos_transactions.csv` 2 482 dòng (1 header + **2 481** hoá đơn), `pos_line_items.csv` 4 329, `app_events.jsonl` **2 146** event, `iot_readings.csv` 2 714, `stores.csv` 31, `menu_items.csv` 24, `customers.csv` 2 001, `machines.csv` 61.
- `head -1 data/samples/app_events.jsonl` là một JSON có `event_type`, `ts`, `ingested_at` (hai timestamp khác nhau — bạn sẽ hiểu vì sao ở video #15).
- Sơ đồ Mermaid render được, không còn chữ `TODO`, có ít nhất 12 node: 3 nguồn, Kafka, bronze, silver, gold, Postgres/Trino (warehouse), dashboard, Airflow, dbt, Great Expectations.
- `roadmap.md` có ít nhất 3 JD được map; thường bạn sẽ thấy 1–2 yêu cầu khoá không dạy (ví dụ Scala, Snowflake, Databricks cụ thể) — ghi lại, đó là bài tập 3.

## Câu hỏi phỏng vấn của video
> "Team data của Phin Phố có đúng 3 người: 1 Data Engineer, 1 Data Analyst, 1 Data Scientist. Sáng mùng 1 Tết dashboard báo doanh thu ngày 30 Tết thấp hơn thực tế 7 triệu. Mỗi người kiểm tra cái gì trước, theo thứ tự nào, và ai là người 'own' con số đó?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Vẽ lại sơ đồ ở bước 2 cho **công ty bạn đang làm / muốn làm** (ẩn tên thật): nguồn nào, lưu ở đâu, ai đọc. Dán ảnh render dưới comment hoặc mở Discussion.
2. Trong sơ đồ Phin Phố, chỉ ra **2 chỗ dữ liệu có thể sai** mà dashboard không thể tự phát hiện (gợi ý: bảng lỗi cố ý trong SCHEMA.md). Giải thích vì sao Data Engineer phải bắt lỗi ở đó chứ không phải ở dashboard.
3. Từ mục "Thiếu" trong `roadmap.md`: chọn 1 kỹ năng khoá không dạy, viết 3 dòng về việc bạn sẽ tự học nó ở đâu. Mở Discussion để mọi người góp link.
