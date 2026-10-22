# #08 — Định dạng file: Parquet, Avro, ORC, Delta

📺 Video: `TODO:youtube-link` · 📑 Slide: [V08-Dinh-dang-file-du-lieu.pdf](slides/V08-Dinh-dang-file-du-lieu.pdf)

## Bạn sẽ học được gì
- Giải thích được vì sao cùng 2 481 hoá đơn, CSV nặng 224 KB mà Parquet zstd chỉ 36 KB — và vì sao *kích thước* chưa phải lý do chính để chọn Parquet.
- Đọc được footer Parquet: row group, statistics min/max, codec — rồi giải thích **predicate pushdown** và **projection pushdown** bằng chính số liệu bạn đo.
- Biết khi nào dùng Avro (row-based, schema evolution, Kafka) thay cho Parquet (columnar, analytics).
- Biết ORC đứng ở đâu (họ Hive/Trino), và Delta/Iceberg/Hudi là *lớp bảng* nằm **trên** Parquet chứ không thay thế nó (chi tiết ở #17).

## Yêu cầu trước khi xem
- Đã xem #07 (đã biết bronze/silver ghi Parquet).
- `make install`. Bước 1–4 **không cần Docker**. Bước 5 (Avro) cần `make up` để dùng Jupyter — vì `fastavro` chỉ có trong image Jupyter, không có trong `.venv`.

## Lab: đo đạc 5 định dạng trên cùng một bảng

### Bước 1 — Điền TODO và chạy benchmark
Mở [`lab/formats_bench.py`](lab/formats_bench.py), điền TODO 1 → 5 (ghi JSONL/Parquet, đọc 1 cột Parquet, đọc statistics, kiểm tra row group). Starter sẽ báo `FileNotFoundError` cho tới khi bạn xong TODO 1 — đúng như vậy.
```bash
uv run python videos/08-dinh-dang-file/lab/formats_bench.py --out /tmp/formats
uv run python videos/08-dinh-dang-file/lab/formats_bench.py --out /tmp/formats --scale 40   # ~100 k dòng
uv run pytest videos/08-dinh-dang-file -q
```
Lời giải: [`lab/solution/formats_bench.py`](lab/solution/formats_bench.py).

### Bước 2 — Đọc bảng kết quả
Cột `bytes` nói về **nén + encoding** (dictionary, RLE). Cột `1 col ms` so với `all ms` nói về **columnar**: Parquet/ORC chỉ đọc đúng cột cần; CSV/JSONL phải quét hết file rồi mới bỏ cột đi.

### Bước 3 — Soi footer Parquet
Đoạn `Parquet footer` in ra `num_row_groups` và với mỗi row group: `rows`, `bytes`, min/max của `ts`, `store_id`, `total_amount_vnd`. Hãy để ý:
- `store_id` có `nulls: 8` — chính là 8 dòng `null_store` của cả 6 ngày; footer đã "biết" điều này mà không cần đọc dữ liệu.
- `ts.min = '2026-02-14 09:07:26'` (có dấu cách) nhưng `ts.max = '2026-02-19T22:06:02+07:00'` — vì cột `ts` là **string** nên min/max so theo thứ tự chữ, và dòng lỗi `tz_drift` (thiếu `T` và offset) lọt lên đầu. Đây là lý do silver phải ép kiểu: statistics trên string vô dụng cho filter theo thời gian.

### Bước 4 — Predicate pushdown bằng mắt thường
`pushdown_demo` ghi lại bảng **sort theo `store_id`** với `row_group_size=256` → 10 row group. Khi lọc `store_id = 'SG14'`, chỉ những row group có `min ≤ 'SG14' ≤ max` cần đọc. Thử đổi `'SG14'` thành `'HN01'` và xem danh sách row group thay đổi.

Nguyên tắc rút ra cho lake: **sort (hoặc partition) theo cột hay lọc** trước khi ghi → engine bỏ qua phần lớn file.

### Bước 5 — Avro và schema evolution (trong Jupyter)
Mở http://localhost:8888 (token `dea2z`) → New → Terminal:
```bash
python work/videos/08-dinh-dang-file/lab/avro_evolution.py --out /tmp/avro
```
Điền TODO trong [`lab/avro_evolution.py`](lab/avro_evolution.py): viết schema v2 thêm field `discount_vnd`, đọc lại file v1 bằng schema v2. Rồi thử **bỏ `default`** của field mới và xem lỗi. Lời giải: [`lab/solution/avro_evolution.py`](lab/solution/avro_evolution.py).

### Bước 6 — ORC, Delta, Iceberg: chỉ cần biết chúng ở đâu
- ORC: benchmark ở bước 1 đã ghi 1 file ORC bằng `pyarrow.orc` (nếu bản pyarrow của bạn có). Cùng ý tưởng columnar + stripe statistics như Parquet; phổ biến trong hệ Hive/Trino. Repo này không dùng ORC ở đâu khác.
- Delta / Iceberg / Hudi **không phải định dạng file**. File vẫn là Parquet; chúng thêm *metadata layer* (snapshot, schema evolution, ACID, time travel). Xem DDL Iceberg của silver ở [`platform/lake/iceberg/silver_tables.sql`](../../platform/lake/iceberg/silver_tables.sql) — để ý `write.parquet.compression-codec = zstd` và `target-file-size-bytes = 128 MB`. Video #17 chạy nó.

## Kết quả mong đợi
Bảng benchmark (máy mình, Apple M-series; thời gian của bạn sẽ khác, **tỉ lệ** thì không):

| format | bytes (scale 1) | vs csv | bytes (scale 40, 99 240 dòng) | all ms | 1 col ms |
|--------|----------------:|-------:|------------------------------:|-------:|---------:|
| csv | 223 907 | 1.00 | 8 951 834 | 5.3 | 3.8 |
| jsonl | 597 967 | 2.67 | 23 918 680 | 12.0 | 12.4 |
| parquet_snappy | 50 045 | 0.22 | 308 497 | 1.6 | 0.5 |
| parquet_zstd | 36 227 | **0.16** | 160 946 | 1.7 | 0.5 |
| orc (zstd) | 39 933 | 0.18 | 820 353 | 19.9 | 0.5 |

- Ở scale 40, Parquet zstd nhỏ hơn CSV **55 lần** (dữ liệu nhân bản nên dictionary/RLE nén cực tốt — số thật ngoài đời thường 5–10 lần). Đọc 1 cột Parquet nhanh hơn đọc cả bảng ~3 lần; với CSV đọc 1 cột gần bằng đọc cả bảng.
- Footer: `num_rows: 2481`, `num_row_groups: 1`, `num_columns: 10`, `created_by: Polars ...`; `store_id` có `nulls: 8`; `total_amount_vnd` min `0`, max `428000`, codec `ZSTD`.
- Pushdown: `{"row_groups_total": 10, "row_groups_needed_for_SG14": [8, 9], "rows_SG14": 130}` — đọc 2/10 row group là đủ.
- `pytest videos/08-dinh-dang-file -q` → **4 passed**.
- Bước 5 (chạy trong container Jupyter, output thật):
  ```
  wrote /tmp/avro/pos_transactions.v1.avro (6,701 bytes) with schema v1
  read back 500 records with schema v2; first record: {'transaction_id': 'SG04-20260214-00038', 'store_id': 'SG04', 'ts': '2026-02-14T05:17:12+07:00', 'channel': 'store', 'total_amount_vnd': 74000, 'discount_vnd': 0}
  reader without default → SchemaResolutionError: schema KHÔNG tương thích ngược, đúng như mong đợi
  ```
  Thông điệp gốc của fastavro khi bỏ `default`: `SchemaResolutionError: No default value for field discount_vnd in ...`. 500 hoá đơn Avro = 6,7 KB ≈ 13 byte/dòng — nhỏ hơn cả Parquet zstd ở bảng trên vì Avro không mang footer thống kê; đổi lại nó là row-based, đọc 1 cột vẫn phải đọc hết.

## Câu hỏi phỏng vấn của video
> "Team BI than query trên lake chậm dù dữ liệu chỉ 50 GB Parquet. Bạn mở footer vài file ra xem thì nên nhìn vào những gì, và 3 nguyên nhân hay gặp nhất là gì?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Thêm `--row-group-size` cho benchmark, chạy với 100 / 1 000 / 10 000 ở scale 40. File to hay nhỏ hơn? Số row group cần đọc cho `SG14` thay đổi thế nào? Dán bảng dưới comment.
2. Ép kiểu `ts` về datetime UTC (dùng `parse_vn_timestamp` của `coffee_ingestion.silver`) trước khi ghi Parquet, rồi so sánh statistics min/max của `ts` với bước 3. Vì sao giờ filter theo ngày mới dùng được pushdown?
3. (Discussion) Hệ thống POS gửi từng hoá đơn lên Kafka ngay khi thanh toán. Bạn chọn Avro hay Parquet cho message đó? Còn file đích trong lake? Giải thích bằng row-based vs columnar.
