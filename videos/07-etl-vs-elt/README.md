# #07 — ETL vs ELT: pipeline đầu tiên bằng Python

📺 Video: `TODO:youtube-link` · 📑 Slide: [V07-ETL-vs-ELT.pdf](slides/V07-ETL-vs-ELT.pdf)

## Bạn sẽ học được gì
- Phân biệt ETL và ELT bằng chính kiến trúc của repo: Python làm **E + L + T nhẹ** (bronze → silver), dbt làm **T nặng** ở gold (video #12).
- Đọc hiểu package `platform/ingestion` (`coffee-ingest`): 5 file, mỗi file một việc — sources / bronze / silver / lake / pipeline.
- Chạy pipeline cho một ngày nghiệp vụ, đọc JSON summary và giải thích được từng con số (799 → 792, quarantine 4, duplicate 3, tz_drift 5).
- Hiểu 3 nguyên tắc sống còn của batch pipeline: bronze không sửa gì, ghi đè nguyên partition (idempotent), quarantine thay vì xoá.
- Tự viết thêm một rule silver theo đúng kiểu của repo.

## Yêu cầu trước khi xem
- Đã xem #04 (Python, Polars) và #06 (bronze/silver/gold là gì).
- `make install` xong (có `.venv`). **Không cần Docker** cho video này — lab chạy hoàn toàn trên máy với thư mục `./lake-local`.

## Lab: chạy và đọc pipeline `coffee-ingest`

### Bước 1 — Đọc code trước khi chạy
Mở theo đúng thứ tự dữ liệu chảy, mỗi file < 120 dòng:

| File | Việc | Câu hỏi bạn tự trả lời khi đọc |
|------|------|-------------------------------|
| [`sources.py`](../../platform/ingestion/src/coffee_ingestion/sources.py) | đọc CSV/Postgres, **mọi cột là string** | vì sao không cho Polars tự đoán kiểu? |
| [`bronze.py`](../../platform/ingestion/src/coffee_ingestion/bronze.py) | thêm `_ingested_at`, `_source`, `_run_date`, ghi Parquet | bronze có được lọc/sửa dòng nào không? |
| [`silver.py`](../../platform/ingestion/src/coffee_ingestion/silver.py) | type, dedup, tz_drift, quarantine | dòng duplicate nào được giữ? |
| [`lake.py`](../../platform/ingestion/src/coffee_ingestion/lake.py) | `./lake-local` hay `s3://lake` đều cùng một API | đổi sang MinIO cần sửa code không? |
| [`pipeline.py`](../../platform/ingestion/src/coffee_ingestion/pipeline.py) | nối tất cả lại cho **một ngày** | chạy lại cùng ngày thì có sinh duplicate không? |

Chú ý hàm `parse_vn_timestamp` trong `silver.py`: timestamp thiếu `+07:00` được hiểu là giờ Việt Nam rồi đổi về UTC — đây là cách repo xử lý lỗi `tz_drift` **một lần duy nhất**, ở silver.

### Bước 2 — Chạy pipeline cho ngày 15/02
```bash
make smoke
# = uv run coffee-ingest run --source data/samples --lake ./lake-local --date 2026-02-15
```
Pipeline in ra JSON summary. Đoạn quan trọng:
```json
"silver": {
  "pos_transactions": {
    "rows_in": 799, "rows_out": 792, "quarantined": 4,
    "reasons": {"null_store": 4, "duplicate": 3, "tz_drift": 5}
  },
  "pos_line_items": {
    "rows_in": 1401, "rows_out": 1392, "quarantined": 9,
    "reasons": {"negative_qty": 3, "orphan_txn": 6}
  }
}
```
Tự kiểm tra phép tính: 799 − 4 (null_store) − 3 (duplicate) = 792. `tz_drift` = 5 không trừ đi vì dòng đó được **sửa**, không bị loại.
`orphan_txn` = 6 là line item của 4 hoá đơn null_store — hoá đơn vào quarantine thì món của nó cũng không được vào silver.

### Bước 3 — Xem cây thư mục lake
```bash
find lake-local -name "*.parquet" | sort
```
12 file, mỗi bảng một partition `dt=2026-02-15/part-000.parquet`. Bronze 5 bảng; silver 7 bảng (5 bảng + 2 bảng `quarantine_*`).

### Bước 4 — Mở Parquet bằng DuckDB
```bash
uv run python -c "
import duckdb
con = duckdb.connect()
print(con.sql(\"DESCRIBE SELECT * FROM 'lake-local/bronze/pos_transactions/dt=2026-02-15/*.parquet'\"))
print(con.sql(\"DESCRIBE SELECT * FROM 'lake-local/silver/pos_transactions/dt=2026-02-15/*.parquet'\"))
print(con.sql(\"SELECT count(*) n, sum(total_amount_vnd) revenue_vnd FROM 'lake-local/silver/pos_transactions/dt=2026-02-15/*.parquet'\"))
print(con.sql(\"SELECT * FROM 'lake-local/silver/quarantine_pos_transactions/dt=2026-02-15/*.parquet'\"))
"
```
So sánh hai `DESCRIBE`: bronze toàn `VARCHAR` (kể cả `ts`, `total_amount_vnd`), silver có `TIMESTAMP WITH TIME ZONE`, `BIGINT`, `BOOLEAN`. Đó chính là chữ **T** nhẹ.

### Bước 5 — Chạy lại để thấy idempotent
```bash
make smoke && make smoke
uv run python -c "import duckdb; print(duckdb.sql(\"SELECT count(*) FROM 'lake-local/silver/pos_transactions/dt=2026-02-15/*.parquet'\"))"
```
Vẫn 792. Pipeline ghi đè nguyên partition `dt=2026-02-15`, không append.

### Bước 6 — Viết rule silver của bạn
Mở [`lab/extra_rule.py`](lab/extra_rule.py), điền 5 TODO trong `apply_extra_rules()` để thêm 2 rule:
- `discount_gt_total`: `discount_vnd > total_amount_vnd` (giảm giá nhiều hơn tiền khách trả — cần đối soát);
- `app_cash`: `channel = 'app'` nhưng `payment_method = 'cash'` (SCHEMA.md nói app không bao giờ cash).

```bash
uv run python videos/07-etl-vs-elt/lab/extra_rule.py --date 2026-02-15
uv run python videos/07-etl-vs-elt/lab/extra_rule.py --date all
uv run pytest videos/07-etl-vs-elt -q        # test trong lab/solution/ chấm bài cho bạn
```
Hàm nhận silver đã dedup, trả về `(silver_sạch, quarantine, reasons)` — đúng kiểu `clean_transactions` trong `silver.py`. Lời giải: [`lab/solution/extra_rule.py`](lab/solution/extra_rule.py).

## Kết quả mong đợi
- `make smoke` chạy < 1 giây, exit code 0, in JSON với `rows_in: 799`, `rows_out: 792`, `quarantined: 4`, `reasons: {null_store: 4, duplicate: 3, tz_drift: 5}`; line items `1401 → 1392`, `negative_qty: 3`, `orphan_txn: 6`.
- Bước 4: silver 15/02 có **792 dòng, doanh thu 76.939.000 VND**. Bảng quarantine có đúng 4 dòng, `_reason = null_store`, các mã `HN13-20260215-00034`, `HN14-20260215-00012`, `SG10-20260215-00018`, `HN04-20260215-00012`.
- 3 hoá đơn bị duplicate trong bronze: `HN08-20260215-00035`, `HN11-20260215-00015`, `SG02-20260215-00006` (mỗi mã xuất hiện 2 lần).
- Bước 6 với lời giải: ngày 15/02 `extra_quarantined: 1` (`HN01-20260215-00014`, total 19.000 nhưng discount 20.000), `app_cash: 0`; `--date all` → `discount_gt_total: 8`, `app_cash: 0`. `pytest` → **3 passed**.
  Rule `app_cash` bắt 0 dòng trên samples là **bình thường**: rule tồn tại để bảo vệ tương lai, không phải để có số đẹp hôm nay.

## Câu hỏi phỏng vấn của video
> "Pipeline của bạn chạy lúc 2h sáng, bị lỗi giữa chừng và được chạy lại lúc 3h. Làm sao bạn chắc chắn bảng silver không có dòng nào bị đếm hai lần? Nếu nguồn là Postgres đang thay đổi liên tục thì câu trả lời có khác không?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Thêm rule thứ ba vào `extra_rule.py`: hoá đơn `channel = 'app'` nhưng `customer_id` rỗng (app luôn yêu cầu đăng nhập). Có bao nhiêu dòng trên samples? Dán kết quả JSON dưới comment.
2. Chạy `coffee-ingest run --date 2026-02-17` (mùng 1 Tết). Giải thích vì sao `duplicate = 0` nhưng `tz_drift = 1`, và vì sao doanh thu chỉ còn 5.549.000 VND.
3. Đọc `read_postgres` trong `sources.py` và trả lời trong Discussion: với bảng 50 triệu dòng, cách `SELECT * FROM table` có vấn đề gì? Video #16 (CDC) sẽ quay lại câu này.
