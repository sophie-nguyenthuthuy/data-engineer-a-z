# #02 — Kiến trúc dữ liệu hiện đại: OLTP, OLAP, Warehouse, Lake

📺 Video: `TODO:youtube-link`

## Bạn sẽ học được gì
- Phân biệt OLTP (máy tính tiền ghi hoá đơn) và OLAP (sếp hỏi doanh thu theo ngày × cửa hàng) bằng **query plan thật**, không bằng định nghĩa.
- Hiểu row store vs column store ở mức I/O: vì sao cùng một câu hỏi, Parquet nhanh hơn CSV ~15 lần và nhỏ hơn ~4,6 lần.
- Biết Data Warehouse, Data Lake, Lakehouse khác nhau ở đâu, và vì sao Phin Phố dùng lake (MinIO) + warehouse (Postgres gold / Trino) thay vì chạy dashboard thẳng trên Postgres của POS.
- Đọc được `EXPLAIN (ANALYZE, BUFFERS)` của Postgres: Seq Scan, Index Scan, HashAggregate, `Buffers: shared hit`.

## Yêu cầu trước khi xem
- Đã xem [#01](../01-data-engineer-la-gi).
- Bước 1–2 chạy offline (chỉ cần `make install`). Bước 3 cần Postgres: `make up`.

## Lab: cùng một câu hỏi, hai cách lưu trữ

Câu hỏi nghiệp vụ dùng cả bài: **doanh thu theo ngày × cửa hàng** (bỏ hoá đơn thiếu `store_id`).

### Bước 1 — DuckDB đọc CSV rồi đọc Parquet
Mở [`lab/oltp_vs_olap.py`](lab/oltp_vs_olap.py), điền 4 TODO (viết SQL, chuyển CSV → Parquet, hàm đo thời gian, in tỉ lệ), rồi chạy:
```bash
uv run python videos/02-kien-truc-du-lieu/lab/oltp_vs_olap.py
```
Script ghi Parquet vào `./lake-local/v02/` (thư mục đã có trong `.gitignore`).

### Bước 2 — Thử với dữ liệu lớn hơn
Samples chỉ có 2 481 hoá đơn, khác biệt tính bằng mili-giây. Sinh 30 ngày (~195 k hoá đơn, mất ~5 s) rồi chạy lại:
```bash
uv run coffeegen pos --start 2026-01-01 --days 30 --stores 30 --seed 7 --out ./out
uv run python videos/02-kien-truc-du-lieu/lab/oltp_vs_olap.py --source ./out
```
Để ý: tỉ lệ dung lượng gần như không đổi, tỉ lệ thời gian thì phụ thuộc máy — nhưng Parquet luôn thắng khi câu hỏi chỉ đụng vài cột.

### Bước 3 — Query plan của Postgres
```bash
docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/02-kien-truc-du-lieu/lab/explain.sql
```
File có 2 câu hỏi: tra 1 hoá đơn theo `transaction_id` (OLTP) và câu doanh thu ở trên (OLAP). Điền TODO: tạo index, chạy lại, so `Seq Scan` với `Index Scan`, rồi tự viết `EXPLAIN` cho câu OLAP và trả lời vì sao index `store_id` không được dùng.

### Bước 4 — So với lời giải
[`lab/solution/oltp_vs_olap.py`](lab/solution/oltp_vs_olap.py) và [`lab/solution/explain.sql`](lab/solution/explain.sql). Test tự động (chạy < 2 s, không cần Docker):
```bash
uv run pytest videos/02-kien-truc-du-lieu -q
```

## Kết quả mong đợi
- Bước 1 trên samples (Mac M-series, số của bạn sẽ khác chút):
  ```
  CSV     :   180 dòng      15.6 ms       219 KB
  Parquet :   180 dòng       1.7 ms        47 KB
  Parquet nhanh hơn 8.9x, nhỏ hơn 4.6x
  Parquet SELECT * (đủ 10 cột, không aggregate): 3.5 ms — lợi thế columnar biến mất
  ```
  Thời gian dao động theo lần chạy (mình đo 9–15× tuỳ máy đang bận gì); tỉ lệ dung lượng thì cố định. **180 dòng** = 30 cửa hàng × 6 ngày. Tổng `revenue_vnd` của 180 dòng = **238 299 000** (chưa dedup — video #03 sẽ sửa).
- Bước 2 với 30 ngày: CSV 17,6 MB → Parquet 3,0 MB (5,8×); thời gian ~93 ms → ~12 ms.
- Bước 3 (output thật trên Postgres 16.4, samples 2 481 dòng; thời gian của bạn sẽ khác, **số page thì không**):
  ```
  -- OLTP, chưa có index: đọc hết 39 page heap để trả 1 dòng
  Seq Scan on pos_transactions  (cost=0.00..70.01 rows=1 width=81) (actual time=0.081..0.181 rows=1 loops=1)
    Filter: (transaction_id = 'HN01-20260215-00010'::text)
    Rows Removed by Filter: 2480
    Buffers: shared hit=39
  Execution Time: 0.194 ms

  -- OLTP, sau CREATE INDEX: đi B-tree, đụng 3 page
  Index Scan using idx_pos_transactions_txn on pos_transactions  (cost=0.28..8.30 rows=1 width=81) (actual time=0.019..0.019 rows=1 loops=1)
    Index Cond: (transaction_id = 'HN01-20260215-00010'::text)
    Buffers: shared hit=1 read=2
  Execution Time: 0.036 ms

  -- OLAP: đọc 100 % heap, không index nào được dùng
  GroupAggregate  (cost=215.55..283.32 rows=2459 width=41) (actual time=2.381..2.757 rows=180 loops=1)
    Group Key: (((ts AT TIME ZONE 'Asia/Ho_Chi_Minh'::text))::date), store_id
    ->  Sort  (cost=215.55..221.74 rows=2473 width=17) (actual time=2.371..2.472 rows=2473 loops=1)
          Sort Key: (((ts AT TIME ZONE 'Asia/Ho_Chi_Minh'::text))::date), store_id
          Sort Method: quicksort  Memory: 212kB
          ->  Seq Scan on pos_transactions  (cost=0.00..76.17 rows=2473 width=17) (actual time=0.467..1.391 rows=2473 loops=1)
                Filter: (store_id IS NOT NULL)
                Rows Removed by Filter: 8
                Buffers: shared hit=39
  Execution Time: 2.804 ms
  ```
  Index giảm Buffers từ 39 page xuống 3 và thời gian ~5×. Câu OLAP là `GroupAggregate ← Sort ← Seq Scan` (planner chọn sort-then-group vì có `ORDER BY` cùng key; với bảng lớn hơn bạn sẽ thấy `HashAggregate`), `Filter: (store_id IS NOT NULL)`, `Rows Removed by Filter: 8`, và **không dùng index nào** dù `idx_pos_transactions_store` tồn tại.
- `pg_total_relation_size('pos_transactions')` = **464 kB** (heap 312 kB + 2 index) — nhỏ hơn CSV? Không: Postgres có thêm 2 index và header mỗi dòng (~24 byte). Đó là cái giá của việc ghi/sửa từng dòng nhanh.

## Câu hỏi phỏng vấn của video
> "Sếp muốn dashboard doanh thu realtime, đề nghị 'cứ cho Metabase query thẳng vào database POS cho nhanh'. Bạn giải thích thế nào bằng query plan? Và nếu bắt buộc phải làm ngay tuần này, bạn chọn giải pháp tạm nào ít rủi ro nhất (read replica? CDC? dump hàng giờ?)."

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Thêm vào `oltp_vs_olap.py` một câu **point lookup** (`WHERE transaction_id = '...'`) và đo trên Parquet. Column store có nhanh hơn cho câu này không? Vì sao Parquet vẫn trả lời được nhanh (gợi ý: row group statistics, min/max)? Trả lời dưới comment.
2. Chạy `EXPLAIN (ANALYZE, BUFFERS)` cho câu OLAP **sau** khi `SET enable_seqscan = off;`. Planner bị ép dùng index — thời gian tăng hay giảm? Dán 2 plan vào Discussion kèm 2 dòng giải thích.
3. Vẽ lại sơ đồ bạn làm ở video #01, đánh dấu mỗi node là OLTP / OLAP / "cả hai". Có node nào bạn phân vân? Mở Discussion.
