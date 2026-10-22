# #03 — SQL cho Data Engineer: window function, CTE, query plan

📺 Video: `TODO:youtube-link` · 📑 Slide: [V03-SQL-cho-Data-Engineer.pdf](slides/V03-SQL-cho-Data-Engineer.pdf)

## Bạn sẽ học được gì
- Dedup đúng cách bằng `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …)` — và vì sao `DISTINCT` không đủ.
- Dùng window function cho 2 bài kinh điển trong phỏng vấn: running total và top-N mỗi nhóm (`DENSE_RANK` vs `RANK` vs `ROW_NUMBER`).
- Lập lịch bằng `generate_series` / CTE đệ quy để báo cáo **không bị mất dòng** ở ngày/giờ không có dữ liệu.
- Bắt lỗi `tz_drift` (timestamp thiếu `+07:00`) — và hiểu vì sao phải bắt **trước** khi dữ liệu thành `TIMESTAMPTZ`.
- Đọc `EXPLAIN ANALYZE` của DuckDB và Postgres: Table Scan, Sort, WindowAgg, HashAggregate.

## Yêu cầu trước khi xem
- Đã xem [#02](../02-kien-truc-du-lieu). Biết `SELECT … GROUP BY … JOIN` cơ bản.
- Bước 1–3 chạy **offline** bằng DuckDB (chỉ cần `make install`). Bước 4 cần Postgres: `make up`.

## Lab: 6 bài SQL trên dữ liệu POS

Cả 6 bài viết 2 lần: DuckDB đọc thẳng CSV (nhanh, không Docker) rồi "dịch" sang Postgres (dialect thật ở công ty).

### Bước 1 — Chạy starter để thấy khung
```bash
uv run python videos/03-sql-cho-data-engineer/lab/run_sql.py videos/03-sql-cho-data-engineer/lab/queries.sql
```
`run_sql.py` chạy từng câu (mỗi câu bắt đầu bằng dòng `-- Qn`), câu còn chữ `TODO` thì báo "chưa làm", câu lỗi thì in lỗi rồi đi tiếp. Ba câu `Q0*` nạp CSV vào bảng tạm — chú ý `all_varchar=true` ở `pos_transactions`: mình **cố tình không** cho DuckDB đoán kiểu, để `ts` còn là chuỗi.

### Bước 2 — Làm 6 bài trong [`lab/queries.sql`](lab/queries.sql)
| Bài | Kỹ thuật | Kết quả mong đợi |
|-----|----------|------------------|
| Q1a–d | dedup `ROW_NUMBER`, tạo view `txn_clean` | 5 id trùng; 2 481 → 2 476 dòng |
| Q2 | running total `SUM() OVER (PARTITION BY … ORDER BY …)` | 180 dòng |
| Q3 | top 3 món / cửa hàng, `DENSE_RANK` | 90 dòng |
| Q4a–b | `generate_series`, `WITH RECURSIVE` | 24 giờ (6 giờ = 0), 6 ngày |
| Q5a–b | tz_drift trên chuỗi thô, `LIKE '%+07:00'` | 11 dòng |
| Q6 | `EXPLAIN ANALYZE` | đọc plan từ dưới lên |

Mỗi TODO có gợi ý ngay trong comment. Xoá chữ `TODO` khi làm xong để runner chạy câu đó. Chạy lại lệnh ở bước 1 sau mỗi câu.

### Bước 3 — So với lời giải và chạy test
```bash
uv run python videos/03-sql-cho-data-engineer/lab/run_sql.py videos/03-sql-cho-data-engineer/lab/solution/queries.sql
uv run pytest videos/03-sql-cho-data-engineer -q
```
Test đọc chính file `solution/queries.sql` và assert số dòng thật — bạn có thể trỏ test sang file của mình để tự chấm.

### Bước 4 — Dịch sang Postgres
```bash
make up
docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/03-sql-cho-data-engineer/lab/queries_postgres.sql
```
Ba điểm khác ([`lab/queries_postgres.sql`](lab/queries_postgres.sql) ghi rõ): `ts` đã là `TIMESTAMPTZ` nên `business_date` phải `AT TIME ZONE 'Asia/Ho_Chi_Minh'`; không có `QUALIFY`/`EXCLUDE`; và **tz_drift không còn dấu vết** trong bảng — phải `COPY` lại CSV thô (đã mount tại `/opt/samples`) vào bảng tạm toàn `TEXT`. Lời giải: [`lab/solution/queries_postgres.sql`](lab/solution/queries_postgres.sql).

## Kết quả mong đợi
- **Q1a**: 5 dòng — `HN08-20260215-00035`, `HN10-20260218-00002`, `HN11-20260215-00015`, `SG02-20260214-00012`, `SG02-20260215-00006`, mỗi id 2 bản. **Q1b**: `rows_before=2481, rows_after=2476`.
- **Q1d** (sau dedup + bỏ 8 dòng `store_id` NULL), khớp với silver của `coffee-ingest`:

  | business_date | transactions | revenue_vnd |
  |---|---|---|
  | 2026-02-14 | 766 | 73 397 000 |
  | 2026-02-15 | 792 | 76 939 000 |
  | 2026-02-16 | 730 | 70 254 000 |
  | 2026-02-17 | 59 | 5 549 000 |
  | 2026-02-18 | 62 | 6 488 000 |
  | 2026-02-19 | 59 | 5 275 000 |
- **Q2**: 180 dòng; `HN01` chạy 1 111 000 → 2 693 000 → 3 835 000 → 3 925 000 → 4 075 000 → 4 110 000 (nhìn thấy Tết "phanh" từ 17/02).
- **Q3**: 90 dòng. `HN01`: Trà đào cam sả 471 000 · Trà vải 452 000 · Cà phê sữa đá 380 000. `SG14`: Bạc xỉu 1 593 000 · Cà phê sữa đá 1 540 000 · Trà đào cam sả 1 114 000.
- **Q4a**: 24 dòng; giờ 0–4 và 23 = 0; cao điểm 7 h = 80, 8 h = 87, 15 h = 82. **Q4b**: 6 dòng, `is_tet_mung_1 = true` ở 2026-02-17.
- **Q5a**: 11 dòng, tất cả có dạng `2026-02-15 08:50:00` (dấu cách, không offset). **Q5b**: `HN07-20260214-00014` đọc sai thành `2026-02-15 04:06:37`, đọc đúng là `2026-02-14 21:06:37`.
- **Q6**: plan có `Table Scan` ở dưới cùng với `Projections: transaction_id, ts` (chỉ đọc 2 cột). DuckDB đổi `WINDOW` thành `Hash Group By` vì ta chỉ `count(*)` — optimizer được phép viết lại câu của bạn.
- **Postgres (bước 4)**: Q1d lệch so với DuckDB đúng **172 000 VND** giữa ngày 14 và 15 — chính là hoá đơn `HN07-20260214-00014` bị trôi 7 giờ. Q5b vẫn 11 dòng; Q5d (heuristic giờ đóng cửa) chỉ bắt được 2/11. Q6 thật trên Postgres 16.4:
  ```
  Aggregate  (cost=284.36..284.37 rows=1 width=8) (actual time=7.528..7.528 rows=1 loops=1)
    ->  Subquery Scan on ranked  (cost=203.70..284.33 rows=12 width=0) (actual time=5.775..7.243 rows=2476 loops=1)
          Filter: (ranked.rn = 1)
          ->  WindowAgg  (cost=203.70..253.32 rows=2481 width=217) (actual time=5.774..6.949 rows=2476 loops=1)
                Run Condition: (row_number() OVER (?) <= 1)
                ->  Sort  (cost=203.70..209.90 rows=2481 width=28) (actual time=5.767..5.938 rows=2481 loops=1)
                      Sort Key: pos_transactions.transaction_id, pos_transactions.ts
                      Sort Method: quicksort  Memory: 232kB
                      ->  Seq Scan on pos_transactions  (cost=0.00..63.81 rows=2481 width=28) (actual time=0.003..0.246 rows=2481 loops=1)
                            Buffers: shared hit=39
  Execution Time: 7.564 ms
  ```
  Đọc từ dưới lên: `Seq Scan` 39 page → `Sort` theo `(transaction_id, ts)` → `WindowAgg`. Để ý `Run Condition: row_number() <= 1` — Postgres ≥ 15 đẩy điều kiện `rn = 1` **vào trong** WindowAgg nên dừng sớm mỗi partition; DuckDB thì viết lại hẳn thành `Hash Group By`. Hai optimizer, hai cách, cùng kết quả 2 476.

## Câu hỏi phỏng vấn của video
> "Bảng hoá đơn có bản ghi trùng do retry. Bạn dedup bằng `ROW_NUMBER() ... ORDER BY ts` — nhưng hai bản sao có `ts` giống hệt nhau. Kết quả có deterministic không? Nếu hai bản sao khác nhau ở `total_amount_vnd` (bản sau là bản sửa), bạn đổi `ORDER BY` thế nào và cần thêm cột gì ở nguồn để làm được việc đó?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Viết Q3 ba lần với `ROW_NUMBER`, `RANK`, `DENSE_RANK`. Tạo một trường hợp hoà điểm (sửa 1 dòng trong bảng tạm `raw_lines`) và chỉ ra 3 kết quả khác nhau thế nào. Dán 3 output dưới comment.
2. Viết lại Q2 để tính **doanh thu trung bình trượt 3 ngày** (`ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`). Ngày 17/02 của `SG14` ra bao nhiêu?
3. Trong Postgres, chạy `EXPLAIN (ANALYZE, BUFFERS)` câu Q1b trước và sau `CREATE INDEX ON pos_transactions (transaction_id, ts)`. Node `Sort` còn không? Thời gian đổi thế nào? Mở Discussion kèm 2 plan (nhớ `DROP INDEX` sau khi xong).
