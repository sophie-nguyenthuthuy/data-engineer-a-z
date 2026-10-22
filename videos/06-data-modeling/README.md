# #06 — Data Modeling: Star Schema, Snowflake, Data Vault

📺 Video: `TODO:youtube-link`

## Bạn sẽ học được gì
- Đi từ 5 bảng OLTP của POS tới một **star schema** trả lời được "doanh thu/margin theo ngày × cửa hàng × món × kênh" bằng 1 fact JOIN 3 dim — không CTE, không window.
- Chọn **grain** của fact và hiểu vì sao đó là quyết định quan trọng nhất của mô hình; phân biệt measure additive, degenerate dimension, surrogate key.
- Biết SCD type 1 vs type 2 qua `snapshots/menu_price_snapshot.sql` của dbt, và cách "SCD miễn phí" bằng việc lưu giá tại thời điểm bán vào fact.
- Nhận ra Snowflake schema và Data Vault (hub / link / satellite) khi gặp, và biết khi nào chúng đáng dùng — và khi nào không.
- Thấy rõ star schema bạn viết tay hôm nay chính là thứ dbt sinh ra ở video #12 (`platform/dbt/models/marts/`).

## Yêu cầu trước khi xem
- Đã xem [#03](../03-sql-cho-data-engineer) (window function, dedup) và [#02](../02-kien-truc-du-lieu) (OLTP vs OLAP).
- **Không cần Docker.** Lab chạy DuckDB trên `data/samples` (`make install`).

## Lab: thiết kế và load star schema cho Phin Phố

### Bước 1 — Thiết kế trên giấy
Mở [`lab/star_schema.md`](lab/star_schema.md): chọn grain, điền ERD Mermaid (thuộc tính từng dim, measure của fact), trả lời 4 câu hỏi ngắn (degenerate dim, `is_rainy_day` để đâu, SCD, có nên snowflake không). Render ERD tại https://mermaid.live.

### Bước 2 — Viết DDL và load
Mở [`lab/star_schema.sql`](lab/star_schema.sql). File **chạy được ngay** nhưng còn thiếu: `txn_clean` chưa sửa tz / chưa dedup, dim thiếu cột suy ra, `dim_customer` và `dim_date` chỉ có khoá, `fct_sales` rỗng. 9 TODO, mỗi cái vài dòng — phần làm sạch bạn đã viết ở #03, phần tz ở #04.
```bash
uv run python videos/06-data-modeling/lab/load_star.py videos/06-data-modeling/lab/star_schema.sql
```
Chạy lại sau mỗi TODO và nhìn số dòng đổi. Muốn tự query: thêm `--db lake-local/star.duckdb` rồi `duckdb.connect('lake-local/star.duckdb', read_only=True)` trong Python/Jupyter.

### Bước 3 — Trả lời câu hỏi nghiệp vụ
TODO 9 tạo view `rpt_revenue_margin`. Thử vài câu trên nó:
```python
import duckdb

con = duckdb.connect("lake-local/star.duckdb", read_only=True)
con.sql(
    "SELECT channel, sum(revenue_vnd), sum(margin_vnd) FROM rpt_revenue_margin WHERE business_date = '2026-02-15' GROUP BY 1"
).show()
con.sql(
    "SELECT item_name, sum(margin_vnd) m FROM rpt_revenue_margin WHERE is_tet_season GROUP BY 1 ORDER BY m DESC LIMIT 5"
).show()
```

### Bước 4 — Đối chiếu với dbt
Mở [`platform/dbt/models/marts/fct_sales.sql`](../../platform/dbt/models/marts/fct_sales.sql), [`dim_date.sql`](../../platform/dbt/models/marts/dim_date.sql), [`dim_store.sql`](../../platform/dbt/models/marts/dim_store.sql) cạnh file của bạn: cùng cột, cùng công thức `line_margin_vnd`, cùng cờ Tết. Khác biệt: dbt đọc **silver Parquet** (đã làm sạch bởi `coffee-ingest`) nên không có bước 1; và `schema.yml` biến các ràng buộc PK/FK của bạn thành **test** (`unique`, `not_null`, `relationships`). Video #12 sẽ chạy nó.

### Bước 5 — SCD2 và Data Vault
- Đọc [`platform/dbt/snapshots/menu_price_snapshot.sql`](../../platform/dbt/snapshots/menu_price_snapshot.sql): `strategy='check', check_cols=['price_vnd','cost_vnd']` — giá đổi → dbt đóng dòng cũ (`dbt_valid_to`) và mở dòng mới. Đó là SCD type 2 bằng 6 dòng config.
- Chạy ví dụ Data Vault cho store/machine:
  ```bash
  uv run python videos/06-data-modeling/lab/load_star.py videos/06-data-modeling/lab/solution/data_vault.sql
  ```
  Đọc [`lab/solution/data_vault.sql`](lab/solution/data_vault.sql): hub chỉ có business key, link chỉ có khoá, satellite có thuộc tính + `hash_diff`. View `dim_store_from_vault` dựng lại dim từ Vault — star schema sống **phía trên** Vault.

### Bước 6 — So với lời giải và chạy test
[`lab/solution/star_schema.md`](lab/solution/star_schema.md), [`lab/solution/star_schema.sql`](lab/solution/star_schema.sql).
```bash
uv run pytest videos/06-data-modeling -q
```
Test load star schema và **so với silver của `coffee_ingestion`** (`clean_transactions`, `clean_line_items`): số line item và tổng doanh thu phải khớp từng đồng.

## Kết quả mong đợi
- `load_star.py` trên **starter** (chưa làm gì): `fct_sales 0`, `txn_clean 2 473` (chưa dedup), `lines_clean 4 320` (chưa bỏ quantity âm), `dim_date 1 095`.
- Trên **solution** / bài làm xong:
  ```
  dim_customer   2,000    dim_date   1,095    dim_menu_item   23    dim_store   30
  fct_sales      4,302    lines_clean 4,302   txn_clean    2,468    rpt_revenue_margin 2,551
  ```
  `txn_clean` 2 468 = 2 481 − 5 trùng − 8 thiếu store. `fct_sales` 4 302 = 4 328 − 10 `negative_qty` − 16 line của hoá đơn thiếu store.
- `SELECT sum(line_total_vnd), sum(line_margin_vnd) FROM fct_sales` → **240 469 000** và **181 623 000**. Tổng hoá đơn gộp (`total + discount`) là 241 002 000, lệch 533 000 = đúng giá trị 10 line hoàn-hàng-sai bị loại.
- Bước 3, ngày 15/02: `app` 29 881 000 / margin 22 587 000 · `store` 47 972 000 / margin 36 246 000.
- `dim_date`: 2026-02-15 `is_weekend=true, is_tet_season=true, is_tet_holiday=false`; 2026-02-17 `is_tet_holiday=true`.
- Data Vault: `hub_store` 30, `hub_machine` 60, `link_store_machine` 60, `sat_store` 30, `dim_store_from_vault` 30 dòng.
- `pytest`: 6 passed.

## Câu hỏi phỏng vấn của video
> "Ngày 1/3 Phin Phố tăng giá Bạc xỉu từ 39k lên 45k. Với star schema của bạn, báo cáo margin tháng 2 có đổi không? Nếu PM hỏi 'margin tháng 2 *theo giá mới* là bao nhiêu' thì mô hình hiện tại trả lời được không? Bạn chọn SCD type 1, type 2, hay lưu giá vào fact — và cái giá phải trả của mỗi cách?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Thêm `dim_payment_method(payment_method, provider, fee_pct)` (tự bịa phí: momo 1,5 %, zalopay 1,2 %, vnpay 1,0 %, card 2,0 %, cash 0, app_wallet 0) và tính **margin sau phí thanh toán** theo cửa hàng ngày 15/02. Việc này có làm bạn đổi ý về degenerate dimension ở bước 1 không? Dán DDL + kết quả dưới comment.
2. Viết `fct_daily_store_revenue` (grain ngày × cửa hàng) từ `fct_sales` của bạn và so với [`platform/dbt/models/marts/fct_daily_store_revenue.sql`](../../platform/dbt/models/marts/fct_daily_store_revenue.sql). Vì sao dbt tính từ `stg_pos_transactions` chứ không từ `fct_sales`? Số có khớp không (gợi ý: 10 line bị quarantine)?
3. Mô phỏng SCD2 bằng tay: tạo `dim_menu_item_scd2(menu_item_sk, item_id, price_vnd, cost_vnd, valid_from, valid_to, is_current)`, insert giá hiện tại với `valid_from = '2026-01-01'`, rồi "tăng giá Bạc xỉu ngày 1/3": đóng dòng cũ, mở dòng mới. `fct_sales` phải trỏ `menu_item_sk` nào cho hoá đơn ngày 15/02? Mở Discussion kèm SQL.
