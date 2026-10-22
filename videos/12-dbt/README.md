# #12 — dbt: biến silver thành gold bằng SQL có kiểm thử

📺 Video: `TODO:youtube-link` · 📑 Slide: [V12-dbt.pdf](slides/V12-dbt.pdf)

## Bạn sẽ học được gì
- Hiểu vì sao chữ **T** nặng của ELT nên viết bằng SQL + dbt thay vì Python: `ref()`/`source()` tạo DAG tự động, test đi kèm model, docs sinh từ code.
- Chạy project [`platform/dbt`](../../platform/dbt) (`coffee_gold`) trên DuckDB đọc **trực tiếp Parquet silver** — không load dữ liệu vào đâu cả — và đọc hiểu 45 kết quả PASS.
- Phân biệt 6 khái niệm bằng file thật trong repo: `source` / `ref` / materialization (view vs table) / snapshot (SCD2) / test (generic, singular, tự viết) / docs.
- Thêm một model mới có test, chạy riêng nó bằng `--select`, và xem nó xuất hiện trong lineage graph.
- Biết `incremental` là gì và vì sao repo **chưa** dùng nó (bài tập).

## Yêu cầu trước khi xem
- Đã xem #07 (silver là gì) và #06 (star schema: dim/fct).
- `make install`. **Không cần Docker**: target `dev` là file DuckDB đọc `./lake-local`.

## Lab: build gold, rồi thêm bảng của bạn

### Bước 1 — Tạo silver cho 4 ngày
dbt chỉ hay khi có nhiều hơn một ngày. Chạy pipeline #07 cho 14→17/02 (trước Tết tới mùng 1):
```bash
for d in 2026-02-14 2026-02-15 2026-02-16 2026-02-17; do
  uv run coffee-ingest run --source data/samples --lake ./lake-local --date $d | tail -1
done
find lake-local/silver -name "*.parquet" | wc -l     # 28
```

### Bước 2 — `dbt debug` rồi `dbt build`
```bash
cd platform/dbt
uv run dbt debug --profiles-dir . --target dev
uv run dbt build --profiles-dir . --target dev
```
`build` = run + test + snapshot theo đúng thứ tự DAG. Mở [`profiles.yml`](../../platform/dbt/profiles.yml): target `dev` là DuckDB với extension `httpfs` + `parquet`; đổi `LAKE_ROOT=s3://lake` (và endpoint MinIO) là đọc thẳng từ #09 — không sửa model nào.

### Bước 3 — Đọc project theo thứ tự DAG
| File | Khái niệm | Điều cần để ý |
|------|-----------|---------------|
| [`models/staging/sources.yml`](../../platform/dbt/models/staging/sources.yml) | **source** | `external_location: read_parquet('.../silver/{name}/*/*.parquet', hive_partitioning = true)` — dbt-duckdb biến mỗi source thành một `read_parquet()`; `dt` thành cột nhờ Hive partitioning. Test `unique`/`not_null` đặt **trên source** để bắt lỗi của #07 trước khi build gì. |
| [`stg_pos_transactions.sql`](../../platform/dbt/models/staging/stg_pos_transactions.sql) | staging, **view** | UTC → `ordered_at_local` (giờ VN) và `business_date` — chỗ **duy nhất** đổi timezone trong cả lake. |
| [`stg_stores.sql`](../../platform/dbt/models/staging/stg_stores.sql) | `qualify row_number() ... order by dt desc` | Dimension được chụp lại nguyên bản mỗi ngày (4 partition × 30 store) → staging lấy bản mới nhất. |
| [`stg_customers.sql`](../../platform/dbt/models/staging/stg_customers.sql) + [`macros/mask_pii.sql`](../../platform/dbt/macros/mask_pii.sql) | **macro** | PII dừng ở staging; `phone_masked` giữ 3 số cuối. |
| [`dbt_project.yml`](../../platform/dbt/dbt_project.yml) | **materialization** | `staging: view` (rẻ, luôn mới), `marts: table` (BI query nhanh). |
| [`marts/fct_sales.sql`](../../platform/dbt/models/marts/fct_sales.sql), [`fct_daily_store_revenue.sql`](../../platform/dbt/models/marts/fct_daily_store_revenue.sql) | **ref** | `ref('stg_pos_transactions')` → dbt biết thứ tự build và vẽ lineage. |
| [`marts/schema.yml`](../../platform/dbt/models/marts/schema.yml) | **generic test** | `unique`, `not_null`, `relationships` (FK tới dim) có sẵn; `dbt_utils_free_unique_combination` là generic test **tự viết** trong [`tests/generic/`](../../platform/dbt/tests/generic) — mở ra đọc, 6 dòng SQL. |
| [`tests/assert_revenue_non_negative.sql`](../../platform/dbt/tests/assert_revenue_non_negative.sql) | **singular test** | Một câu SELECT; trả về dòng nào là FAIL. |
| [`snapshots/menu_price_snapshot.sql`](../../platform/dbt/snapshots/menu_price_snapshot.sql) | **snapshot** (SCD2) | `strategy='check'`, `check_cols=[price_vnd, cost_vnd]`: giá đổi → dbt đóng dòng cũ (`dbt_valid_to`), mở dòng mới. |
| [`analyses/revenue_tet_vs_normal.sql`](../../platform/dbt/analyses/revenue_tet_vs_normal.sql) | analysis | Compile nhưng không materialize — SQL để "hỏi", không phải bảng. |

Kiểm tra bằng DuckDB (file do dbt tạo):
```bash
uv run python -c "
import duckdb; con = duckdb.connect('target/coffee_gold.duckdb', read_only=True)
print(con.sql('select business_date, count(*) stores, sum(transactions) txns, sum(revenue_vnd) revenue_vnd from main_gold.fct_daily_store_revenue group by 1 order by 1'))
print(con.sql('select count(*) from main_gold.fct_sales'))
print(con.sql('select item_id, price_vnd, dbt_valid_from, dbt_valid_to from snapshots.menu_price_snapshot limit 3'))
"
```

### Bước 4 — Docs và lineage
```bash
uv run dbt docs generate --profiles-dir . --target dev
uv run dbt docs serve --profiles-dir . --port 8099
```
Mở http://localhost:8099 → nút lineage (góc phải dưới) → bấm `fct_sales`: thấy `silver.pos_line_items` → `stg_pos_line_items` → `fct_sales` ← `stg_menu_items`. Mọi `description` trong YAML hiện ở đây — đây là data catalog miễn phí của team bạn.

### Bước 5 — Thêm model `fct_hourly_revenue`
Bài toán: giờ cao điểm thật của Phin Phố là mấy giờ, ở từng cửa hàng? Mở [`lab/models/fct_hourly_revenue.sql`](lab/models/fct_hourly_revenue.sql) và [`lab/models/fct_hourly_revenue.yml`](lab/models/fct_hourly_revenue.yml), điền 8 TODO (grain = `business_date × store_id × hour_local`, giờ **Việt Nam**; test unique-combination, relationships tới `dim_store`, accepted_values 0–23).

dbt chỉ nhìn thấy model nằm trong `models/`, nên bạn **copy** file vào project rồi build riêng model đó:
```bash
cp videos/12-dbt/lab/models/fct_hourly_revenue.sql videos/12-dbt/lab/models/fct_hourly_revenue.yml platform/dbt/models/marts/
cd platform/dbt && uv run dbt build --profiles-dir . --target dev --select fct_hourly_revenue
uv run dbt build --profiles-dir . --target dev            # cả project, giờ là 54 node
```
Khi xong, **xoá 2 file đã copy** để `git status` của repo sạch (`rm platform/dbt/models/marts/fct_hourly_revenue.*`) — nếu bạn fork repo và muốn giữ, hãy commit vào `platform/dbt` như một PR thật. Lời giải: [`lab/solution/models/`](lab/solution/models/).

### Bước 6 — Dọn
`lake-local/` và `platform/dbt/target/` đều trong `.gitignore`. `make reset` xoá lake-local; `rm -rf platform/dbt/target` nếu muốn build sạch.

## Kết quả mong đợi
- Bước 1: 4 lần `rows_out` lần lượt **766 / 792 / 730 / 59**; `find` đếm **28** file silver (7 bảng × 4 ngày).
- Bước 2: `dbt debug` → `All checks passed!`. `dbt build` → `Done. PASS=45 WARN=0 ERROR=0 SKIP=0 TOTAL=45` (1 snapshot, 5 view, 6 table, 33 test), chạy ~1 giây.
- Bước 3, bảng `fct_daily_store_revenue` có **120 dòng** (30 store × 4 ngày — kể cả mùng 1 Tết mỗi cửa hàng vẫn có ít nhất 1 hoá đơn):

| business_date | stores | txns | revenue_vnd |
|---|---:|---:|---:|
| 2026-02-14 | 30 | 766 | 73 397 000 |
| 2026-02-15 | 30 | 792 | 76 939 000 |
| 2026-02-16 | 30 | 730 | 70 254 000 |
| 2026-02-17 | 30 | 59 | 5 549 000 |

  `fct_sales` **4 090** dòng (grain line item); `menu_price_snapshot` 23 dòng, tất cả `dbt_valid_to = NULL` (giá chưa đổi lần nào — bài tập 2 làm nó đổi). `dim_store`: 12 flagship / 9 standard / 9 kiosk. `analyses/revenue_tet_vs_normal`: ngày thường TB **73,53 triệu**, mùng 1 Tết **5,55 triệu** (−92 %).
- Bước 5 với lời giải: `--select fct_hourly_revenue` → `PASS=9` (1 model + 8 test); build cả project → `PASS=54`. Bảng có **1 163 dòng** (376 dòng cho ngày 15/02). Giờ cao điểm 15/02 toàn chuỗi: **15 h = 8 862 000** (81 hoá đơn), 8 h = 8 159 000 (87), 7 h = 7 590 000 (80) — đúng hai đỉnh 7–9 h và 14–16 h mô tả trong SCHEMA.md. Ô cao nhất: `SG14` lúc 16 h, 8 hoá đơn, 1 054 000 VND.
- Bẫy cố ý: nếu bạn đặt `dbt_utils_free_positive` lên `revenue_vnd` sẽ **FAIL 1** — ô `2026-02-16 / SG04 / 17 h` có đúng 1 hoá đơn `total_amount_vnd = 0` (voucher 15 000 ≥ tiền hàng, mã `SG04-20260216-00024`). Doanh thu 0 hợp lệ, âm mới sai → test đúng là `not_null` (hoặc singular test `< 0`). Đây là cách test dbt dạy bạn về nghiệp vụ.

## Câu hỏi phỏng vấn của video
> "Model `fct_sales` của bạn build lại toàn bộ 3 năm dữ liệu mỗi đêm, mất 40 phút. Bạn chuyển sang incremental thế nào, chọn `unique_key` gì, và điều gì xảy ra khi silver của một ngày cũ được re-process (late-arriving data)? Khi nào bạn vẫn chọn full refresh?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Chuyển `fct_hourly_revenue` sang `materialized='incremental'` với `unique_key=['business_date','store_id','hour_local']` và khối `{% if is_incremental() %}` lọc `business_date >= (select max(business_date) - 1 from {{ this }})`. Chạy 2 lần, so log — lần 2 có `INSERT`/`MERGE` gì? Vì sao trừ 1 ngày?
2. Sửa giá `CF01` trong `data/samples/menu_items.csv` lên +2 000 (bản copy của bạn), chạy lại `coffee-ingest` cho ngày 17/02 và `dbt snapshot`. `menu_price_snapshot` giờ có mấy dòng cho `CF01`, `dbt_valid_to` là gì? Nhớ `git checkout data/samples` sau đó.
3. (Discussion) Team BI đòi thêm cột `revenue_usd` vào `fct_daily_store_revenue`. Bạn để tỉ giá ở đâu: hard-code trong model, `seed` CSV, hay source riêng? Trade-off của từng cách về test và lineage?
