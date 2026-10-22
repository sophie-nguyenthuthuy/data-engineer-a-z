# platform/dbt — gold layer (video #12)

dbt project `coffee_gold`: silver Parquet → staging views → star schema (dim_*, fct_*).

```bash
make smoke                                   # tạo ./lake-local/silver từ data/samples
cd platform/dbt
uv run dbt debug  --profiles-dir .           # target dev = DuckDB đọc parquet trực tiếp
uv run dbt build  --profiles-dir .           # run + test + snapshot
uv run dbt docs generate --profiles-dir . && uv run dbt docs serve --profiles-dir .
```

CI chỉ chạy `dbt parse --target ci` (DuckDB in-memory, không cần file nào) để bắt lỗi Jinja/YAML.

| Thư mục | Nội dung |
|---------|----------|
| `models/staging` | 1 view / bảng silver, đổi tên cột, UTC → giờ VN, mask PII |
| `models/marts` | `dim_store`, `dim_menu_item`, `dim_customer`, `dim_date`, `fct_sales` (grain line item), `fct_daily_store_revenue` |
| `snapshots` | SCD2 bảng giá |
| `tests/generic` | generic test tự viết (không cần dbt_utils để bạn đọc được code) |
| `tests/*.sql` | singular test |
