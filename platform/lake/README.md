# platform/lake — quy ước Data Lake (video #09, #17)

Bucket: `lake` (MinIO local) hoặc bucket S3 của bạn (Terraform ở `infra/terraform`).

```
s3://lake/
├── bronze/<table>/dt=YYYY-MM-DD/part-*.parquet     # raw, cột toàn string, + _ingested_at/_source/_run_date
├── silver/<table>/dt=YYYY-MM-DD/part-*.parquet     # typed, dedup, UTC; quarantine_* bên cạnh
└── gold/   (dbt quản lý, hoặc Iceberg table ở warehouse/)
s3://warehouse/                                      # Iceberg warehouse — catalog JDBC trong Postgres (db iceberg_catalog) — video #17
└── silver/pos_transactions/{metadata,data}/
```

| Nguyên tắc | Vì sao |
|-----------|--------|
| Partition theo **ngày nghiệp vụ** `dt=` (Hive style) | Mọi engine (Spark, DuckDB, Trino, Athena) đọc được; filter theo ngày là query phổ biến nhất |
| Bronze **không bao giờ** sửa giá trị | Replay silver từ đầu khi phát hiện bug |
| Ghi lại partition **ghi đè nguyên partition** (overwrite), không append | Re-run idempotent, không sinh duplicate |
| File 64–256 MB, nén zstd | Quá nhiều file nhỏ → chậm (video #11) |
| Timestamp lưu **UTC**, hiển thị giờ VN ở BI | Một quy ước duy nhất; tz drift chỉ sửa một lần ở silver |
| Tiền để **int64 VND**, không float | VND không có xu; float sinh lỗi làm tròn |
| Quarantine thay vì xoá | Dữ liệu bẩn cũng là dữ liệu — team nguồn cần thấy để sửa |

`iceberg/` chứa DDL cho bảng Iceberg tương ứng (video #17) — chạy bằng `spark-sql` hoặc Trino.
