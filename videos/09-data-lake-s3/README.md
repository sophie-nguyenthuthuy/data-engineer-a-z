# #09 — Data Lake trên S3: bronze/silver/gold

📺 Video: `TODO:youtube-link`

## Bạn sẽ học được gì
- Hiểu quy ước lake của repo ([`platform/lake/README.md`](../../platform/lake/README.md)): 3 layer, partition `dt=` kiểu Hive, overwrite nguyên partition, UTC, int64 VND, quarantine thay vì xoá — và **vì sao** từng quy ước tồn tại.
- Chạy đúng pipeline của video #07 nhưng đích là MinIO (`--lake s3://lake`) mà **không đổi một dòng code** — nhờ `Lake` trong `lake.py`.
- Đọc Parquet trên S3/MinIO bằng DuckDB (`httpfs`), bằng pyarrow (`S3FileSystem`), và duyệt bằng MinIO Console.
- Tự viết công cụ kiểm kê lake (partition, số file, dung lượng, số dòng) chỉ đọc *listing + footer*, không tải dữ liệu.
- Biết S3 thật khác MinIO ở chỗ nào (IAM, chi phí request, lifecycle) và lúc nào mới nên bật AWS.

## Yêu cầu trước khi xem
- Đã xem #07 và #08.
- `make up` (profile `core`: cần `minio` chạy — kiểm tra bằng `make ps`). Bước 5 chạy được **offline** với `./lake-local`.

## Lab: đưa lake lên MinIO rồi kiểm kê

### Bước 1 — Đọc quy ước
Đọc [`platform/lake/README.md`](../../platform/lake/README.md) (1 trang). Với mỗi dòng trong bảng "Nguyên tắc", tự tìm **dòng code** tương ứng trong `platform/ingestion/src/coffee_ingestion/` (ví dụ "overwrite nguyên partition" → `Lake.write_parquet` luôn ghi `part-000.parquet`; "UTC" → `parse_vn_timestamp`).

### Bước 2 — Ingest thẳng lên MinIO
```bash
export S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
for d in 2026-02-14 2026-02-15 2026-02-16 2026-02-17; do
  uv run coffee-ingest run --source data/samples --lake s3://lake --date $d | tail -3
done
```
Khác duy nhất so với #07 là `--lake s3://lake` và 3 biến môi trường. Mở `lake.py` → `_fs()` để thấy pyarrow `S3FileSystem(endpoint_override=...)` được tạo thế nào. Cùng lệnh này, bỏ `S3_ENDPOINT` và dùng credentials AWS thật → ghi lên S3 thật.

### Bước 3 — Duyệt bằng MinIO Console và `mc`
- http://localhost:9001 (`minioadmin`/`minioadmin`) → bucket `lake` → `silver/pos_transactions/dt=2026-02-15/part-000.parquet`. Để ý thư mục trên S3 **không tồn tại thật** — chỉ là prefix của object key; Console vẽ cây cho bạn dễ nhìn.
- Dòng lệnh:
```bash
docker compose exec minio mc ls --recursive local/lake/silver/pos_transactions/
docker compose exec minio mc du local/lake/bronze/ local/lake/silver/
```

### Bước 4 — Query trực tiếp trên MinIO bằng DuckDB
```bash
uv run python -c "
import duckdb
con = duckdb.connect()
con.sql(\"INSTALL httpfs; LOAD httpfs;\")
con.sql(\"SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false; SET s3_url_style='path';\")
con.sql(\"SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin';\")
print(con.sql(\"SELECT dt, count(*) n, sum(total_amount_vnd) revenue_vnd FROM read_parquet('s3://lake/silver/pos_transactions/*/*.parquet', hive_partitioning=true) GROUP BY 1 ORDER BY 1\"))
print(con.sql(\"SELECT count(*) FROM read_parquet('s3://lake/silver/pos_transactions/*/*.parquet', hive_partitioning=true) WHERE dt = DATE '2026-02-15'\"))
"
```
`hive_partitioning=true` biến thư mục `dt=2026-02-15` thành cột `dt`; `WHERE dt = ...` làm DuckDB **chỉ tải object của partition đó** (partition pruning). Đây chính là lý do repo partition theo ngày nghiệp vụ — dbt ở video #12 dùng y hệt cách này.

### Bước 5 — Viết `lake_inventory.py`
Mở [`lab/lake_inventory.py`](lab/lake_inventory.py), điền 4 TODO: chọn filesystem (local hay S3, dùng lại `Lake._fs()`), liệt kê file bằng `pyarrow.fs.FileSelector`, tách `layer/table/partition` từ đường dẫn, đếm dòng bằng footer Parquet.
```bash
# offline — không cần MinIO
uv run coffee-ingest run --source data/samples --lake ./lake-local --date 2026-02-15
uv run python videos/09-data-lake-s3/lab/lake_inventory.py --lake ./lake-local
# trên MinIO (vẫn cần 3 biến môi trường ở bước 2)
uv run python videos/09-data-lake-s3/lab/lake_inventory.py --lake s3://lake
uv run pytest videos/09-data-lake-s3 -q
```
Lời giải: [`lab/solution/lake_inventory.py`](lab/solution/lake_inventory.py). Test tạo lake riêng trong `tmp_path` bằng `coffee_ingestion.pipeline.run` nên không đụng `./lake-local` của bạn.

### Bước 6 (tuỳ chọn, có phí) — S3 thật trên AWS
Chỉ làm nếu bạn muốn ghi vào CV "đã dựng lake trên AWS". Đọc [`infra/terraform/README.md`](../../infra/terraform/README.md): `terraform apply` tạo 1 bucket (versioning, block public access, lifecycle xoá bronze sau 90 ngày), 1 Glue database, 1 IAM user least-privilege. Sau đó:
```bash
unset S3_ENDPOINT                       # không override endpoint nữa → đi AWS thật
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=ap-southeast-1
uv run coffee-ingest run --source data/samples --lake s3://<bucket-cua-ban> --date 2026-02-15
uv run python videos/09-data-lake-s3/lab/solution/lake_inventory.py --lake s3://<bucket-cua-ban> --no-rows
```
⚠️ Chi phí: dung lượng lab ~0,5 MB nên gần như 0 đồng, nhưng **request** (PUT/LIST) và bucket quên không xoá mới tốn. Luôn `terraform destroy` khi xong. Video #22 và #23 nói kỹ về Terraform và chi phí.

## Kết quả mong đợi
- Bước 2: 4 lần chạy, mỗi lần JSON summary như #07, `path` giờ là `s3://lake/bronze/...`. Ngày 15/02 vẫn `rows_out: 792`.
- Bước 3: `mc ls --recursive` liệt kê 4 object `dt=2026-02-1[4-7]/part-000.parquet`, mỗi file 5–16 KB. <!-- TODO(thuy): dán output mc ls/du thật sau khi quay -->
- Bước 4: bảng 4 dòng — `2026-02-14 | 766 | 73397000`, `2026-02-15 | 792 | 76939000`, `2026-02-16 | 730 | 70254000`, `2026-02-17 | 59 | 5549000`; query thứ hai trả `792`.
- Bước 5 với 4 ngày đã ingest (số đo trên thư mục local; trên MinIO `bytes` giống hệt vì cùng file):

```
layer   table                           partition        files     bytes    rows
bronze  pos_transactions                dt=2026-02-15        1    16,489     799
...
silver  pos_transactions                dt=2026-02-15        1    15,322     792
silver  quarantine_pos_transactions     dt=2026-02-15        1     1,923       4
...
bronze   tables=5 partitions=20 files=20 bytes=262,618 rows=14,686
silver   tables=7 partitions=28 files=28 bytes=238,512 rows=14,682
```
  Hai điều đáng để ý: (1) `customers` và `stores` được chụp lại **nguyên bản mỗi ngày** (4 partition × 2 000 khách) — đó là kiểu *full snapshot dimension*, staging của dbt sẽ chỉ lấy bản mới nhất; (2) tổng dòng silver (14 682) chỉ kém bronze (14 686) 4 dòng vì quarantine được tính vào silver — không dòng nào "biến mất".
- `pytest videos/09-data-lake-s3 -q` → **3 passed**.

## Câu hỏi phỏng vấn của video
> "Lake của bạn có 3 năm dữ liệu POS partition theo ngày, mỗi ngày 1 file. Một hôm team phát hiện bug ở silver từ 6 tháng trước. Bạn làm gì, mất bao lâu, và thiết kế nào trong repo này làm việc đó khả thi? Nếu bronze cũng bị xoá theo lifecycle 90 ngày thì sao?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Thêm cột `avg_file_mb` vào inventory và cảnh báo (in `⚠️`) khi partition nào có trung bình file < 1 MB hoặc > 512 MB. Với samples thì partition nào bị cảnh báo? Vì sao ở quy mô thật đây là vấn đề (xem #11)?
2. Chạy `coffee-ingest run --date 2026-02-15` lần thứ hai lên MinIO, rồi vào Console bật **Versioning** cho bucket `lake` và chạy lần thứ ba. Object có bao nhiêu version? Điều này liên quan gì tới "idempotent" và tới chi phí lưu trữ?
3. (Discussion) Nếu phải chọn partition cho bảng `iot_readings` (60 máy, 1 bản ghi/phút/máy): theo `dt`, theo `dt/store_id`, hay `dt/hour`? Tính số file/ngày cho mỗi phương án và giải thích lựa chọn của bạn.
