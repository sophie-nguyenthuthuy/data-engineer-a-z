# #17 — Lakehouse: Iceberg vs Delta vs Hudi

📺 Video: `TODO:youtube-link`

## Bạn sẽ học được gì
- Hiểu vì sao "thư mục Parquet + `dt=`" ở #09 thiếu 4 thứ: **ACID**, **schema evolution**, **time travel**, **liệt kê file nhanh** — và table format ra đời để bù đúng 4 thứ đó.
- Đọc được cây metadata của Iceberg (catalog → `metadata.json` → manifest list → manifest → data file) và tự tay in snapshots bằng 60 dòng Python stdlib.
- Chạy `MERGE INTO` hai lần cùng một ngày mà **không sinh duplicate**, time travel về snapshot cũ, `ADD COLUMN` không ghi lại dữ liệu.
- So sánh Iceberg / Delta Lake / Hudi theo thứ bạn sẽ bị hỏi trong phỏng vấn: metadata, catalog, engine, ai dùng ở Việt Nam.

## Yêu cầu trước khi xem
- Đã xem #09 (Data Lake, quy ước `dt=`), #10 (Spark). Hiểu `MERGE` trong SQL (#03).
- `make up PROFILE=spark` (MinIO nằm trong `core` nên luôn có). Tuỳ chọn: `make up PROFILE=analytics` để thử Trino.
- Silver Parquet ngày 15/02 phải có trên MinIO tại `s3a://lake/silver/pos_transactions/dt=2026-02-15`. Nếu chưa (hoặc đã `make reset`), chạy lại pipeline #07 trỏ vào MinIO:
  ```bash
  S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
    uv run coffee-ingest run --source data/samples --lake s3://lake --date 2026-02-15
  docker compose exec minio mc ls local/lake/silver/pos_transactions/dt=2026-02-15/
  ```

## Table format trong 5 phút
Một **table format** là cách ghi lại "bảng này gồm đúng những file nào, schema gì, partition gì, ở thời điểm nào" vào metadata nằm **cạnh** dữ liệu trên object storage. Engine (Spark, Trino, DuckDB, Flink…) đọc metadata thay vì `LIST` cả bucket. Mỗi lần ghi là một **snapshot** mới, commit nguyên tử bằng cách đổi con trỏ "snapshot hiện hành" — nên có ACID, time travel và rollback gần như miễn phí.

```
catalog (hadoop: version-hint.text | Hive/Glue/REST/Nessie)
 └─ v3.metadata.json      schema(s), partition spec(s), properties, snapshots[], current-snapshot-id
     └─ snap-<id>.avro    manifest list: danh sách manifest + thống kê theo partition
         └─ *-m0.avro     manifest: danh sách data file + min/max từng cột (để skip file)
             └─ data/ts_day=2026-02-15/*.parquet
```

| | **Apache Iceberg** | **Delta Lake** | **Apache Hudi** |
|--|--|--|--|
| Gốc | Netflix (2017) | Databricks (2019) | Uber (2016) |
| Metadata | `metadata.json` + manifest (Avro); snapshot bất biến | `_delta_log/0000N.json` + checkpoint Parquet mỗi 10 commit | timeline `.hoodie/` + metadata table; record key bắt buộc |
| Partition | **hidden partitioning** (`days(ts)`, `bucket(8, id)`), đổi spec không ghi lại dữ liệu | cột partition tường minh (như Hive); liquid clustering ở bản mới | cột partition tường minh + index theo record key |
| Catalog | bắt buộc có (hadoop/Hive/Glue/REST/Nessie) — điểm mạnh và điểm phiền | không bắt buộc (log là catalog); Unity Catalog nếu dùng Databricks | Hive sync tuỳ chọn |
| Engine đọc/ghi tốt | Spark, Trino, Flink, Snowflake, BigQuery, DuckDB, Athena | Spark/Databricks hàng đầu; Trino/DuckDB đọc được; ghi ngoài Spark hạn chế hơn | Spark, Flink; Trino/Presto đọc |
| Mạnh về | multi-engine, schema/partition evolution, đọc nhiều hơn ghi | tích hợp Databricks, đơn giản để bắt đầu | upsert/CDC tần suất cao (Merge-on-Read), streaming ingestion |
| VN thực tế hay gặp | on-prem MinIO/HDFS + Trino/Spark; AWS Athena/Glue; "lakehouse" do team tự dựng | **Databricks trên Azure** rất phổ biến ở ngân hàng, bán lẻ, viễn thông → mặc định Delta | hiếm; vài team Flink CDC |

Chọn thế nào? Có Databricks → Delta, không cần nghĩ. Tự dựng với MinIO/Trino/nhiều engine → Iceberg. Upsert liên tục từ CDC với độ trễ phút → cân nhắc Hudi hoặc Iceberg v2 (merge-on-read bằng delete file). Ba cái đang hội tụ (Delta UniForm, XTable), nên khác biệt lớn nhất năm 2026 là **catalog và hệ sinh thái**, không phải định dạng file.

## Lab: silver thành bảng Iceberg

### Bước 1 — Mở spark-sql
```bash
docker compose exec spark-master spark-sql --master spark://spark-master:7077
```
Catalog `lake` đã có trong `spark-defaults.conf` của image: **JDBC catalog** lưu trong Postgres (db `iceberg_catalog`), `warehouse=s3a://warehouse/`. Trino (`PROFILE=analytics`) trỏ vào đúng catalog đó, nên hai engine thấy cùng một bảng. `SHOW NAMESPACES IN lake;` → rỗng.

### Bước 2 — Điền và chạy `iceberg_lab.sql`
Mở [`lab/iceberg_lab.sql`](lab/iceberg_lab.sql): 5 TODO (CREATE TABLE, MERGE chạy 2 lần, `.snapshots`, time travel, `ADD COLUMN`). Dán từng khối vào shell. DDL mẫu ở [`platform/lake/iceberg/silver_tables.sql`](../../platform/lake/iceberg/silver_tables.sql); job Python làm cùng việc ở [`platform/spark/iceberg_silver.py`](../../platform/spark/iceberg_silver.py):
```bash
docker compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/jobs/iceberg_silver.py --date 2026-02-15 --lake s3a://lake
```
Chạy job đó 2 lần cũng cho cùng kết quả như MERGE tay 2 lần — đó là ý nghĩa của "pipeline idempotent" (#07) khi engine giúp bạn.

### Bước 3 — Nhìn metadata thật trên MinIO
```bash
docker compose exec minio mc ls local/warehouse/silver/pos_transactions/metadata/
docker compose exec minio mc cp -r local/warehouse/silver/pos_transactions/metadata /tmp/meta
docker compose cp minio:/tmp/meta ./tmp-meta
```
`00000-<uuid>.metadata.json`, `00001-<uuid>.metadata.json`…, `snap-*.avro`, `*-m0.avro`. Mỗi `MERGE`/`ALTER` sinh thêm một metadata.json với số thứ tự tăng 1; **vị trí file hiện hành nằm trong catalog** (bảng `iceberg_tables` trong Postgres — xem thử bằng `psql -d iceberg_catalog`). Catalog kiểu hadoop thì thay bằng `version-hint.text` + `vN.metadata.json` (fixture trong `lab/solution/fixtures/` dùng dạng này). Mở một `metadata.json` bằng mắt trước khi viết code.

### Bước 4 — `metadata_explorer.py`: in snapshots không cần Spark
Mở [`lab/metadata_explorer.py`](lab/metadata_explorer.py), điền 4 TODO (tìm file hiện hành theo `version-hint.text`, load, tóm tắt snapshots, in bảng). Thử ngay với fixture trong `lab/solution/fixtures/metadata/` (một `v2.metadata.json` tối giản nhưng đúng spec v2) rồi với `./tmp-meta` ở bước 3:
```bash
uv run python videos/17-lakehouse-iceberg-delta-hudi/lab/metadata_explorer.py videos/17-lakehouse-iceberg-delta-hudi/lab/solution/fixtures/metadata
uv run python videos/17-lakehouse-iceberg-delta-hudi/lab/metadata_explorer.py ./tmp-meta
```
Bạn vừa viết một "engine" đọc Iceberg ở mức metadata — đúng cách DuckDB/Trino bắt đầu.

### Bước 5 (tuỳ chọn) — Đọc cùng bảng từ Trino
```bash
make up PROFILE=analytics
docker compose exec trino trino --catalog lake --schema silver --execute "SELECT count(*) FROM pos_transactions"
```
Ý tưởng: cùng dữ liệu, cùng metadata, engine khác — không copy, không ETL. Điều làm được việc đó là **catalog** dùng chung (ở đây: JDBC catalog trong Postgres). Catalog kiểu hadoop (chỉ file `version-hint.text`) Spark hiểu nhưng Trino không; production thường dùng REST catalog (Polaris, Lakekeeper, Nessie) hoặc Hive Metastore/Glue.
<!-- TODO(thuy): kiểm chứng Trino 460 đọc được bảng do Spark tạo qua JDBC catalog (driver postgres trong plugin iceberg); nếu không, mount jar postgresql vào /usr/lib/trino/plugin/iceberg/ -->

### Bước 6 — Test offline
```bash
uv run pytest videos/17-lakehouse-iceberg-delta-hudi -q
```

## Kết quả mong đợi
- Bước 2: `incoming_rows = 792`; count sau MERGE lần 1 = **792**, sau lần 2 **vẫn 792**. `.snapshots` có 2 dòng: `append` (added 792) rồi `overwrite` (added 792, **deleted 792** — copy-on-write ghi lại file dù giá trị không đổi). `VERSION AS OF <snapshot đầu>` cũng trả 792 nhưng đọc file cũ. `ADD COLUMN`: `.files` không thay đổi, cột mới toàn NULL.
- Bước 3: thư mục metadata có `version-hint.text` = số lần commit, ít nhất `v1..v4.metadata.json` (create, merge, merge, alter).
- Bước 4 với fixture: 2 snapshot, dòng `*8821937160440015002  2026-02-15 06:01:00  overwrite  792  792  792` được đánh `*`. Test `test_v17_metadata.py`: `3 passed`.
<!-- TODO(thuy): dán output thật của .snapshots và metadata_explorer trên ./tmp-meta sau khi quay -->

## Câu hỏi phỏng vấn của video
> "Team bạn có data lake Parquet partition theo ngày, chạy ổn 2 năm. Sếp đọc bài về lakehouse và muốn chuyển sang Iceberg. Bạn nêu 3 vấn đề cụ thể mà Iceberg sẽ giải quyết **cho team này**, và 2 chi phí mới xuất hiện (gợi ý: catalog, compaction/expire snapshots)."

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Mở rộng `metadata_explorer.py`: in thêm schema hiện hành (tên cột, kiểu, `required`) và partition spec. Với `ADD COLUMN`/`RENAME COLUMN` ở khối 7, `schemas[]` có mấy phần tử, `current-schema-id` là gì?
2. Chạy `MERGE` lần 3 nhưng với `incoming` là ngày **16/02**. `.partitions` có thêm partition nào? Snapshot mới `operation` là gì và `changed-partition-count` bằng mấy?
3. Đọc `_delta_log` của một bảng Delta (tạo bằng `spark.range(10).write.format("delta")` trong Jupyter nếu bạn thêm jar, hoặc đọc tài liệu): so với `vN.metadata.json`, Delta ghi gì mỗi commit? Ưu/nhược của "log of actions" so với "snapshot bất biến"? Trả lời dưới comment / mở Discussion.
