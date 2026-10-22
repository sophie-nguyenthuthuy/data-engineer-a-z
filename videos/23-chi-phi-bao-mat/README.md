# #23 — Chi phí & bảo mật: FinOps, PII, access control

📺 Video: `TODO:youtube-link` · 📑 Slide: [V23-Chi-phi-bao-mat.pdf](slides/V23-Chi-phi-bao-mat.pdf)

## Bạn sẽ học được gì
- Tìm PII trong lake bằng một scanner 100 dòng (pattern số điện thoại VN, email, họ tên) và hiểu vì sao báo cáo PII **không được in PII**.
- Mask vs hash vs tokenize: khi nào dùng cái nào; vì sao `md5(phone)` không phải bảo mật (brute-force được trong vài phút).
- Ước lượng chi phí S3 + Athena cho 90 ngày dữ liệu bằng số đo thật từ samples; thấy Parquet và partition pruning tiết kiệm bao nhiêu — và khi nào con số đó mới đáng quan tâm.
- Least privilege ở 3 lớp: IAM policy (Terraform), MinIO policy, Postgres role chỉ-đọc **theo cột**.

## Yêu cầu trước khi xem
- Đã xem #08 (định dạng file), #09 (data lake), #12 (dbt — `stg_customers` đã mask phone), #22 (Terraform).
- Lab 1–3 chạy **offline** (`make install` là đủ). Lab 4 phần Postgres cần `make up` (profile core).

## Lab: rẻ hơn và an toàn hơn

### Bước 1 — Quét PII trong samples và trong lake
`customers.csv` có `full_name`, `phone`, `email` ([SCHEMA.md](../../data/generator/SCHEMA.md#customers-2-000-dòng) đánh dấu **PII**). Pipeline hiện tại: bronze/silver giữ nguyên PII (đúng — raw phải là raw), còn dbt [`stg_customers.sql`](../../platform/dbt/models/staging/stg_customers.sql) chỉ đưa `phone_masked` lên gold qua macro [`mask_pii`](../../platform/dbt/macros/mask_pii.sql).

Mở [`lab/pii_scan.py`](lab/pii_scan.py), điền TODO (regex phone VN `^0[3-9]\d{8}$`, regex email, heuristic họ tên, hàm `mask`, vòng lặp `scan_frame`) rồi chạy:
```bash
uv run python videos/23-chi-phi-bao-mat/lab/pii_scan.py data/samples
make smoke && uv run python videos/23-chi-phi-bao-mat/lab/pii_scan.py lake-local/silver
```
Exit code 1 khi có PII — để dùng làm CI gate cho thư mục `gold/` (nơi **không được** có PII thô).

### Bước 2 — Hash hay tokenize?
Mở [`lab/tokenize_pii.py`](lab/tokenize_pii.py). Ba cách giấu định danh, từ yếu đến đúng:

| Cách | Ví dụ | Vấn đề |
|------|-------|--------|
| Mask | `*******580` | giữ được đối soát, mất khả năng join |
| `md5(value)` không salt | `3388…c58` | không gian số điện thoại VN ~7×10⁸ → brute-force hết trong vài phút; cùng hash ở hai hệ thống → join chéo được, rò rỉ qua liên kết |
| **HMAC-SHA256(salt, value)** | `56409615d2186783` | ổn định để join trong nội bộ, không đảo ngược nếu salt bí mật (giữ ở Secrets Manager, **không** trong code hay dữ liệu) |

Điền TODO ở `tokenize` và `brute_force_md5`, rồi:
```bash
uv run python videos/23-chi-phi-bao-mat/lab/tokenize_pii.py --value 0959710580 --salt "doi-thanh-bi-mat"
```
Test sẽ brute-force thật một md5 (thu nhỏ còn 10⁴ ứng viên) để bạn thấy nó vô dụng.

### Bước 3 — FinOps: chi phí 90 ngày dữ liệu
Mở [`lab/cost_model.py`](lab/cost_model.py). Phần đo đã viết sẵn: đọc 4 nguồn trong `data/samples`, tính **bytes/dòng CSV vs Parquet (zstd)** và **dòng/ngày** (ngày thường, bỏ Tết), quy từ scale 0.08 về 1.0. Bạn điền 3 TODO: công thức S3, công thức Athena (nhớ sàn 10 MB/query), và tổng trong `estimate()`.
```bash
uv run python videos/23-chi-phi-bao-mat/lab/cost_model.py
uv run python videos/23-chi-phi-bao-mat/lab/cost_model.py --multiplier 100   # nếu Phin Phố có 3 000 cửa hàng
```
Giá dùng trong model (kiểm lại qua AWS Price List API cho `ap-southeast-1`, 22/10/2026): S3 Standard **0,025 USD/GB-tháng** cho 50 TB đầu (0,024 cho 450 TB tiếp, 0,023 trên 500 TB), PUT/COPY/POST/LIST **0,005 USD/1 000 request**, GET **0,004 USD/10 000 request**; Athena **5 USD/TB quét**, sàn 10 MB/query. Giá đổi theo thời gian — tự kiểm lại bằng:
```bash
curl -s https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonAthena/current/ap-southeast-1/index.json | python3 -c "import json,sys; d=json.load(sys.stdin); print([pd['description'] for t in d['terms']['OnDemand'].values() for v in t.values() for pd in v['priceDimensions'].values() if 'Scanned' in pd['description']])"
```

### Bước 4 — Access control ở 3 lớp
1. **IAM (AWS)**: đọc policy trong [`infra/terraform/main.tf`](../../infra/terraform/main.tf): `s3:ListBucket` trên bucket, `s3:Get/Put/DeleteObject` trên `bucket/*`, không có `s3:*`, không có `"Resource": "*"` cho S3. Glue dùng `*` — trade-off có comment.
2. **MinIO**: cùng ngôn ngữ policy với IAM. Tạo user chỉ-đọc `gold/`:
   ```bash
   docker compose exec minio mc admin user add local analyst analyst123
   docker compose exec -T minio mc admin policy create local gold-readonly /dev/stdin <<'EOF'
   {"Version":"2012-10-17","Statement":[
     {"Effect":"Allow","Action":["s3:GetBucketLocation"],"Resource":["arn:aws:s3:::lake"]},
     {"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::lake"],
      "Condition":{"StringLike":{"s3:prefix":["gold/*"]}}},
     {"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::lake/gold/*"]}]}
   EOF
   docker compose exec minio mc admin policy attach local gold-readonly --user analyst
   ```
   Để ý `s3:GetBucketLocation` phải nằm **statement riêng**: MinIO (mc 2024-10) từ chối policy nếu gắn condition `s3:prefix` vào action đó — `unsupported condition keys '[s3:prefix]' used for action 's3:GetBucketLocation'`. Kiểm chứng bằng chính `mc` trong container:
   ```bash
   docker compose exec minio sh -c 'mc alias set analyst http://localhost:9000 analyst analyst123 && mc ls analyst/lake/gold/ && mc ls analyst/lake/silver/'
   # gold/ liệt kê được (.keep), silver/ → "Unable to list folder. Access Denied."
   ```
3. **Postgres**: analyst cần đọc POS nhưng không được thấy `phone`/`email`. Điền TODO trong [`lab/analyst_role.sql`](lab/analyst_role.sql) — `GRANT SELECT` 4 bảng, **column-level GRANT** cho `customers`, view `customers_masked` — rồi:
   ```bash
   docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/23-chi-phi-bao-mat/lab/analyst_role.sql
   ```

### Bước 5 — Test offline
```bash
uv run pytest videos/23-chi-phi-bao-mat -q
```

## Kết quả mong đợi
- Bước 1, `data/samples`: đúng **3 cột** PII, tất cả ở `customers.csv`:
  ```
  file                          column     kind          match  sample(masked)
  data/samples/customers.csv    full_name  person_name   100%   **********Lan
  data/samples/customers.csv    phone      phone_vn      100%   *******580
  data/samples/customers.csv    email      email         100%   **************com
  ```
  (2 000/2 000 phone khớp `^0[3-9]\d{8}$`, 2 000/2 000 email; `stores.csv` có "Phin Phố Hoàn Kiếm" nhưng **không** bị coi là tên người.) `lake-local/silver`: thêm cùng 3 cột trong `silver/customers/dt=.../part-000.parquet`. Exit code 1.
- Bước 2: `md5 plain : 3388572334d7edfc69f32d5b9af22c58`, `token : 56409615d2186783`. Đổi salt → token khác hoàn toàn.
- Bước 3 (90 ngày, 30 cửa hàng):

  | source | rows | raw MB | parquet MB | ratio |
  |--------|-----:|-------:|-----------:|------:|
  | pos_transactions | 862 500 | 74.2 | 12.0 | 6.2x |
  | pos_line_items | 1 504 595 | 90.6 | 13.9 | 6.5x |
  | app_events | 2 243 250 | 691.0 | 89.3 | 7.7x |
  | iot_readings | 5 208 960 | 646.0 | 67.6 | 9.6x |

  Tổng raw **1.47 GB** vs Parquet **0.18 GB** (giảm 88%). S3/tháng: 0.037 vs 0.0045 USD. Athena/tháng với 900 query: full scan CSV **6.4 USD**, Parquet **0.79 USD**, Parquet + partition pruning **0.043 USD** (chạm sàn 10 MB/query).
  Với `--multiplier 100`: raw 115 GB, Athena CSV **~508 USD/tháng** vs pruning **0.68 USD**. Bài học: ở 30 cửa hàng, tiền không nằm ở S3/Athena mà ở giờ kỹ sư và cluster chạy rỗi; nhưng cấu trúc đúng (Parquet + partition) từ ngày đầu là thứ giúp bạn không phải viết lại khi ×100.
- Bước 4.3: trong output psql, `SELECT count(*) FROM pos_transactions` trả số; `SELECT customer_id, city, tier FROM customers` trả 2 dòng; `SELECT phone FROM customers` → `ERROR:  permission denied for table customers`.
- Bước 5: `12 passed`.

## Câu hỏi phỏng vấn của video
> "Marketing xin export danh sách khách gold kèm số điện thoại để chạy SMS Tết. Bạn đưa cho họ cái gì, qua đâu, và log gì? Nếu họ nói 'chỉ cần hash là đủ ẩn danh', bạn trả lời sao?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Thêm detector `customer_id` (`^C\d{6}$`) vào `pii_scan.py`. Nó sẽ báo ở `pos_transactions` — đó là PII hay chỉ là pseudonymous identifier? Bạn muốn scanner báo hay bỏ qua? Lý do.
2. Thêm vào `cost_model.py` tham số `bronze_retention_days` (Terraform đặt lifecycle 90 ngày cho `bronze/`) và tính S3 tiết kiệm bao nhiêu khi silver/gold giữ 2 năm nhưng bronze chỉ 90 ngày. Bronze có cần giữ lâu hơn không nếu bạn muốn re-process silver?
3. Viết **Row-Level Security** cho Postgres: quản lý vùng Hà Nội chỉ thấy `pos_transactions` của cửa hàng `HN%`. Dán `CREATE POLICY` dưới comment / mở Discussion.
