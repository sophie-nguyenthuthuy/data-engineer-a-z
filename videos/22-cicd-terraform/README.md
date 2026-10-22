# #22 — CI/CD + Terraform

📺 Video: `TODO:youtube-link`

## Bạn sẽ học được gì
- Đọc hiểu CI của chính repo này ([`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)): 5 job chạy song song trên mỗi PR, mỗi job tương ứng một target trong `Makefile`.
- Viết một GitHub Actions job từ đầu (trigger, `paths` filter, steps, action của bên thứ ba có pin version).
- Quy trình **trunk-based**: branch ngắn → PR → checks xanh → merge vào `main`; environments, secrets, vì sao không bao giờ commit `.tfvars`.
- Terraform cơ bản: `init / plan / apply / destroy`, state backend, và đọc một IAM policy least-privilege.
- `pre-commit` để bắt lỗi trước khi CI phải bắt.

## Yêu cầu trước khi xem
- Đã xem #05 (Docker) và #19 (Airflow). Không cần Docker cho lab này.
- `make install` xong. Bước 5 (Terraform) **tuỳ chọn** và cần tài khoản AWS — có phát sinh chi phí nhỏ.
- Cài `terraform` nếu làm bước 4–5: https://developer.hashicorp.com/terraform/install (≥ 1.6).

## Lab: để máy kiểm tra thay bạn

### Bước 1 — Đọc CI hiện có
<!-- bảng này mô tả ci.yml hiện tại; cập nhật khi thêm/bớt job -->
Mở [`ci.yml`](../../.github/workflows/ci.yml). Mỗi job = một lệnh bạn đã chạy tay trong khoá:

| Job | Lệnh | Bắt lỗi gì | Video |
|-----|------|-----------|-------|
| `lint` | `uv run ruff check . && ruff format --check .` | style, import thừa, bug nông (B rules) | #04 |
| `test` | `uv run pytest -q` | generator, ingestion, **mọi `lab/solution/test_*.py`** | #07+ |
| `dbt-parse` | `make dbt-parse` (target `ci`, DuckDB in-memory) | Jinja/YAML sai, ref() tới model không tồn tại — không cần dữ liệu | #12 |
| `smoke` | `make generate` + `git diff --exit-code data/samples` + `make smoke` + contract check + `dbt build` | samples lệch generator, pipeline hỏng, contract vi phạm, gold không build được | #07, #20 |
| `compose-and-terraform` | `docker compose config -q`, `terraform fmt -check`, `init -backend=false`, `validate` | compose YAML sai, HCL chưa format/không hợp lệ | #05, #22 |

Để ý: `on: push` chỉ cho `main`, còn `pull_request` cho mọi branch; `concurrency` huỷ run cũ khi bạn push tiếp; `UV_LOCKED=1` bắt `uv.lock` phải khớp `pyproject` (reproducible).

Chạy đúng những gì CI chạy, trên máy bạn:
```bash
make lint && make test && make dbt-parse && make smoke
```

### Bước 2 — pre-commit: bắt lỗi trước khi push
```bash
uv run pre-commit install
uv run pre-commit run --all-files
```
Hook nằm ở [`.pre-commit-config.yaml`](../../.pre-commit-config.yaml): trailing whitespace, YAML hợp lệ, file > 1 MB, ruff. Từ giờ mỗi `git commit` sẽ chạy chúng. Thử: thêm một dòng `import os` thừa vào file Python bất kỳ, `git add`, `git commit` → bị chặn, ruff tự sửa, bạn `git add` lại.

### Bước 3 — Viết job CI mới
CI đã có job Terraform, nhưng bạn sẽ **viết lại từ đầu** trong [`lab/ci-extra.yml`](lab/ci-extra.yml) để hiểu từng dòng, và thêm một thứ CI hiện chưa có: chỉ chạy khi thư mục `infra/terraform/**` thay đổi (`paths` filter — tiết kiệm phút CI). Điền TODO:
1. `on:` — `pull_request` và `push` vào `main`, cả hai có `paths: ["infra/terraform/**"]`.
2. Job `terraform-check`: checkout → `hashicorp/setup-terraform@v3` (pin `terraform_version`) → `fmt -check -recursive` → `init -backend=false -input=false` → `validate`.

Vì sao `-backend=false`? `init` bình thường sẽ kết nối state backend (S3/Terraform Cloud) — CI check cú pháp không cần và **không được** có credential đó. `validate` chỉ cần provider schema, không cần AWS.

Kiểm tra YAML của bạn:
```bash
uv run pytest videos/22-cicd-terraform -q
```
Muốn chạy thật trên GitHub: copy file vào `.github/workflows/ci-extra.yml` trên fork của bạn, mở PR sửa một dòng trong `infra/terraform/README.md` → job chạy; PR sửa file khác → job **không** xuất hiện.

### Bước 4 — Terraform offline: fmt + validate
Không cần AWS:
```bash
cd infra/terraform
terraform fmt -check -recursive -diff
terraform init -backend=false -input=false
terraform validate
```
Đọc [`main.tf`](../../infra/terraform/main.tf): bucket + versioning + block public access + SSE + lifecycle xoá `bronze/` sau 90 ngày; Glue database; IAM user với policy **least privilege** — chỉ `s3:GetObject/PutObject/DeleteObject` trên đúng một bucket, không có `s3:*`. `glue:*` dùng `resources = ["*"]` vì Glue catalog ARN phức tạp — đó là một **trade-off có ghi chú**, không phải lười.

### Bước 5 — Terraform thật trên AWS (tuỳ chọn, có chi phí)
> ⚠️ S3 vài MB gần như miễn phí, nhưng **quên `destroy`** thì bucket versioning + lifecycle vẫn tồn tại mãi. Đặt lịch nhắc.
```bash
cd infra/terraform
cp example.tfvars terraform.tfvars        # sửa bucket_name — duy nhất toàn cầu
aws configure                              # hoặc export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
terraform init
terraform plan                             # đọc kỹ: 12 resource "to add"
terraform apply
terraform output                           # pipeline_access_key_id; secret phải -raw
terraform destroy                          # ĐỪNG QUÊN
```
`terraform.tfvars` đã nằm trong `.gitignore` (chỉ `example.tfvars` được commit). `*.tfstate` cũng vậy — state chứa secret access key dạng plaintext.

### Lý thuyết đi kèm trong video
- **Trunk-based**: một branch `main` luôn deploy được; feature branch sống < 2 ngày; PR nhỏ; merge khi checks xanh + 1 review. Không có branch `develop`/`release` dài hạn.
- **PR checks** = cổng chất lượng tự động; review người = cổng thiết kế. Hai cổng khác nhau, đừng để người làm việc của máy.
- **Environments** (dev/staging/prod) trong GitHub Actions: secrets khác nhau theo env, `prod` cần approver. Với data: dev đọc `lake-local`, prod đọc `s3://lake` — cùng code, khác biến (`--vars '{lake_root: ...}'` của dbt là ví dụ trong repo).
- **Secrets**: GitHub Secrets / AWS SSM, inject bằng biến môi trường lúc chạy; không bao giờ trong code, `.tfvars`, log. Tốt hơn nữa: OIDC để Actions assume role, không cần access key dài hạn.
- **State backend**: `terraform.tfstate` local chỉ dùng cho lab một người. Team dùng backend S3 + DynamoDB lock (hoặc Terraform Cloud) để không hai người `apply` cùng lúc.

## Kết quả mong đợi
- Bước 1 (số thật ở commit hiện tại): `make lint` → `All checks passed!` + `160 files already formatted`; `make test` → **101 passed** (test DagBag của Airflow không nằm trong `testpaths`, chạy riêng trong container ở #19); `make dbt-parse` với dbt 1.9 chỉ in `Performance info: …/target/perf_info.json` (không lỗi = parse OK — dòng `Found 11 models, 1 snapshot, 1 analysis, 33 data tests, 5 sources` chỉ xuất hiện ở `dbt build`); `make smoke` → JSON summary với `"rows_out": 792`.
- Bước 2: `pre-commit run --all-files` in từng hook `Passed`. Lần commit có `import os` thừa: `ruff....Failed` kèm `Fixed 1 error`, commit bị chặn.
- Bước 3: `3 passed`. Lưu ý cú pháp YAML: PyYAML đọc key `on` thành boolean `True` (chuẩn YAML 1.1) — test đã xử lý cả hai trường hợp; đây là lý do nhiều người viết `"on":` có ngoặc kép.
- Bước 4: `terraform fmt -check` không in gì (exit 0); `validate` → `Success! The configuration is valid.`
- Bước 5: `plan` → `Plan: 12 to add, 0 to change, 0 to destroy.` (bucket, versioning, public-access-block, SSE, lifecycle, 3 object `bronze/ silver/ gold/` từ một `for_each`, Glue database, IAM user + policy + access key — 10 khối `resource` trong `main.tf`, 12 resource sau khi `for_each` nở ra); `destroy` → `Destroy complete! Resources: 12 destroyed.` (cần tài khoản AWS; `fmt`/`validate` thì CI chạy hộ mỗi commit.)

## Câu hỏi phỏng vấn của video
> "PR của bạn đổi một model dbt. CI nên chạy gì để **đủ tin** merge mà không cần cluster/warehouse thật? `dbt parse` bắt được lỗi gì và bỏ sót lỗi gì? Nếu có 15 phút CI, bạn thêm bước nào tiếp theo?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Thêm vào `ci-extra.yml` một job `dbt-build-smoke` phụ thuộc (`needs`) vào job terraform: `make smoke` rồi `dbt build --target dev` — và giải thích vì sao job `smoke` thật trong `ci.yml` **không** dùng `needs` mà chạy song song.
2. Viết `infra/terraform/backend.tf.example` cấu hình backend S3 + DynamoDB lock cho state (không apply). Vì sao file này không thể dùng `var.` bên trong?
3. Đổi job Terraform sang dùng **OIDC** (`aws-actions/configure-aws-credentials` với `role-to-assume`) thay cho access key trong Secrets — chỉ cần viết YAML và mô tả trust policy của role. Trả lời dưới comment / mở Discussion.
