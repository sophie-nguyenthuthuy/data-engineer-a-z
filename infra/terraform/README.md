# infra/terraform — TUỲ CHỌN (video #22)

> ⚠️ **Phần này không bắt buộc.** Toàn bộ khoá học chạy được với MinIO local. Chỉ làm phần này nếu bạn muốn thử Data Lake trên AWS thật. Có **phát sinh chi phí** (vài USD nếu bạn quên dọn).

Tạo gì:
- 1 bucket S3 làm data lake (`bronze/`, `silver/`, `gold/`), versioning + block public access + lifecycle xoá bronze sau 90 ngày.
- 1 Glue Data Catalog database `coffee_lake`.
- 1 IAM user `dea2z-pipeline` với policy chỉ đọc/ghi bucket đó (least privilege).

```bash
cd infra/terraform
cp example.tfvars terraform.tfvars   # sửa bucket_name cho duy nhất toàn cầu
terraform init
terraform plan
terraform apply
# ... làm lab ...
terraform destroy                    # ĐỪNG QUÊN
```

Terraform ≥ 1.6, AWS provider 5.x. Credentials lấy từ `aws configure` hoặc biến môi trường — không bao giờ commit vào repo.
