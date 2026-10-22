# videos/

Một thư mục cho mỗi video, đánh số `NN-slug`. Trong mỗi thư mục:

```
NN-slug/
├── README.md        # mục tiêu, lab từng bước, câu hỏi phỏng vấn, bài tập, "Kết quả mong đợi"
├── lab/             # starter code có TODO để bạn tự làm
│   └── solution/    # lời giải — chỉ mở sau khi đã thử
└── slides/          # slide của video: PDF (đọc) + PPTX (sửa), CC BY-NC-SA 4.0
```

Lab chạy trên môi trường chung (`make up`, xem [SETUP.md](../SETUP.md)) và bộ dữ liệu mẫu ở [data/samples](../data/samples).
Ngày nghiệp vụ "chuẩn" dùng xuyên suốt các lab: **2026-02-15** (Chủ nhật, 2 ngày trước Tết, có đủ các loại lỗi dữ liệu).

| Phần | Video |
|------|-------|
| 1 — Nền tảng | [00](00-intro) · [01](01-data-engineer-la-gi) · [02](02-kien-truc-du-lieu) · [03](03-sql-cho-data-engineer) · [04](04-python-cho-data-engineer) · [05](05-docker-cho-data-engineer) · [06](06-data-modeling) |
| 2 — Batch & Lưu trữ | [07](07-etl-vs-elt) · [08](08-dinh-dang-file) · [09](09-data-lake-s3) · [10](10-apache-spark) · [11](11-toi-uu-spark) · [12](12-dbt) |
| 3 — Streaming & Lakehouse | [13](13-kafka) · [14](14-delivery-semantics-dlq-schema-registry) · [15](15-spark-structured-streaming) · [16](16-cdc-debezium) · [17](17-lakehouse-iceberg-delta-hudi) · [18](18-kien-truc-streaming-end-to-end) |
| 4 — Vận hành | [19](19-airflow) · [20](20-data-quality) · [21](21-observability-lineage) · [22](22-cicd-terraform) · [23](23-chi-phi-bao-mat) · [24](24-portfolio-phong-van) |
