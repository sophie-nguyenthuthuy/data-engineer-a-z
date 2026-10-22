# Cài đặt môi trường

Toàn bộ khoá học chạy trên **một máy laptop** bằng Docker. Phần 1–2 không cần tài khoản cloud nào. AWS chỉ là tuỳ chọn ở video #09 và #22.

## 1. Yêu cầu máy

| Profile | Dùng cho | RAM container đo thật (rảnh) | RAM máy nên có | Disk |
|---------|----------|------------------------------|----------------|------|
| `core` (mặc định: postgres + minio + jupyter) | Video #01–#12 | ~0,3 GB (Spark `local[*]` trong notebook: +1–2 GB) | **8 GB** (Docker cấp ~4 GB) | 5 GB |
| `streaming` (+ kafka, schema-registry, kafka-ui, debezium) | Video #13–#18 | +1,1 GB | 8 GB nếu chỉ bật profile này, 12 GB thoải mái | 8 GB |
| `spark` (+ spark master/worker) | Video #10, #11, #15, #17 | +0,5 GB rảnh, +2 GB khi chạy job | 12 GB (Docker ~6 GB) | 8 GB |
| `all` (mọi service, gồm airflow, marquez, trino) | Video #19–#23 | ≈ 4,5 GB rảnh, 6–7 GB khi Spark + Trino cùng chạy | 16 GB (Docker ≥ 8 GB) | 15 GB |

Số đo: `docker stats` trên Docker Desktop macOS giới hạn 7,75 GiB — `PROFILE=all` vẫn chạy trọn 24 lab trên giới hạn đó, nhưng chật.

CPU: 4 core trở lên. Mac M1/M2/M3 và Windows/Linux x86_64 đều chạy được (image đã chọn đều có bản arm64).

> Máy 8 GB RAM: bạn vẫn học hết phần 1–2 bình thường. Phần 3–4 hãy bật từng profile một (`make down` trước khi đổi profile), hoặc chạy lab Spark bằng `local[*]` trong Jupyter thay cho cluster.

## 2. Cài Docker

### macOS
1. Tải [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop/) (chọn đúng chip Apple/Intel).
2. Mở Docker Desktop → **Settings → Resources**: Memory ≥ 6 GB (8 GB nếu học phần 3–4), Disk ≥ 40 GB.
3. Kiểm tra trong Terminal:
   ```bash
   docker --version && docker compose version
   ```

### Windows 10/11
1. Bật WSL 2: mở PowerShell (Admin) → `wsl --install` → khởi động lại.
2. Tải Docker Desktop, trong lúc cài tick **Use WSL 2 based engine**.
3. Mở **Ubuntu (WSL)** và làm việc hoàn toàn trong WSL — clone repo vào `~/` của WSL, **không** vào `/mnt/c/...` (chậm 10–20 lần).
4. Settings → Resources → WSL integration: bật cho distro Ubuntu.
5. Trong WSL: `sudo apt install -y make git` rồi kiểm tra `docker compose version`.

### Linux (Ubuntu/Debian)
```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER   # đăng xuất/đăng nhập lại
sudo apt install -y make git
docker compose version
```

## 3. Cài công cụ Python (để chạy generator, test, dbt ngoài Docker)

Mình dùng [uv](https://docs.astral.sh/uv/) vì nó cài Python 3.11 và dependencies trong một lệnh:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh   # Windows: xem docs uv, hoặc dùng trong WSL
```

## 4. Khởi động

```bash
git clone https://github.com/sophie-nguyenthuthuy/data-engineer-a-z.git
cd data-engineer-a-z
make install          # tạo .venv, cài coffeegen + coffee-ingestion + dev tools
make up               # postgres + minio + jupyter (profile core)
make ps               # tất cả phải là "healthy" hoặc "running"
```

Lần đầu `make up` build image Jupyter (~5 phút, tải jar Spark/Iceberg + pip). `PROFILE=spark` build thêm image Spark (~2 phút), `PROFILE=all` build thêm Airflow (~5 phút). Các lần sau chỉ vài giây.

### Các profile khác
```bash
make up PROFILE=streaming   # thêm Kafka stack (video #13)
make up PROFILE=spark       # thêm Spark cluster (video #10)
make up PROFILE=analytics   # thêm Trino (tuỳ chọn, video #17)
make up PROFILE=all         # tất cả (video #19+)
make down                   # dừng, giữ dữ liệu
make reset                  # dừng và XOÁ volume — bắt đầu lại từ đầu
```

## 5. Địa chỉ các service

| Service | URL | Đăng nhập |
|---------|-----|-----------|
| Jupyter | http://localhost:8888 | token `dea2z` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9000 | (dùng từ code) |
| Postgres | `localhost:5432` db `coffee_pos` | `coffee` / `coffee` |
| Spark Master UI | http://localhost:8085 | — |
| Kafka (từ máy host) | `localhost:9092` | — |
| Kafka UI | http://localhost:8080 | — |
| Schema Registry | http://localhost:8081 | — |
| Debezium Connect | http://localhost:8083 | — |
| Airflow | http://localhost:8082 | `admin` / `admin` |
| Marquez UI | http://localhost:3000 | — |
| Marquez API | http://localhost:5050 | (port 5000 trên macOS đã bị AirPlay chiếm) |
| Trino | http://localhost:8090 | user bất kỳ |

Bên trong Docker network, service gọi nhau bằng tên: `postgres:5432`, `minio:9000`, `kafka:29092`, `spark-master:7077`.

## 6. Kiểm tra nhanh

```bash
# Postgres có dữ liệu POS chưa?
docker compose exec postgres psql -U coffee -d coffee_pos -c "SELECT count(*) FROM pos_transactions;"

# MinIO có bucket lake chưa?
docker compose exec minio mc ls local/lake/
```

Kết quả mong đợi: số dòng > 0 và ba thư mục `bronze/ silver/ gold/`.

## 7. Troubleshooting

| Triệu chứng | Nguyên nhân thường gặp | Cách xử lý |
|-------------|------------------------|------------|
| `port is already allocated` / `address already in use` | Máy bạn đang chạy Postgres/MinIO/Jupyter khác; trên macOS port **5000** và **7000** thuộc AirPlay Receiver; **8080/8081** hay bị tool khác chiếm | Đổi port trong `.env` (ví dụ `POSTGRES_PORT=5433`, `SCHEMA_REGISTRY_PORT=8091`) rồi `make up` lại. Tìm thủ phạm: `lsof -nP -iTCP:8081 -sTCP:LISTEN` |
| Service `unhealthy` mãi, nhất là Kafka/Airflow | Docker thiếu RAM | Tăng Memory trong Docker Desktop; chạy ít profile hơn |
| `make up` báo `.env: No such file` | Chưa copy env | `make env` (hoặc `cp .env.example .env`) |
| Postgres lên nhưng bảng rỗng | Volume cũ từ lần chạy trước, init SQL chỉ chạy khi volume mới | `make reset && make up`, hoặc `make seed` |
| Jupyter không mở, hỏi token | — | Dùng token `dea2z`, hoặc `docker compose logs jupyter` để lấy URL |
| Windows: build image rất chậm, lỗi `permission denied` | Repo nằm ở `/mnt/c` | Clone lại vào `~/` trong WSL |
| Mac M-series: `no matching manifest for linux/arm64` | Image không có bản arm | Mọi image trong repo đều có arm64; nếu gặp, mở issue kèm tên image |
| `pull access denied for bitnami/spark` hoặc `minio/minio` | Bitnami và MinIO đã gỡ image miễn phí khỏi Docker Hub (2025) | Repo đã đổi sang `apache/spark:3.5.3` (chính thức) và `bitnamilegacy/minio` (bản lưu trữ, có sẵn `mc`). `git pull` bản mới nhất |
| Airflow task `ingest` lỗi `No module named coffee_ingestion` hoặc `pyarrow requires NumPy 2.0` | Image Airflow cũ | `docker compose build airflow-init && make up PROFILE=orchestration` |
| Spark streaming không bao giờ ra `Batch: 0`, log lặp `UnknownTopicOrPartitionException` | Topic Kafka chưa tồn tại khi job start | Tạo topic trước (`kafka-topics.sh --create`), xem #15 |
| Spark job lỗi `ClassNotFoundException: S3AFileSystem` | Chạy sai image (thiếu jar) | Dùng `dea2z/spark` hoặc Jupyter của repo, đừng dùng image gốc |
| Debezium không bắt được thay đổi | Postgres chưa `wal_level=logical` | Repo đã cấu hình sẵn; nếu bạn dùng Postgres ngoài, bật `wal_level=logical` |
| Kafka client từ host không kết nối | Dùng sai port | Host dùng `localhost:9092`; container dùng `kafka:29092` |

Vẫn kẹt? Mở issue theo mẫu **Lab question**, nhớ dán output của `make ps` và `docker compose logs <service> --tail=50`.

## 8. Gỡ hoàn toàn

```bash
make reset
docker image prune -a   # xoá image đã build (tuỳ chọn)
rm -rf .venv
```
