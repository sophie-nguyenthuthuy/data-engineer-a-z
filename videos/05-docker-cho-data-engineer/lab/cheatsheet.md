# Docker cheatsheet cho Data Engineer (những lệnh mình gõ mỗi ngày)

Tất cả chạy từ root repo. `make up/down/ps/logs` chỉ là lớp mỏng trên `docker compose --env-file .env ...` — xem [Makefile](../../../Makefile).

## Vòng đời
| Việc | Lệnh |
|------|------|
| Bật môi trường (core + notebook) | `make up` |
| Bật thêm profile | `make up PROFILE=streaming` (= `--profile core --profile streaming`) |
| Xem trạng thái + healthcheck | `make ps` → cột STATUS: `healthy` / `starting` / `unhealthy` / `exited (0)` |
| Dừng, giữ dữ liệu | `make down` |
| Dừng, XOÁ volume (bắt đầu lại) | `make reset` |
| Khởi động lại 1 service | `docker compose restart postgres` |
| Build lại image sau khi sửa Dockerfile | `docker compose build jupyter` rồi `make up` |
| Kéo image mới (khi đổi tag) | `docker compose pull` |

## Nhìn vào bên trong
| Việc | Lệnh |
|------|------|
| Log 1 service, theo dõi live | `docker compose logs -f --tail=100 postgres` |
| Log tất cả | `make logs` |
| Vào shell trong container | `docker compose exec postgres bash` (Alpine image: `sh`) |
| Chạy 1 lệnh rồi thoát | `docker compose exec postgres psql -U coffee -d coffee_pos -c "SELECT count(*) FROM pos_transactions;"` |
| Đưa file qua stdin (không cần copy) | `docker compose exec -T postgres psql -U coffee -d coffee_pos < path/to/file.sql` (`-T` = không mở TTY) |
| Copy file vào/ra container | `docker compose cp data/samples/stores.csv minio:/tmp/` · `docker compose cp postgres:/tmp/out.csv ./` |
| RAM/CPU từng container | `docker stats --no-stream` |
| Chi tiết 1 container (IP, mount, env) | `docker inspect dea2z-postgres-1 \| less` |
| Lý do healthcheck fail | `docker inspect --format '{{json .State.Health}}' dea2z-postgres-1 \| jq` |
| Xem compose đã resolve biến/anchor | `docker compose --env-file .env --profile core config` |

## Network & volume
| Việc | Lệnh |
|------|------|
| Liệt kê network, volume của project | `docker network ls \| grep dea2z` · `docker volume ls \| grep dea2z` |
| Container A gọi container B | dùng **tên service** làm hostname: `postgres:5432`, `minio:9000`, `kafka:29092` — không phải `localhost` |
| Từ máy host gọi vào | `localhost:<port đã map>`: `localhost:5432`, `localhost:9000`, `localhost:9092` |
| Xem dữ liệu volume nằm đâu | `docker volume inspect dea2z_pgdata` (trên Mac/Windows nằm trong VM của Docker Desktop, không mở trực tiếp được) |
| Xoá 1 volume (mất dữ liệu!) | `docker compose down && docker volume rm dea2z_pgdata` |

## MinIO qua `mc` (có sẵn trong container minio)
```bash
docker compose exec minio mc ls local/lake/                       # bucket lake: bronze/ silver/ gold/
docker compose exec minio mc ls --recursive local/lake/bronze/
docker compose cp data/samples/stores.csv minio:/tmp/stores.csv   # bước 1: đưa file vào container
docker compose exec minio mc cp /tmp/stores.csv local/lake/bronze/stores/stores.csv   # bước 2: đẩy lên bucket
docker compose exec -T minio mc pipe local/lake/bronze/stores/stores.csv < data/samples/stores.csv   # hoặc 1 bước qua stdin
docker compose exec minio mc rm local/lake/bronze/stores/stores.csv
docker compose exec minio mc du local/lake
```
`local` là alias trỏ tới `http://localhost:9000` bên trong container minio. Từ Python/Spark ta dùng S3 API (`boto3`, `s3a://`) với endpoint `http://minio:9000` — video #09.

## Dọn dẹp máy
```bash
docker system df                 # đang tốn bao nhiêu
docker image prune               # xoá image dangling
docker builder prune             # xoá build cache (hay tốn vài GB)
docker system prune -a --volumes # XOÁ HẾT thứ không chạy — nghĩ kỹ trước khi gõ
```

## Lỗi hay gặp
| Triệu chứng | Lệnh chẩn đoán | Thường là |
|-------------|----------------|-----------|
| `port is already allocated` | `lsof -i :5432` (Mac/Linux) | Postgres/MinIO khác đang chạy trên máy → đổi port trong `.env` |
| service `unhealthy` | `docker compose logs <svc> --tail=50` + `docker inspect ... .State.Health` | thiếu RAM, hoặc dependency chưa lên |
| `exited (1)` ngay sau up | `docker compose logs <svc>` | sai env, sai mount path, file init SQL lỗi |
| Bảng Postgres rỗng | `docker volume ls` | volume cũ → init SQL không chạy lại → `make seed` |
| `no space left on device` | `docker system df` | build cache / image cũ → `docker builder prune` |
| Jupyter thấy repo rỗng | `docker inspect dea2z-jupyter-1 \| grep -A3 Mounts` | bind mount sai đường dẫn (Windows: repo phải nằm trong WSL) |
