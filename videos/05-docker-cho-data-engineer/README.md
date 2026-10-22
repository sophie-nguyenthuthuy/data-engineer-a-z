# #05 — Docker cho Data Engineer

📺 Video: `TODO:youtube-link` · 📑 Slide: [V05-Docker-cho-Data-Engineer.pdf](slides/V05-Docker-cho-Data-Engineer.pdf)

## Bạn sẽ học được gì
- Đọc hiểu [`docker-compose.yml`](../../docker-compose.yml) của repo — file bạn sẽ sống cùng 19 video còn lại: `profiles`, `healthcheck` + `depends_on: condition: service_healthy`, named volume vs bind mount, network và hostname, biến `${VAR:-default}`, YAML anchor `&hc` / `<<: *hc`.
- Vận hành hằng ngày: `make up/ps/down`, đọc log, `exec` vào container, copy file, xem RAM từng service.
- Dùng MinIO như S3: console web và `mc` trong container, upload file CSV đầu tiên lên `lake/bronze/`.
- Thêm một service mới bằng **file override** mà không sửa compose của repo — cách làm sạch khi bạn muốn thử công cụ ở công ty.
- Ước lượng RAM theo profile để biết máy mình chạy được tới video nào.

## Yêu cầu trước khi xem
- Đã xem [#04](../04-python-cho-data-engineer). Docker Desktop (hoặc Docker Engine) đã cài theo [SETUP.md](../../SETUP.md).
- `make up` (profile `core` + `notebook`). Lần đầu build image Jupyter ~5 phút.

## Lab: đọc compose, vận hành, thêm service

### Bước 1 — Đọc compose bằng câu hỏi
Mở [`docker-compose.yml`](../../docker-compose.yml) và trả lời (ghi ra giấy, 1 dòng mỗi câu):
1. `name: dea2z` ảnh hưởng tới tên gì? (gợi ý: `docker network ls`, `docker volume ls`)
2. `x-healthcheck-defaults: &hc` là gì, và `<<: *hc` trong `postgres.healthcheck` làm gì?
3. `minio-init` không có `ports`, không có `healthcheck`, chạy xong thì thoát — nó là kiểu container gì? Vì sao `$$l` có 2 dấu `$`?
4. `postgres` mount 3 thứ: `pgdata:` (named volume), `./data/samples/sql:/docker-entrypoint-initdb.d:ro` và `./data/samples:/opt/samples:ro` (bind mount). Cái nào mất khi `make reset`? Cái nào bạn sửa trên máy là container thấy ngay?
5. `jupyter` có `depends_on: postgres: { condition: service_healthy }`. Nếu chỉ ghi `depends_on: [postgres]` thì khác gì?
6. `kafka` có `profiles: [streaming]`. Lệnh `make up` (không PROFILE) có bật nó không? Đọc [Makefile](../../Makefile) để chắc.

Kiểm tra câu trả lời bằng cách xem compose **sau khi resolve** biến và anchor:
```bash
docker compose --env-file .env --profile core --profile notebook config | less
```

### Bước 2 — Vận hành
```bash
make up
make ps
docker compose logs --tail=30 postgres
docker compose logs minio-init            # phải thấy "bucket ready"
docker compose exec postgres psql -U coffee -d coffee_pos -c "\dt"
docker compose exec postgres bash -c 'ls /docker-entrypoint-initdb.d /opt/samples'
docker stats --no-stream
```
Mọi lệnh hay dùng gom trong [`lab/cheatsheet.md`](lab/cheatsheet.md) — in ra dán cạnh màn hình.

### Bước 3 — MinIO: console và `mc`
Mở http://localhost:9001 (`minioadmin` / `minioadmin`) → Object Browser → bucket `lake` → 3 "thư mục" `bronze/ silver/ gold/` (thực ra là 3 object `.keep`, S3 không có thư mục).

Upload file CSV đầu tiên bằng `mc` **trong container minio**:
```bash
docker compose cp data/samples/stores.csv minio:/tmp/stores.csv
docker compose exec minio mc cp /tmp/stores.csv local/lake/bronze/stores/stores.csv
docker compose exec minio mc ls --recursive local/lake/bronze/
```
Reload console → `bronze/stores/stores.csv` xuất hiện. Đây đúng là chỗ `coffee-ingest` sẽ ghi Parquet ở video #07 (chỉ khác format).

### Bước 4 — Thêm service bằng override
Mở [`lab/compose.override.yml`](lab/compose.override.yml), điền 6 TODO để thêm **Adminer** (web UI cho Postgres, image `adminer:4.8.1`) ở port 8089, rồi:
```bash
docker compose --env-file .env -f docker-compose.yml -f videos/05-docker-cho-data-engineer/lab/compose.override.yml \
    --profile core up -d adminer
docker compose --env-file .env -f docker-compose.yml -f videos/05-docker-cho-data-engineer/lab/compose.override.yml ps adminer
```
Mở http://localhost:8089 → System `PostgreSQL`, Server **`postgres`** (tên service, không phải `localhost`), user/pass `coffee`/`coffee`, DB `coffee_pos`.
Lời giải: [`lab/solution/compose.override.yml`](lab/solution/compose.override.yml). Xoá service khi xong: thay `up -d adminer` bằng `rm -sf adminer`.

### Bước 5 — RAM theo profile
Đầu `docker-compose.yml` ghi ước lượng: core ~1,5 GB, notebook +2 GB, streaming +3 GB, spark +2,5 GB, orchestration +3 GB, analytics +2 GB. Kiểm chứng phần core + notebook bằng `docker stats --no-stream` và ghi lại số thật của máy bạn.

## Kết quả mong đợi
- `make ps`: `postgres` và `minio` **healthy**, `jupyter` healthy (hoặc `running` vài chục giây đầu), `minio-init` **`exited (0)`** — thoát với mã 0 là đúng, không phải lỗi.
- `docker compose logs minio-init` kết thúc bằng `bucket ready`. `\dt` liệt kê 5 bảng. `ls /opt/samples` thấy 8 file CSV/JSONL giống `data/samples/` trên máy bạn (bind mount).
- `docker compose config` cho thấy `postgres.healthcheck` đã được "trải" ra đủ `interval: 10s, timeout: 5s, retries: 12, start_period: 20s` + `test` — anchor đã biến mất, đó là việc của YAML, không phải Docker.
- Bước 3: `mc ls --recursive local/lake/bronze/` có 2 dòng: `bronze/.keep` và `bronze/stores/stores.csv` (2,0 KiB).
- Bước 4: `ps adminer` là `healthy`; trang Adminer liệt kê 5 bảng, bấm `pos_transactions` → `select` thấy 2 481 dòng.
- `make ps` thật (macOS, Docker Desktop; cột PORTS rút gọn):
  ```
  NAME                 IMAGE                            SERVICE      STATUS                   PORTS
  dea2z-jupyter-1      dea2z/jupyter:3.5.3              jupyter      Up 1 minute (healthy)    0.0.0.0:8888->8888/tcp
  dea2z-minio-1        bitnamilegacy/minio:2024.10.13   minio        Up 9 minutes (healthy)   0.0.0.0:9000-9001->9000-9001/tcp
  dea2z-postgres-1     postgres:16.4                    postgres     Up 9 minutes (healthy)   0.0.0.0:5432->5432/tcp
  ```
  (`minio-init` chỉ hiện với `docker compose ps -a`: `Exited (0)`.)
- `docker stats --no-stream` khi rảnh, đo thật trên Docker Desktop (giới hạn 7,75 GiB):
  ```
  NAME                 CPU %     MEM USAGE
  dea2z-jupyter-1      0.00%     167 MiB
  dea2z-minio-1        0.06%     89 MiB
  dea2z-postgres-1     0.37%     27 MiB
  ```
  Core + notebook **chưa tới 300 MB** lúc rảnh — con số 1,5–2 GB ở đầu compose là *đỉnh* khi chạy Spark `local[*]` trong notebook. Để so sánh, lúc bật **tất cả** profile trên cùng máy: kafka 320 MB, schema-registry 205 MB, kafka-ui 154 MB, debezium 423 MB, spark master+worker ~480 MB (rảnh), airflow webserver 765 MB + scheduler 524 MB + db 39 MB, marquez api 397 MB + web 89 MB + db 113 MB, trino 758 MB — tổng ≈ **4,5 GB rảnh**, chưa kể JVM Spark/Trino nở ra khi có query.

Trả lời bước 1 (tự chấm): (1) prefix cho network `dea2z_default`, volume `dea2z_pgdata`…, container `dea2z-postgres-1`; (2) anchor YAML, merge 4 key healthcheck vào, chỉ sống trong 1 file; (3) init/one-shot container, `$$` để compose không thay biến `$l` của shell; (4) `pgdata` mất khi `reset`, 2 bind mount là file trên máy — sửa là thấy ngay; (5) chỉ chờ container *start*, Postgres có thể chưa nhận kết nối → Jupyter lỗi lúc boot; (6) không — `make up` chỉ bật `core` + `notebook`.

## Câu hỏi phỏng vấn của video
> "Pipeline Airflow của bạn chạy `psql` sang Postgres và lỗi `connection refused` khoảng 1/10 lần, chỉ xảy ra ngay sau khi deploy. Bạn nghi ngờ gì trong compose/K8s manifest, kiểm chứng bằng lệnh nào, và sửa ở đâu: healthcheck, depends_on, hay retry trong code?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Thay Adminer bằng **pgweb** (`sosedoff/pgweb`, pin một version cụ thể) trong override của bạn. pgweb nhận connection string qua biến `PGWEB_DATABASE_URL` — bạn viết URL đó thế nào bên trong network? Dán file override dưới comment.
2. Cố tình làm `postgres` **unhealthy**: đổi `POSTGRES_USER` trong `.env` thành `wrong` rồi `make up`. `make ps` báo gì? `docker inspect --format '{{json .State.Health}}' dea2z-postgres-1` nói gì? Jupyter có lên không? Trả lại `.env` và `make reset` sau khi xong.
3. Viết lệnh **một dòng** upload cả 8 file trong `data/samples/` lên `lake/bronze/<tên-bảng>/` bằng `mc pipe` qua stdin (không dùng `docker compose cp`). Mở Discussion so lệnh với mọi người.
