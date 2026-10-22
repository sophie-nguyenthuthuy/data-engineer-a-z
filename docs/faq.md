# FAQ

### Mình chưa biết gì về lập trình, học được không?
Được nếu bạn chịu gõ. Video #03 (SQL) và #04 (Python) bắt đầu từ đầu, nhưng tốc độ nhanh. Nếu thấy đuối, dừng lại 1–2 tuần học SQL cơ bản (bất kỳ nguồn miễn phí nào) rồi quay lại — repo không đi đâu cả.

### Máy 8 GB RAM có học hết được không?
Phần 1–2: thoải mái. Phần 3–4: bật từng profile một (`make down` rồi `make up PROFILE=streaming`), hoặc chạy Spark bằng `local[*]` trong Jupyter thay cho cluster. Xem bảng RAM trong [SETUP.md](../SETUP.md).

### Windows dùng được không?
Dùng WSL 2 + Docker Desktop. Làm việc hoàn toàn trong WSL (clone vào `~/`, không vào `/mnt/c`). Mọi lệnh trong khoá đều là lệnh Linux.

### Có cần tài khoản AWS không?
Không. Toàn bộ khoá dùng MinIO (S3 local). AWS chỉ là tuỳ chọn ở #09 (so sánh) và #22 (Terraform). Nếu làm phần đó, nhớ `terraform destroy`.

### Sao không dùng Snowflake / BigQuery / Databricks?
Vì (1) cần thẻ tín dụng hoặc trial hết hạn, (2) mình muốn bạn hiểu *bên dưới* trước khi dùng cái đã đóng gói. Học xong khoá, chuyển sang bất kỳ nền tảng nào cũng chỉ là đổi cú pháp.

### Sao dữ liệu lại có lỗi? Repo bị bug à?
Cố ý. ~1.5% dòng có duplicate, `store_id` null, timestamp thiếu múi giờ, quantity âm. Danh sách đầy đủ ở [SCHEMA.md](../data/generator/SCHEMA.md). Việc của Data Engineer phần lớn là phát hiện và xử lý chúng.

### Số liệu trong "Kết quả mong đợi" khác máy mình một chút?
Nếu bạn chạy đúng `data/samples` thì phải **giống hệt** (generator deterministic). Khác → bạn đang dùng dữ liệu tự sinh (`coffeegen` với tham số khác) hoặc volume Postgres cũ. `make reset && make up` rồi thử lại. Thời gian chạy (giây) thì khác máy là bình thường.

### Mình muốn dữ liệu lớn hơn để thử Spark cho "thật"
```bash
uv run coffeegen pos --days 365 --stores 60 --scale 3 --out ./out   # ~7 triệu hoá đơn
```
Rồi `coffee-ingest run --source ./out ...`. Đừng commit thư mục `out/`.

### Có thể dùng repo này dạy lớp của mình không?
Có, miễn phí, ghi nguồn, theo [CC BY-NC-SA 4.0](../LICENSE-CONTENT). Khoá thu phí thì liên hệ mình trước.

### Nộp bài tập ở đâu?
Trả lời câu hỏi phỏng vấn dưới comment video. Bài tập code: tự làm, so với `solution/`. Muốn được review: mở Discussion kèm link gist/fork — mình và cộng đồng sẽ góp ý. Không PR bài tập vào repo chính.

### Slide ở đâu?
`videos/NN-slug/slides/` — thêm sau khi mỗi video phát hành.

### Lịch phát hành có đổi không?
Có thể. Bảng trong README là nguồn chính xác nhất; mình cập nhật trạng thái 🟢/⚪ khi lên video.

### Mình tìm thấy lỗi / muốn đóng góp
Issue mẫu *Bug report*, hoặc PR theo [PULL_REQUEST_TEMPLATE](../.github/PULL_REQUEST_TEMPLATE.md). Sửa chính tả cũng là đóng góp.
