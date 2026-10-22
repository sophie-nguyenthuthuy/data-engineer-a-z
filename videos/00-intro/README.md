# #00 — Giới thiệu khoá học & cách dùng repo

📺 Video: `TODO:youtube-link`

## Bạn sẽ học được gì
- Biết khoá học đi từ đâu tới đâu trong 24 video, và vì sao mình chọn một dự án xuyên suốt thay vì 24 ví dụ rời.
- Hiểu bài toán nghiệp vụ: chuỗi cà phê **Phin Phố** 30 cửa hàng, 3 nguồn dữ liệu (POS, app, cảm biến máy pha).
- Cài xong môi trường, chạy được query đầu tiên trên dữ liệu mẫu.
- Biết cách học hiệu quả với repo này: xem → lab → trả lời câu hỏi phỏng vấn dưới comment.

## Yêu cầu trước khi xem
- Laptop 8 GB RAM, cài được Docker (hướng dẫn: [SETUP.md](../../SETUP.md)).
- Biết lập trình cơ bản (bất kỳ ngôn ngữ nào). Chưa cần biết SQL hay Python — video #03, #04 sẽ dạy.

## Lab: chạy query đầu tiên

### Bước 1 — Clone và khởi động
```bash
git clone https://github.com/sophie-nguyenthuthuy/data-engineer-a-z.git
cd data-engineer-a-z
make install
make up
make ps
```
Chờ tới khi `postgres`, `minio`, `jupyter` đều `healthy`/`running` (lần đầu ~5 phút vì build image).

### Bước 2 — Nhìn dữ liệu nguồn
```bash
docker compose exec postgres psql -U coffee -d coffee_pos -c "\dt"
docker compose exec postgres psql -U coffee -d coffee_pos -c "SELECT * FROM stores LIMIT 5;"
```

### Bước 3 — Câu hỏi nghiệp vụ đầu tiên
Chạy file [`lab/first_query.sql`](lab/first_query.sql) — mình để sẵn một câu query đếm hoá đơn theo ngày, bạn **điền TODO** để tính thêm doanh thu:
```bash
docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/00-intro/lab/first_query.sql
```

### Bước 4 — Mở Jupyter
Vào http://localhost:8888 (token `dea2z`) → `work/videos/00-intro/lab/hello_data.ipynb` → Run All.

## Kết quả mong đợi
- `\dt` liệt kê 5 bảng: `stores`, `menu_items`, `customers`, `pos_transactions`, `pos_line_items`.
- Query ở bước 3 trả về **6 dòng** (14→19/02/2026). Ba ngày đầu mỗi ngày ~700–800 hoá đơn, ba ngày sau chỉ ~60: đó là **Tết** (mùng 1 = 17/02). Doanh thu ngày 15/02 khoảng **76–77 triệu VND**.
- Notebook in ra một bar chart doanh thu theo ngày với cú "rơi" rõ rệt từ ngày 17/02.

Nếu bảng rỗng: `make seed`. Nếu port bị chiếm: xem phần Troubleshooting trong SETUP.md.

## Câu hỏi phỏng vấn của video
> "Data Engineer khác Data Analyst và Data Scientist ở điểm nào? Trong một team 3 người đó, ai chịu trách nhiệm khi dashboard sai số?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Sửa query ở bước 3 để tính doanh thu theo **thành phố** (join `stores`).
2. Tìm số hoá đơn có `store_id` rỗng. Bạn nghĩ nguyên nhân là gì? (Gợi ý: đọc [SCHEMA.md](../../data/generator/SCHEMA.md).)
3. Mở issue theo mẫu **Lab question** nếu bạn kẹt ở bất kỳ bước nào — đó cũng là một kỹ năng của Data Engineer.
