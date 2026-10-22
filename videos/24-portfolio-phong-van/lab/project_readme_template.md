# <Tên dự án> — <một dòng: làm gì, cho ai>

<!-- Khung README dự án data. Thay mọi <...> và TODO:. Độ dài mục tiêu: ≤ 2 màn hình, 1 ảnh kiến trúc, mọi con số tái tạo được bằng lệnh trong repo. -->

[![CI](https://github.com/<user>/<repo>/actions/workflows/ci.yml/badge.svg)](https://github.com/<user>/<repo>/actions) · 📺 [Demo 3 phút](<link>)

## 1. Problem
<!-- 3–5 dòng. Ai dùng dữ liệu, để quyết định gì, hiện tại họ đau ở đâu. Không nói về công nghệ ở mục này. -->
TODO: Ví dụ: "Chuỗi <X> có <N> điểm bán, <M> nguồn dữ liệu rời. Quản lý vùng cần doanh thu theo ngày/cửa hàng trước 8h sáng; hiện tổng hợp tay bằng Excel, sai số ~5% vì bản ghi trùng và lệch múi giờ."

## 2. Architecture
<!-- 1 ảnh (draw.io / excalidraw, export PNG vào docs/). Dưới ảnh: 1 bảng "lớp → công nghệ → vì sao chọn". -->
![architecture](docs/architecture.png)

| Lớp | Công nghệ | Vì sao (1 dòng) |
|-----|-----------|-----------------|
| Nguồn | TODO | |
| Ingest | TODO | |
| Lưu trữ | TODO | |
| Transform | TODO | |
| Orchestration | TODO | |
| Quality / Observability | TODO | |

## 3. Data
<!-- Schema tóm tắt (link tới SCHEMA.md đầy đủ), khối lượng thật, và DANH SÁCH LỖI DỮ LIỆU bạn xử lý — mục này là thứ phân biệt dự án thật với bài tập. -->
- Nguồn & khối lượng: TODO (ví dụ: "POS ~800 hoá đơn/ngày/30 cửa hàng, app events ~2 000/ngày, IoT 60 máy × 1 reading/phút")
- Lỗi dữ liệu và cách xử lý:

| Lỗi | Tỉ lệ | Xử lý ở lớp | Cách |
|-----|-------|-------------|------|
| TODO: duplicate | | silver | giữ bản đầu theo key + ts |
| TODO: ... | | | |

## 4. Pipeline
<!-- Lệnh chạy từ clone sạch. Người đọc phải copy-paste được. Ghi thời gian chạy thật. -->
```bash
make install && make up
make smoke                          # bronze + silver cho 1 ngày, ~<N> s
cd platform/dbt && dbt build        # gold: <N> model, <N> test
```
- Lịch chạy: TODO (ví dụ "Airflow 02:00 hằng ngày, retries 2, SLA 1 h; backfill bằng DAG riêng với params start/end")
- Idempotency: TODO (ví dụ "partition `dt=` ghi đè; chạy lại không tạo bản ghi trùng — test `test_end_to_end_local_lake`")

## 5. Quality
<!-- Bạn kiểm chứng đúng bằng gì, không phải "đã test kỹ". -->
- Data contract: TODO (file, số ràng buộc, chạy ở đâu)
- Tests: TODO (số test, chạy trong CI? thời gian?)
- Metrics / lineage: TODO (rows_in/out, quarantine ratio, Marquez?)
- Một lần quality **bắt được lỗi thật**: TODO — kể ngắn 3 dòng. Nếu chưa có, ghi "chưa có" (trung thực hơn là bịa).

## 6. Lessons
<!-- 3 điều. Ít nhất 1 điều bạn làm SAI rồi sửa. Người phỏng vấn đọc mục này đầu tiên. -->
1. TODO
2. TODO
3. TODO (điều làm sai: ...)

## Việc tiếp theo
<!-- 3–5 issue đang mở. Repo có backlog = dự án sống. -->
- [ ] TODO

---
<sub>Khởi đầu từ khoá học "Data Engineer từ A–Z" (Nguyễn Thu Thuỷ, CC BY-NC-SA 4.0 cho nội dung, MIT cho code). Những thay đổi của mình: TODO (1 dòng).</sub>
