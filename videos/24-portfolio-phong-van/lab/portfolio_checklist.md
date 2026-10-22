# Checklist: biến repo khoá học thành portfolio của bạn

Tick từng ô. Mục nào có `TODO:` là chỗ bạn phải điền quyết định của mình — không có đáp án chung.

## A. Fork và "đổi chủ"
- [ ] Fork repo về tài khoản của bạn. Đổi tên repo thành tên dự án (không giữ `data-engineer-a-z`).
- [ ] `TODO:` Tên dự án / domain mới: ______________________ (cà phê → ? Chọn thứ bạn hiểu nghiệp vụ.)
- [ ] Sửa `README.md` gốc: xoá phần "khoá học 24 video", thay bằng README dự án (xem `project_readme_template.md`).
- [ ] Xoá `videos/` các bài bạn không dùng. Giữ lại `platform/`, `data/`, `infra/`, `.github/`.
- [ ] Giữ `LICENSE` (MIT cho code) và ghi nguồn một dòng: "Khởi đầu từ khoá học Data Engineer từ A–Z của Nguyễn Thu Thuỷ" — trung thực là một điểm cộng, không phải điểm trừ.

## B. Một thay đổi có ý nghĩa (chọn ít nhất 1)
- [ ] Thêm **nguồn dữ liệu mới** nối vào pipeline hiện có (ví dụ thời tiết thật từ Open-Meteo thay cho `is_rainy_day`; tỉ giá; lịch nghỉ lễ).
- [ ] Đổi **domain**: sửa `data/generator` sinh dữ liệu cho nghiệp vụ khác (nhà thuốc: đơn thuốc + tồn kho; gym: check-in + gói tập). Giữ 6 loại lỗi cố ý.
- [ ] Thêm **một lớp** chưa có: Iceberg table thật cho silver, hoặc dashboard Metabase/Superset đọc gold, hoặc alert Telegram từ Airflow.
- [ ] `TODO:` Thay đổi của bạn là: ______________________ . Vì sao nó có ý nghĩa với người dùng dữ liệu? ______________________

## C. Chứng minh nó chạy
- [ ] CI xanh trên fork (`.github/workflows/ci.yml` chạy được không cần secret). Gắn badge vào README.
- [ ] `make smoke` + `dbt build` chạy từ clone sạch trong < 10 phút. Ghi thời gian thật vào README.
- [ ] Ít nhất 3 test của chính bạn trong `platform/ingestion/tests` hoặc `lab/solution/test_*.py`.
- [ ] Demo 3 phút (YouTube unlisted / Loom), link trong README. Kịch bản: bài toán 20 s → pipeline chạy thật 60 s → gold + quarantine 60 s → bài học 40 s.

## D. README dự án (khung 6 mục, ≤ 2 màn hình)
- [ ] Problem · Architecture (1 ảnh) · Data (schema + lỗi cố ý) · Pipeline (lệnh chạy) · Quality (contract/test/metric) · Lessons (3 điều, có ít nhất 1 điều bạn làm sai).
- [ ] Mọi con số trong README đều **tái tạo được** bằng lệnh có trong repo.
- [ ] Không có từ "đơn giản", "mạnh mẽ", "hiện đại". Có số, có lệnh, có ảnh.

## E. Hồ sơ GitHub
- [ ] Pin repo lên profile. Mô tả repo 1 dòng có domain + 3 công nghệ chính.
- [ ] Commit history sạch: không có "fix", "update" liên tiếp — squash trước khi public.
- [ ] Issues: mở 3–5 issue "việc tiếp theo" cho chính bạn. Repo có backlog = dự án sống.
- [ ] `TODO:` Ngày bạn public repo: ____ / ____ / ______ . Gửi link vào Discussion "Portfolio review".
