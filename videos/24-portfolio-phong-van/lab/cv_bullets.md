# CV bullets — công thức "động từ + việc + công cụ + kết quả đo được"

Mỗi bullet ≤ 2 dòng, **một con số bạn tự đo**, công cụ nêu đích danh. Không có số → đo lại hoặc bỏ.
Người tuyển dụng đọc bullet trong 3 giây: động từ đầu câu cho họ biết bạn *làm* hay chỉ *tham gia*.

## 10 bullet mẫu từ dự án Phin Phố (số thật của repo này)

1. **Xây** pipeline batch bronze → silver → gold cho 3 nguồn (POS Postgres, app clickstream JSONL, IoT CSV) bằng Polars + DuckDB + dbt; phát hiện và cách ly **~1.5 %** bản ghi lỗi (duplicate, null store, lệch múi giờ, quantity âm) vào bảng quarantine thay vì xoá.
2. **Thiết kế** silver idempotent theo partition `dt=` (ghi đè, không append): chạy lại cùng ngày **0 bản ghi trùng**, kiểm chứng bằng test end-to-end trong CI.
3. **Chuẩn hoá** timestamp lệch offset (+07:00 thiếu) về UTC trong silver — loại sai số **7 giờ** trên ~0.6 % hoá đơn làm doanh thu theo ngày lệch giữa hai ngày liền nhau.
4. **Mô hình hoá** star schema 4 dim + 2 fact bằng dbt (**11 model, SCD2 snapshot bảng giá**), 100 % cột khoá có test `unique`/`not_null`, 2 generic test tự viết không cần `dbt_utils`.
5. **Giảm 88 %** dung lượng lưu trữ và chi phí quét Athena ước tính **từ 6.4 USD xuống 0.04 USD/tháng** (90 ngày dữ liệu, 900 query) bằng Parquet zstd + partition pruning theo ngày — số đo từ mô hình chi phí tự viết trên dữ liệu thật.
6. **Điều phối** pipeline hằng ngày bằng Airflow 2.9 (TaskFlow + Dataset-triggered quality DAG), `retries=2`, backfill 4 ngày bằng dynamic task mapping tuần tự để không đè MinIO.
7. **Viết** data contract YAML + checker 80 dòng (Polars) cho 2 bảng silver; bắt lỗi schema/giá trị trước dbt, có exceptions theo lịch nghiệp vụ (Tết) để tránh **false alert 3 ngày/năm**.
8. **Phát** lineage OpenLineage từ Airflow và dbt vào Marquez; xuất **20 metric** Prometheus (rows_in/out/quarantined theo bảng và lý do) từ summary JSON của pipeline.
9. **Dựng** CI GitHub Actions 5 job song song (ruff, pytest, `dbt parse`, smoke run + contract, `terraform validate`) — mọi PR được kiểm trong **< 5 phút**, không cần warehouse thật. <!-- TODO: đo thời gian CI thật trên fork của bạn -->
10. **Áp** least privilege 3 lớp: IAM policy Terraform chỉ 1 bucket, MinIO policy chỉ-đọc `gold/`, Postgres role analyst với column-level GRANT che **3 cột PII** (phone/email/full_name); tokenize `customer_id` bằng HMAC-SHA256 có salt.

## TODO: 5 bullet của BẠN

Viết lại bằng dự án bạn đã đổi ở `portfolio_checklist.md` (phần B). Mỗi bullet trả lời được "đo bằng lệnh nào?".

1. TODO: **<động từ>** <việc> bằng <công cụ>; <kết quả có số>. — *đo bằng:* `<lệnh>`
2. TODO:
3. TODO:
4. TODO:
5. TODO:

## Tự kiểm tra trước khi gửi CV
- [ ] Đọc to từng bullet: có thể nói tiếp 2 phút về nó không? Nếu không → bỏ.
- [ ] Mỗi con số: bạn **tự** đo, hay chép từ khoá học? Chỉ giữ số tự đo (hoặc tự chạy lại và xác nhận).
- [ ] Không có từ: "tham gia", "hỗ trợ", "tìm hiểu", "làm quen". Có: xây, thiết kế, giảm, phát hiện, chuẩn hoá, dựng.
- [ ] Bullet 1 là bullet mạnh nhất. Người đọc có thể dừng sau bullet 1.
