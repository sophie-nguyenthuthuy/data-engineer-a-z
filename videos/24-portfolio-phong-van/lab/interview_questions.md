# 44 câu hỏi phỏng vấn Data Engineer — bản tự luyện

Cách dùng: đọc câu hỏi, **nói thành tiếng** câu trả lời trong ≤ 2 phút, ghi 3 dòng vào ô TODO, rồi so với
`solution/interview_questions.md`. Câu nào trả lời tệ → quay lại video tương ứng, làm lại lab.

Phần A là 24 câu kết mỗi video (bạn đã *làm* chúng trong lab — hãy trả lời bằng chuyện thật của repo).
Phần B là 20 câu hay gặp ở VN mà khoá học không có video riêng.

---

## Phần A — 24 câu từ 24 video

### #00 — Giới thiệu
> "Data Engineer khác Data Analyst và Data Scientist ở điểm nào? Trong một team 3 người đó, ai chịu trách nhiệm khi dashboard sai số?"

TODO:

### #01 — Data Engineer là gì
> "Team data của Phin Phố có đúng 3 người: 1 Data Engineer, 1 Data Analyst, 1 Data Scientist. Sáng mùng 1 Tết dashboard báo doanh thu ngày 30 Tết thấp hơn thực tế 7 triệu. Mỗi người kiểm tra cái gì trước, theo thứ tự nào, và ai là người 'own' con số đó?"

TODO:

### #02 — Kiến trúc dữ liệu
> "Sếp muốn dashboard doanh thu realtime, đề nghị 'cứ cho Metabase query thẳng vào database POS cho nhanh'. Bạn giải thích thế nào bằng query plan? Và nếu bắt buộc phải làm ngay tuần này, bạn chọn giải pháp tạm nào ít rủi ro nhất (read replica? CDC? dump hàng giờ?)."

TODO:

### #03 — SQL cho Data Engineer
> "Bảng hoá đơn có bản ghi trùng do retry. Bạn dedup bằng `ROW_NUMBER() ... ORDER BY ts` — nhưng hai bản sao có `ts` giống hệt nhau. Kết quả có deterministic không? Nếu hai bản sao khác nhau ở `total_amount_vnd` (bản sau là bản sửa), bạn đổi `ORDER BY` thế nào và cần thêm cột gì ở nguồn để làm được việc đó?"

TODO:

### #04 — Python cho Data Engineer
> "Pipeline của bạn chạy pandas trên máy 16 GB RAM, đọc file CSV 12 GB thì bị kill. Đồng nghiệp bảo 'đổi sang Spark'. Có cách nào rẻ hơn không? Bạn chọn giữa Polars lazy, DuckDB, và chunking pandas theo tiêu chí gì?"

TODO:

### #05 — Docker cho Data Engineer
> "Pipeline Airflow của bạn chạy `psql` sang Postgres và lỗi `connection refused` khoảng 1/10 lần, chỉ xảy ra ngay sau khi deploy. Bạn nghi ngờ gì trong compose/K8s manifest, kiểm chứng bằng lệnh nào, và sửa ở đâu: healthcheck, depends_on, hay retry trong code?"

TODO:

### #06 — Data modeling
> "Ngày 1/3 Phin Phố tăng giá Bạc xỉu từ 39k lên 45k. Với star schema của bạn, báo cáo margin tháng 2 có đổi không? Nếu PM hỏi 'margin tháng 2 *theo giá mới* là bao nhiêu' thì mô hình hiện tại trả lời được không? Bạn chọn SCD type 1, type 2, hay lưu giá vào fact — và cái giá phải trả của mỗi cách?"

TODO:

### #07 — ETL vs ELT
> "Pipeline của bạn chạy lúc 2h sáng, bị lỗi giữa chừng và được chạy lại lúc 3h. Làm sao bạn chắc chắn bảng silver không có dòng nào bị đếm hai lần? Nếu nguồn là Postgres đang thay đổi liên tục thì câu trả lời có khác không?"

TODO:

### #08 — Định dạng file
> "Team BI than query trên lake chậm dù dữ liệu chỉ 50 GB Parquet. Bạn mở footer vài file ra xem thì nên nhìn vào những gì, và 3 nguyên nhân hay gặp nhất là gì?"

TODO:

### #09 — Data Lake / S3
> "Lake của bạn có 3 năm dữ liệu POS partition theo ngày, mỗi ngày 1 file. Một hôm team phát hiện bug ở silver từ 6 tháng trước. Bạn làm gì, mất bao lâu, và thiết kế nào trong repo này làm việc đó khả thi? Nếu bronze cũng bị xoá theo lifecycle 90 ngày thì sao?"

TODO:

### #10 — Apache Spark
> "Job Spark của bạn có 200 task, 199 task xong trong 10 giây, 1 task chạy 20 phút. Bạn mở tab nào trong Spark UI trước, nhìn cột gì, và đoán nguyên nhân gì?"

TODO:

### #11 — Tối ưu Spark
> "Bạn join bảng giao dịch 2 TB với bảng khách hàng 50 GB, key `customer_id`, và 3 % khách 'vãng lai' có `customer_id = NULL`. Job treo ở 1 task cuối. Giải thích chuyện gì xảy ra và đưa ra 2 cách sửa — broadcast có dùng được không?"

TODO:

### #12 — dbt
> "Model `fct_sales` của bạn build lại toàn bộ 3 năm dữ liệu mỗi đêm, mất 40 phút. Bạn chuyển sang incremental thế nào, chọn `unique_key` gì, và điều gì xảy ra khi silver của một ngày cũ được re-process (late-arriving data)? Khi nào bạn vẫn chọn full refresh?"

TODO:

### #13 — Kafka
> "Topic có 3 partition, team bạn chạy 5 consumer cùng group để 'nhanh hơn'. Chuyện gì xảy ra, và nếu cần thật sự nhanh hơn thì phải làm gì? Tăng partition về sau có rủi ro gì với thứ tự message theo `store_id`?"

TODO:

### #14 — Delivery semantics, DLQ, Schema Registry
> "Pipeline của bạn đọc Kafka rồi ghi doanh thu vào Postgres. Sếp bảo 'phải exactly-once'. Bạn bật `enable.idempotence=true` và transactions là xong chưa? Nếu chưa, phần còn thiếu nằm ở đâu và bạn làm thế nào với bảng Postgres đó?"

TODO:

### #15 — Spark Structured Streaming
> "Job streaming của bạn đặt watermark 10 phút. PM hỏi: 'Sao dashboard trễ 10 phút, giảm xuống 30 giây được không?' Bạn trả lời thế nào — và nếu buộc phải giảm, bạn làm gì với dữ liệu tới muộn hơn 30 giây để không mất doanh thu?"

TODO:

### #16 — CDC với Debezium
> "Debezium connector của bạn chết lúc 2 giờ sáng, không ai biết tới 9 giờ. Chuyện gì xảy ra với Postgres của POS trong 7 tiếng đó, bạn kiểm tra bằng query gì, và thiết kế lại thế nào để lần sau nó tự kêu?"

TODO:

### #17 — Lakehouse: Iceberg / Delta / Hudi
> "Team bạn có data lake Parquet partition theo ngày, chạy ổn 2 năm. Sếp đọc bài về lakehouse và muốn chuyển sang Iceberg. Bạn nêu 3 vấn đề cụ thể mà Iceberg sẽ giải quyết **cho team này**, và 2 chi phí mới xuất hiện (gợi ý: catalog, compaction/expire snapshots)."

TODO:

### #18 — Kiến trúc streaming end-to-end
> "Khách hàng là chuỗi 30 cửa hàng, yêu cầu 'dashboard real-time'. Bạn có 3 tháng và 2 kỹ sư. Bạn đề xuất kiến trúc gì, chỗ nào dùng streaming, chỗ nào kiên quyết không, và bạn đo cái gì để chứng minh lựa chọn đó đúng sau khi go-live?"

TODO:

### #19 — Airflow
> "Pipeline hằng ngày của bạn fail lúc 02:00, 06:00 sáng bạn fix xong bug. Bạn chạy lại thế nào để không có bản ghi trùng, và `{{ ds }}` lúc đó có giá trị gì? Nếu DAG dùng `datetime.now()` thì chuyện gì xảy ra?"

TODO:

### #20 — Data Quality
> "Team POS vừa thêm size `XL`. Pipeline của bạn sẽ phát hiện ở đâu trước: contract ở silver, dbt test ở gold, hay dashboard? Bạn muốn nó fail **cứng** (dừng pipeline) hay **mềm** (cảnh báo, vẫn chạy)? Vì sao?"

TODO:

### #21 — Observability & Lineage
> "Dashboard doanh thu sáng nay thiếu 3 cửa hàng. Bạn có Airflow, Marquez và metric rows_in/rows_out theo bảng. Kể thứ tự bạn mở từng thứ để khoanh vùng trong 10 phút — và metric nào **nên** đã báo trước khi người dùng phát hiện?"

TODO:

### #22 — CI/CD + Terraform
> "PR của bạn đổi một model dbt. CI nên chạy gì để **đủ tin** merge mà không cần cluster/warehouse thật? `dbt parse` bắt được lỗi gì và bỏ sót lỗi gì? Nếu có 15 phút CI, bạn thêm bước nào tiếp theo?"

TODO:

### #23 — Chi phí & bảo mật
> "Marketing xin export danh sách khách gold kèm số điện thoại để chạy SMS Tết. Bạn đưa cho họ cái gì, qua đâu, và log gì? Nếu họ nói 'chỉ cần hash là đủ ẩn danh', bạn trả lời sao?"

TODO:

### #24 — Portfolio
> "Kể về dự án data bạn tự hào nhất. Số liệu nào trong đó bạn **tự đo**, đo bằng cách nào, và nếu tôi nghi ngờ con số đó thì bạn chứng minh bằng gì ngay bây giờ?"

TODO:

---

## Phần B — 20 câu phổ biến ở thị trường VN

### SQL
**B1.** `WHERE` khác `HAVING` ở đâu? Viết query lấy cửa hàng có doanh thu trung bình/ngày > 3 triệu trong tháng 2.

TODO:

**B2.** `RANK`, `DENSE_RANK`, `ROW_NUMBER` khác nhau thế nào? Khi dedup bạn dùng cái nào và vì sao?

TODO:

**B3.** Query `JOIN` 2 bảng chậm. Bạn đọc `EXPLAIN ANALYZE` nhìn vào gì? Khi nào thêm index **không** giúp?

TODO:

**B4.** CTE vs subquery vs temp table — có khác nhau về hiệu năng không? (Gợi ý: Postgres 12+ inline CTE.)

TODO:

**B5.** Viết query tính doanh thu **luỹ kế** theo ngày cho mỗi cửa hàng, và % thay đổi so với ngày trước.

TODO:

### Spark
**B6.** Transformation vs action; lazy evaluation giúp gì và gây khó gì khi debug?

TODO:

**B7.** `repartition` vs `coalesce`. Sau khi filter còn 1 % dữ liệu, bạn làm gì trước khi ghi ra Parquet?

TODO:

**B8.** Shuffle là gì, khi nào xảy ra, và bạn giảm nó bằng cách nào (3 cách)?

TODO:

**B9.** Spark `cache()` khi nào có lợi, khi nào có hại? Storage level nào bạn hay dùng?

TODO:

### Kafka / Streaming
**B10.** Consumer group, offset, commit — nếu consumer crash sau khi xử lý nhưng trước khi commit thì sao? Đó là semantics gì?

TODO:

**B11.** Event time vs processing time. Cho ví dụ trong repo này mà hai thứ lệch nhau tới 10 phút.

TODO:

**B12.** Kafka giữ thứ tự ở mức nào? Nếu cần mọi event của một cửa hàng theo đúng thứ tự, bạn thiết kế key thế nào?

TODO:

### Data modeling
**B13.** Star schema vs snowflake; vì sao BI tool thích star? Khi nào bạn cố ý denormalize thêm?

TODO:

**B14.** SCD type 1 / 2 / 3 — bảng giá menu đổi giá thì dùng type nào, và dbt snapshot làm việc đó ra sao?

TODO:

**B15.** Fact grain là gì? `fct_sales` grain line item còn `fct_daily_store_revenue` grain ngày × cửa hàng — vì sao cần cả hai?

TODO:

**B16.** Surrogate key vs natural key — khi nào `transaction_id` của POS **không** đủ làm khoá?

TODO:

### System design — "thiết kế pipeline cho …"
**B17.** Thiết kế pipeline cho **app giao đồ ăn 500 k đơn/ngày**: nguồn, ingest, lưu trữ, phục vụ dashboard 15 phút, chi phí. Vẽ và nêu 2 điểm sẽ hỏng đầu tiên khi ×10.

TODO:

**B18.** Thiết kế pipeline cho **ngân hàng**: giao dịch core banking → báo cáo cuối ngày cho NHNN trước 7h sáng. Yêu cầu nào thay đổi hoàn toàn thiết kế so với B17?

TODO:

**B19.** Thiết kế **CDC từ 50 bảng Postgres** sang lake với yêu cầu độ trễ < 5 phút và không làm chậm OLTP. Bạn đo gì để biết đang ảnh hưởng OLTP?

TODO:

**B20.** Bạn nhận một pipeline cũ không ai hiểu, chạy 6 giờ mỗi đêm, hay fail. Kế hoạch 30 ngày đầu của bạn?

TODO:
