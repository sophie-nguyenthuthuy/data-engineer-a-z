# #24 — Portfolio, CV, phỏng vấn

📺 Video: `TODO:youtube-link` · 📑 Slide: [V24-Portfolio-CV-phong-van.pdf](slides/V24-Portfolio-CV-phong-van.pdf)

## Bạn sẽ học được gì
- Biến repo khoá học thành **portfolio của bạn**: fork, đổi dữ liệu/bài toán, viết README dự án theo khung Problem → Architecture → Data → Pipeline → Quality → Lessons, quay demo 3 phút.
- Viết gạch đầu dòng CV theo công thức **động từ + việc + công cụ + kết quả đo được**, lấy số thật từ chính dự án Phin Phố.
- Ôn 24 câu hỏi phỏng vấn của 24 video + 20 câu phổ biến ở thị trường VN (SQL, Spark, Kafka, modeling, system design) với gợi ý trả lời ngắn.
- Biết nhà tuyển dụng đọc gì trong 30 giây đầu và họ nghi ngờ gì khi thấy "dự án khoá học".

## Yêu cầu trước khi xem
- Đã làm xong (hoặc gần xong) lab các video trước — nếu chưa, hãy xem video này để biết **mình đang làm vì cái gì**, rồi quay lại.
- Không cần service nào. Chỉ cần một trình soạn thảo và tài khoản GitHub.

## Lab: đóng gói 23 video thành một dự án của bạn

### Bước 1 — Fork và đổi "chủ sở hữu" (30 phút)
Làm theo [`lab/portfolio_checklist.md`](lab/portfolio_checklist.md): fork repo, đổi tên dự án, **đổi dữ liệu** (ít nhất một trong: thêm nguồn mới, đổi domain từ cà phê sang thứ bạn hiểu — nhà thuốc, phòng gym, giao hàng), xoá những video bạn không dùng. Repo giữ nguyên 100% = người đọc biết ngay là bài tập chép.

Nguyên tắc: **một thay đổi có ý nghĩa** (ví dụ thêm nguồn `weather.csv` thật từ Open-Meteo và nối vào `is_rainy_day`) đáng giá hơn 10 thay đổi cosmetic.

### Bước 2 — Viết README dự án (60 phút)
Dùng khung [`lab/project_readme_template.md`](lab/project_readme_template.md). Mỗi mục có câu hỏi dẫn và độ dài gợi ý. Bản mẫu điền sẵn cho Phin Phố ở [`lab/solution/project_readme_example.md`](lab/solution/project_readme_example.md) — đọc để biết "đủ" là bao nhiêu, đừng copy.

Kiểm tra nhanh: một người **không** xem khoá học đọc README của bạn trong 2 phút có trả lời được 3 câu không: dự án giải quyết gì, dữ liệu chảy từ đâu đến đâu, bạn đã kiểm chứng nó chạy đúng bằng cách nào?

### Bước 3 — Quay demo 3 phút
Màn hình + giọng nói, không cần mặt. Kịch bản: 20 s bài toán → 60 s chạy pipeline thật (`make smoke`, Airflow Graph xanh, Marquez) → 60 s mở một bảng gold và một quarantine giải thích lỗi dữ liệu → 40 s bài học. Upload YouTube unlisted, gắn link vào README. Một video 3 phút trả lời được 80% câu "dự án này có thật không".

### Bước 4 — CV bullets (30 phút)
Mở [`lab/cv_bullets.md`](lab/cv_bullets.md): 10 gạch đầu dòng mẫu viết từ dự án Phin Phố với **số thật** của repo (792 hoá đơn/ngày sau dedup, 1.5% bản ghi lỗi cố ý, 88% dung lượng tiết kiệm với Parquet, 11 model dbt…). Phần TODO: viết lại 5 bullet bằng số của **dự án bạn đã đổi** ở bước 1. Bullet không có số → xoá hoặc đo lại.

### Bước 5 — Ôn phỏng vấn (nhiều buổi)
[`lab/interview_questions.md`](lab/interview_questions.md) gom 24 câu hỏi cuối mỗi video + 20 câu phổ biến khác. Cách dùng: che phần gợi ý, trả lời **thành tiếng** trong 2 phút, rồi mở gợi ý ở [`lab/solution/interview_questions.md`](lab/solution/interview_questions.md). Câu nào không trả lời được → quay lại video đó, làm lại lab.

## Kết quả mong đợi
- Repo fork của bạn có: README theo khung 6 mục (≤ 2 màn hình), ảnh kiến trúc, link demo 3 phút, badge CI xanh (CI của repo chạy được trên fork mà không cần secret nào).
- `cv_bullets.md` của bạn: 5 bullet, mỗi bullet ≤ 2 dòng, **mỗi bullet có ít nhất một con số** bạn tự đo được và giải thích được cách đo.
- Trả lời được 44 câu trong `interview_questions.md` mà không nhìn gợi ý, mỗi câu ≤ 2 phút. Nếu bạn làm đủ lab, ít nhất 24 câu đầu là chuyện bạn **đã tự tay làm** — đó là khác biệt giữa "biết" và "làm được".
- Không có test tự động cho video này. "Test" là gửi README cho một người bạn làm nghề khác đọc 2 phút.

## Câu hỏi phỏng vấn của video
> "Kể về dự án data bạn tự hào nhất. Số liệu nào trong đó bạn **tự đo**, đo bằng cách nào, và nếu tôi nghi ngờ con số đó thì bạn chứng minh bằng gì ngay bây giờ?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Đăng link repo portfolio + README của bạn vào **Discussion "Portfolio review"** của repo này. Mình và các bạn khác sẽ góp ý theo đúng 3 câu kiểm tra ở bước 2.
2. Chọn 3 câu trong `interview_questions.md` bạn trả lời **tệ nhất**, viết câu trả lời 5 dòng cho mỗi câu dưới comment video — viết ra là cách ôn nhanh nhất.
3. Tìm 1 tin tuyển dụng Data Engineer thật ở VN (TopCV/LinkedIn/ITviec), đối chiếu với 24 video: kỹ năng nào JD cần mà khoá chưa dạy? Mở Discussion — đó là gợi ý cho mùa 2.
