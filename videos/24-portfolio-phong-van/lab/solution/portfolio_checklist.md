# Checklist portfolio — bản mẫu đã điền (ví dụ: fork thành "Phin Phố + thời tiết thật")

Đây là một cách điền, không phải cách duy nhất. Điểm cần nhìn: mỗi quyết định có **lý do nghiệp vụ** và **cách kiểm chứng**.

## A. Fork và "đổi chủ"
- [x] Fork → đổi tên repo `phinpho-data-platform`.
- [x] Domain giữ cà phê (mình hiểu nghiệp vụ F&B), nhưng bài toán đổi: từ "doanh thu ngày" sang **"doanh thu ngày có giải thích được bằng thời tiết"** — câu quản lý vùng thật sự hỏi.
- [x] README gốc thay bằng README dự án (xem `project_readme_example.md`).
- [x] Xoá `videos/00–06, 24`; giữ `videos/19–23` đổi tên thành `docs/runbooks/` vì nội dung lab chính là runbook vận hành.
- [x] Dòng ghi nguồn ở cuối README.

## B. Một thay đổi có ý nghĩa
- [x] Thêm nguồn **Open-Meteo** (API miễn phí, không cần key): job `coffee-ingest weather --date` kéo mưa/giờ cho 2 thành phố vào `bronze/weather/dt=`; silver `weather_daily` (city, date, rain_mm, is_rainy = rain_mm ≥ 1); dbt `dim_date_weather` join vào `fct_daily_store_revenue` thay cờ `is_rainy_day` sinh ngẫu nhiên.
- [x] Vì sao có ý nghĩa: cờ `is_rainy_day` cũ là **dữ liệu sinh**, không kiểm chứng được; giờ quản lý có thể hỏi "mưa thì app tăng bao nhiêu" với nguồn độc lập. Thay đổi chạm đủ 4 lớp (ingest → silver → gold → test), chứng minh mình hiểu toàn pipeline chứ không chỉ một file.
- [x] Kiểm chứng: contract `weather_daily.yaml` (rain_mm ≥ 0, đủ 2 city × mỗi ngày), test `test_weather_join_has_no_null_flags`.

## C. Chứng minh nó chạy
- [x] CI xanh trên fork, badge trong README. Thêm job `weather-contract` chạy contract trên file mẫu commit sẵn (không gọi API thật trong CI — cố ý, để CI deterministic).
- [x] Clone sạch → `make install && make up && make smoke && dbt build` = 6 phút 40 giây trên MacBook M2, ghi trong README. <!-- TODO: số của bạn -->
- [x] Test của mình: `test_weather_ingest.py` (3), `test_v23_cost_model.py` mở rộng với retention (1).
- [x] Demo 3 phút: 0:00 bài toán · 0:20 `make smoke` + Airflow Graph · 1:20 `fct_daily_store_revenue` join thời tiết trên DuckDB, chỉ vào ngày mưa · 2:20 quarantine giải thích null_store · 2:40 bài học `datetime.now()`.

## D. README dự án
- [x] 6 mục, 1 ảnh excalidraw, ≤ 2 màn hình.
- [x] Mọi số có lệnh: 792 (`make smoke`), 11 model (`dbt build`), 88 % (`cost_model.py`), 6:40 (đo tay, ghi ngày đo).
- [x] Đã grep và xoá "đơn giản/mạnh mẽ/hiện đại".

## E. Hồ sơ GitHub
- [x] Pin repo; mô tả: "Data platform cho chuỗi cà phê 30 cửa hàng — Polars · dbt · Airflow · Kafka, có thời tiết thật".
- [x] Squash lịch sử thành ~15 commit theo cột mốc (ingest / silver rules / dbt / airflow / quality / weather).
- [x] 4 issue mở: Iceberg cho silver; alert Telegram; dashboard Metabase; đổi generator sang 90 ngày.
- [x] Public ngày __/__/____, đã gửi Discussion "Portfolio review".
