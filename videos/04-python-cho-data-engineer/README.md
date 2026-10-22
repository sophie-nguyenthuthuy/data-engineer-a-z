# #04 — Python cho Data Engineer: Pandas, Polars, DuckDB

📺 Video: `TODO:youtube-link`

## Bạn sẽ học được gì
- Viết một pipeline nhỏ (đọc CSV → parse timestamp → lọc → group) bằng **3 engine** và hiểu khi nào dùng cái nào.
- Phân biệt **eager** (pandas: mỗi dòng chạy ngay) và **lazy** (Polars `LazyFrame`, DuckDB SQL: lập plan rồi mới chạy) — và vì sao lazy nhanh hơn trên dữ liệu lớn.
- Biết Apache Arrow là gì và vì sao Polars ↔ DuckDB ↔ pandas trao đổi dữ liệu gần như miễn phí.
- Dùng lại code của platform (`parse_vn_timestamp`, `read_csv_raw` trong `coffee_ingestion`) thay vì viết lại — kỹ năng số 1 khi vào team.

## Yêu cầu trước khi xem
- Đã xem [#03](../03-sql-cho-data-engineer). Biết Python cơ bản (hàm, list, dict).
- **Không cần Docker.** Chỉ cần `make install` (có pandas 3, Polars 2, DuckDB 2 trong `.venv`).

## Lab: một pipeline, ba engine

Pipeline: `pos_transactions.csv` → parse `ts` có/không có `+07:00` → bỏ `store_id` NULL → lọc ngày nghiệp vụ 2026-02-15 (giờ VN) → doanh thu theo cửa hàng. Chưa dedup — việc đó là của silver (#07).

### Bước 1 — Chạy starter
```bash
uv run python videos/04-python-cho-data-engineer/lab/compare_engines.py
```
Ba hàm đều `raise NotImplementedError` → in "chưa làm". Mỗi hàm trả về `list[(store_id, revenue_vnd)]` sắp theo `store_id`, nên 3 hàm so được với nhau bằng `==`.

### Bước 2 — pandas (TODO 1–3)
Điểm khó duy nhất là `ts`: `pd.to_datetime(..., utc=True)` sẽ coi chuỗi **không có offset** là UTC → lệch 7 giờ. Phải nối `+07:00` vào trước. Nhớ `total_amount_vnd` đang là chuỗi (vì `dtype=str`) — ép `int64` trước khi cộng.

### Bước 3 — Polars (TODO 4–6)
Mở [`platform/ingestion/src/coffee_ingestion/silver.py`](../../platform/ingestion/src/coffee_ingestion/silver.py), đọc `parse_vn_timestamp` — 6 dòng, đúng việc bước 2 bạn vừa làm bằng tay. Dùng lại nó. Trước khi `.collect()`, in `lf.explain()` để xem Polars đẩy `FILTER` xuống đâu.

### Bước 4 — DuckDB (TODO 7)
Viết lại bằng SQL — bạn vừa làm ở #03. Để ý DuckDB đọc thẳng file, không có DataFrame trung gian.

### Bước 5 — So và chạy test
```bash
uv run python videos/04-python-cho-data-engineer/lab/solution/compare_engines.py
uv run pytest videos/04-python-cho-data-engineer -q
```
Muốn thấy khác biệt thời gian rõ hơn, sinh 30 ngày (~195 k hoá đơn) rồi chạy lại với `--source ./out/pos_transactions.csv --date 2026-01-15`:
```bash
uv run coffeegen pos --start 2026-01-01 --days 30 --stores 30 --seed 7 --out ./out
```

## Kết quả mong đợi
- Trên samples, ngày 2026-02-15 (máy Mac M-series; số ms của bạn sẽ khác):
  ```
  pandas   30 cửa hàng  tổng   77,113,000 VND     14.8 ms
  polars   30 cửa hàng  tổng   77,113,000 VND      4.0 ms
  duckdb   30 cửa hàng  tổng   77,113,000 VND     31.0 ms
  ✓ 3 engine cho cùng kết quả
  Nếu đọc sai tz: 77,285,000 VND thay vì 77,113,000 VND (lệch +172,000)
  ```
  **77 113 000 VND** = 795 hoá đơn (chưa dedup, đã bỏ 4 dòng `store_id` NULL của ngày đó). Silver sau dedup là 76 939 000 — khác 174 000 là 3 hoá đơn trùng của ngày 15.
  `SG14` cao nhất: 4 776 000. Lệch **+172 000** nếu parse sai tz chính là hoá đơn `HN07-20260214-00014` (21:06 ngày 14, thiếu offset) bị trôi sang ngày 15.
- Trên samples DuckDB **chậm nhất** — không phải vì SQL chậm, mà vì mỗi lần gọi nó phải sniff + parse lại CSV; dữ liệu quá nhỏ để phần thực thi đáng kể. Với 30 ngày (195 k dòng) bức tranh đổi hẳn:
  ```
  pandas   30 cửa hàng  tổng  608,565,000 VND    724.0 ms
  polars   30 cửa hàng  tổng  608,565,000 VND     61.8 ms
  duckdb   30 cửa hàng  tổng  608,565,000 VND    106.1 ms
  ```
  pandas chậm hơn ~10× vì eager + single-thread + mỗi bước copy dữ liệu; Polars và DuckDB lazy, multi-thread, chỉ đọc 3 cột cần.
- `lf.explain()` của Polars kết thúc bằng `DF [...]; PROJECT["store_id", "ts", "total_amount_vnd"] 3/10 COLUMNS` — Polars nhìn cả chuỗi phép biến đổi và chỉ giữ 3 cột cần (projection pushdown). Đổi `read_csv_raw(path).lazy()` thành `pl.scan_csv(path, infer_schema=False, null_values=[""])` rồi explain lại: bạn sẽ thấy `Csv SCAN … PROJECT 3/10 COLUMNS … SELECTION: col("store_id").is_not_null()` — filter được đẩy xuống **tận lúc đọc file**. pandas không làm được vì nó không biết bước tiếp theo bạn định làm gì.

## Câu hỏi phỏng vấn của video
> "Pipeline của bạn chạy pandas trên máy 16 GB RAM, đọc file CSV 12 GB thì bị kill. Đồng nghiệp bảo 'đổi sang Spark'. Có cách nào rẻ hơn không? Bạn chọn giữa Polars lazy, DuckDB, và chunking pandas theo tiêu chí gì?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Thêm engine thứ 4: **PyArrow** thuần (`pyarrow.csv.read_csv` + `pyarrow.compute`). Kết quả có khớp không? Nó nhanh hay chậm hơn Polars, và vì sao Polars/DuckDB đều dùng Arrow bên dưới mà vẫn khác nhau? Dán kết quả dưới comment.
2. Chuyển DataFrame giữa các engine **không copy**: `duckdb.sql(...).pl()` → Polars, `pl_df.to_pandas()` → pandas, `duckdb.sql("SELECT * FROM pl_df")` đọc thẳng biến Polars. Đo thời gian chuyển với file 30 ngày. Giải thích bằng Arrow trong 3 dòng.
3. Thêm bước **dedup** (giống `clean_transactions` trong `silver.py`) vào cả 3 hàm. Tổng ngày 15 phải ra đúng 76 939 000. Hàm nào thêm dễ nhất? Mở Discussion nếu một trong 3 ra số khác.
