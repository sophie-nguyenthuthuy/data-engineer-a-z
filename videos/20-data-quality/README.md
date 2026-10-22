# #20 — Data Quality: Great Expectations, dbt test, contracts

📺 Video: `TODO:youtube-link` · 📑 Slide: [V20-Data-Quality.pdf](slides/V20-Data-Quality.pdf)

## Bạn sẽ học được gì
- Phân biệt 3 lớp kiểm tra chất lượng và chi phí của từng lớp: **data contract** (schema + ràng buộc, rẻ nhất), **dbt test** (ở gold), **Great Expectations** (thống kê phân phối, đắt nhất).
- Đọc và viết một data contract YAML; chạy `contract_check.py` (1 trang code) để kiểm tra Parquet trên lake.
- Hiểu vì sao ngưỡng cứng (`min_rows_per_day`) cần **exceptions** — Tết là ví dụ sống.
- Phân biệt dbt **generic test** và **singular test**, biết khi nào dùng cái nào.
- Chạy một GX checkpoint và đọc kết quả expectation.

## Yêu cầu trước khi xem
- Đã xem #07 (silver + quarantine) và #12 (dbt). #19 giúp hiểu DAG `coffee_quality_checks` nhưng không bắt buộc.
- Lab này chạy **offline**, không cần Docker: chỉ cần `make install` và `make smoke` (tạo `./lake-local`).
- Phần GX (bước 5) tải thêm package ~100 MB bằng `uv run --with`.

## Lab: ba lớp kiểm tra trên silver

### Bước 1 — Tạo lake và chạy contract có sẵn
```bash
make smoke   # = coffee-ingest run --source data/samples --lake ./lake-local --date 2026-02-15
uv run python platform/quality/contract_check.py \
  --contract platform/quality/contracts/pos_transactions.yaml \
  --parquet "lake-local/silver/pos_transactions/dt=2026-02-15/*.parquet"
```
Mở [`contracts/pos_transactions.yaml`](../../platform/quality/contracts/pos_transactions.yaml) và [`contract_check.py`](../../platform/quality/contract_check.py) cạnh nhau: mỗi key trong YAML (`required`, `unique`, `allowed`, `pattern`, `min`) tương ứng đúng một `if` trong hàm `check()`.

### Bước 2 — Ngày Tết và `exceptions`
```bash
uv run coffee-ingest run --source data/samples --lake ./lake-local --date 2026-02-17
uv run python platform/quality/contract_check.py \
  --contract platform/quality/contracts/pos_transactions.yaml \
  --parquet "lake-local/silver/pos_transactions/dt=2026-02-17/*.parquet"
```
Mùng 1 Tết chỉ có 59 hoá đơn. Contract có `min_rows_per_day: 50` nên 59 vẫn **qua** — nhưng hãy thử: sửa tạm `min_rows_per_day: 100` rồi chạy lại. Vẫn OK! Vì `2026-02-17` nằm trong `exceptions.low_volume_dates`. Xoá ngày đó khỏi exceptions → `FAILED ❌ only 59 rows < min_rows_per_day 100`. Nhớ hoàn lại file (`git checkout platform/quality/contracts/pos_transactions.yaml`).

Bài học: mọi ngưỡng cứng đều cần danh sách ngoại lệ **có lý do nghiệp vụ**, nếu không alert sẽ kêu đúng những ngày cả team đang nghỉ.

### Bước 3 — Viết contract cho `pos_line_items`
Mở [`lab/contracts/pos_line_items.yaml`](lab/contracts/pos_line_items.yaml), điền các TODO (cột, kiểu, `allowed` cho `size`, `pattern` cho `item_id`, `min` cho `quantity`...). Schema cột xem ở [SCHEMA.md](../../data/generator/SCHEMA.md#fact-pos_line_items); kiểu dữ liệu thực tế xem bằng:
```bash
uv run python -c "import polars as pl; print(pl.read_parquet('lake-local/silver/pos_line_items/dt=2026-02-15/*.parquet').schema)"
```
Rồi chạy:
```bash
uv run python platform/quality/contract_check.py \
  --contract videos/20-data-quality/lab/contracts/pos_line_items.yaml \
  --parquet "lake-local/silver/pos_line_items/dt=2026-02-15/*.parquet"
```

### Bước 4 — Phá contract để thấy FAIL
Trong contract của bạn, đổi `allowed: [S, M, L]` thành `allowed: [S, M]` và chạy lại. Phải thấy `size: values not allowed ['L']`. Đây là tình huống thật: team POS thêm size `XL` mà không báo → contract bắt ngay ở silver, trước khi dashboard sai.

### Bước 5 — Great Expectations (tuỳ chọn, chậm)
```bash
uv run --with "great-expectations>=1.1,<2" python platform/quality/run_checkpoint.py --date 2026-02-15 --lake ./lake-local
```
Lần đầu `uv` tải GX và dependency (~1–3 phút). Suite nằm ở [`suites/pos_transactions_suite.py`](../../platform/quality/suites/pos_transactions_suite.py) — viết bằng code để review được trong PR; mỗi expectation có lý do nghiệp vụ bên cạnh (`mostly=0.3` cho `customer_id` vì khách vãng lai không có id).

### Bước 6 — dbt test: generic vs singular
```bash
cd platform/dbt && uv run dbt test --profiles-dir . --target dev
```
- **Generic** test: khai báo trong YAML, tái sử dụng cho mọi cột — `unique`, `not_null`, `accepted_values`, `relationships` trong [`models/staging/schema.yml`](../../platform/dbt/models/staging/schema.yml). Repo tự viết thêm 2 generic test ở [`tests/generic/`](../../platform/dbt/tests/generic) để bạn thấy bên trong chỉ là một `SELECT` trả về dòng vi phạm.
- **Singular** test: một file SQL cho một quy tắc nghiệp vụ cụ thể — [`tests/assert_revenue_non_negative.sql`](../../platform/dbt/tests/assert_revenue_non_negative.sql). Test pass khi query trả về **0 dòng**.

Quy tắc chọn: ràng buộc cột → generic; ràng buộc xuyên bảng/nghiệp vụ → singular.

### Bước 7 — Test offline cho lời giải
```bash
uv run pytest videos/20-data-quality -q
```

## Kết quả mong đợi
- Bước 1: `contract silver.pos_transactions v1.0 — 792 rows — OK ✅`, exit code 0.
- Bước 2: `contract silver.pos_transactions v1.0 — 59 rows — OK ✅`. Sau khi sửa `min_rows_per_day: 100` **và** bỏ ngày 17 khỏi exceptions: `FAILED ❌` với dòng `  - only 59 rows < min_rows_per_day 100`, exit code 1.
- Bước 3: `contract silver.pos_line_items v1.0 — 1392 rows — OK ✅`. (Silver ngày 15/02: 1 401 dòng thô → 1 392 sau khi cách ly 3 `negative_qty` + 6 `orphan_txn`; size L/M/S = 243/660/489.)
- Bước 4: `FAILED ❌` + `  - size: values not allowed ['L']`.
- Bước 5 (đã chạy trên samples): GX in `Calculating Metrics: 100% 42/42` rồi dòng cuối `✅ 8/8 expectations passed for dt=2026-02-15`. 8 expectation nhưng 42 metric — GX tách mỗi expectation thành nhiều metric nhỏ (count, null count, min/max…) và cache dùng chung.
- Bước 6: cần `dbt build` (video #12) trước, vì `dbt test` chỉ chạy test trên bảng đã có — chạy trên DuckDB trống sẽ ra `ERROR=28` kiểu `Table ... does not exist`. Trên lake-local 4 ngày: `Done. PASS=33 WARN=0 ERROR=0 SKIP=0 TOTAL=33`, trong đó có `dbt_utils_free_positive_stg_pos_line_items_quantity` và `assert_revenue_non_negative`. (`dbt build` đầy đủ là `PASS=45` = 11 model + 1 snapshot + 33 test.)
- Bước 7: `4 passed` (~1 s: test tự chạy pipeline vào thư mục tạm cho 15/02 và 17/02 rồi gọi `contract_check.check()`).

## Câu hỏi phỏng vấn của video
> "Team POS vừa thêm size `XL`. Pipeline của bạn sẽ phát hiện ở đâu trước: contract ở silver, dbt test ở gold, hay dashboard? Bạn muốn nó fail **cứng** (dừng pipeline) hay **mềm** (cảnh báo, vẫn chạy)? Vì sao?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Thêm vào contract `pos_line_items` một ràng buộc **xuyên cột**: `line_total_vnd == quantity × unit_price_vnd`. `contract_check.py` hiện chưa hỗ trợ — bạn đề xuất key YAML mới (ví dụ `checks: [{expr: "..."}]`) và viết thêm `if` tương ứng. Mô tả thiết kế dưới comment, không cần sửa `platform/`.
2. Viết một **singular test** dbt: không có hoá đơn `channel = 'app'` nào thanh toán `cash` (SCHEMA.md nói app không bao giờ cash). Dán SQL dưới comment.
3. `min_rows_per_day` là ngưỡng tĩnh. Đề xuất một cách phát hiện "số dòng bất thường" **động** (so với trung bình 4 tuần cùng thứ) và chỉ ra một trường hợp cách đó vẫn fail. Mở Discussion.
