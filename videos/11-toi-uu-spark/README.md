# #11 — Tối ưu Spark: skew, broadcast, AQE

📺 Video: `TODO:youtube-link`

## Bạn sẽ học được gì
- Nhận ra **data skew** trên Spark UI trong 30 giây (Max vs Median duration của task, Shuffle Read lệch) và tái hiện nó bằng [`platform/spark/skew_demo.py`](../../platform/spark/skew_demo.py).
- Hiểu 3 cách sửa và thứ tự ưu tiên: **broadcast join** (dim nhỏ) → **AQE skew join** (tự động, có ngưỡng) → **salting** (thủ công, cho 2 bảng lớn).
- Phân biệt `coalesce` / `repartition`, biết khi nào `cache()` có hại, và đặt `spark.sql.shuffle.partitions` theo dữ liệu thay vì để 200.
- Đo được tận tay hậu quả của **small files** mà không cần Spark, và biết 3 cách chữa (coalesce khi ghi, compaction, target-file-size của Iceberg).

## Yêu cầu trước khi xem
- Đã xem #10 (đọc được Spark UI: jobs / stages / tasks).
- `make up PROFILE=spark`. Phần 4 của lab (small files) chạy **offline**, không cần Docker.

## Lab: một job, bốn cách chạy

### Bước 1 — Đọc `skew_demo.py` (60 dòng)
[`platform/spark/skew_demo.py`](../../platform/spark/skew_demo.py) tự sinh fact 5 triệu dòng **trong RAM** (không cần lake): `i % 10 < 4` → `SG01` chiếm 40 %, 60 % còn lại chia đều 15 cửa hàng Hà Nội. Dim chỉ 16 dòng. Job = join fact với dim rồi `groupBy(city, store_id)`. Bốn `--mode` chỉ khác nhau ở **cách join** và vài `spark.conf.set`. Trước khi chạy, tự đoán: mode nào nhanh nhất, chậm nhất?

### Bước 2 — Chạy 4 mode, mở Spark UI sau mỗi lần
```bash
for m in naive salted broadcast aqe; do
  echo "=== $m ==="
  docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
    /opt/jobs/skew_demo.py --mode $m --rows 5000000
done
```
Mỗi lần: http://localhost:8085 → application `skew_demo_<mode>` → Stages → bấm stage có *Shuffle Read* lớn nhất → bảng **Summary Metrics for Tasks** (Min / Median / Max của Duration và Shuffle Read Size). Điền vào [`lab/tuning_checklist.md`](lab/tuning_checklist.md) mục 1.

Máy yếu: giảm `--rows 1000000`. Máy 8 GB không chạy cluster: trong Jupyter `python work/platform/spark/skew_demo.py --mode naive --master "local[*]"`, UI ở http://localhost:4040 (chỉ sống khi job chạy — thêm `input()` tạm cuối `main()` trong bản copy của bạn nếu muốn xem).

### Bước 3 — Trả lời checklist mục 2 và 3
Mỗi config trong bảng mục 3 đều xuất hiện trong repo (`spark-defaults.conf`, `skew_demo.py`, `bronze_to_silver_pos.py`, `silver_tables.sql`). Tìm dòng đó rồi viết 1 câu tác dụng. Lời giải: [`lab/solution/tuning_checklist.md`](lab/solution/tuning_checklist.md) — đọc **sau** khi tự điền.

### Bước 4 — Small files, offline
Mở [`lab/small_files.py`](lab/small_files.py), điền 5 TODO: chia bảng 2 481 dòng thành 500 file, đo thời gian đọc bằng Polars và pyarrow.dataset, compact lại.
```bash
uv run python videos/11-toi-uu-spark/lab/small_files.py --out /tmp/small_files --files 500
uv run python videos/11-toi-uu-spark/lab/small_files.py --out /tmp/small_files --files 5000   # cho vui
uv run pytest videos/11-toi-uu-spark -q
```
Lời giải: [`lab/solution/small_files.py`](lab/solution/small_files.py). Nối lại với #10: `bronze_to_silver_pos.py` có `.coalesce(1)` trước `write` chính là để không sinh 8 file × mỗi ngày.

## Kết quả mong đợi
- Bước 2: cả 4 mode in cùng bảng, `TP.HCM | SG01 | 84 999 600 000` đứng đầu (= 40 % tổng), rồi HN15, HN10, HN05, HN14. Số đo thật từ **event log** của 4 lần chạy trên cluster compose (worker 2 core/2 GB, `--rows 5000000`), stage join + groupBy:

  | mode | wall job | số task stage shuffle | task ngắn nhất | median | **task dài nhất** | shuffle read của task dài nhất |
  |------|--------:|---:|---:|---:|---:|---:|
  | naive | 8,9 s | 8 | 15 ms | 250 ms | **1 115 ms** | 1,2 MB · **2 333 336 dòng** (47 %) |
  | salted | 8,3 s | 8 (+8 stage gộp lại) | 257 ms | 530 ms | 807 ms | 6,6 MB · 1 082 443 dòng (22 %) |
  | broadcast | **6,4 s** | 8 (chỉ groupBy, 10 dòng/task) | 4 ms | 6 ms | 88 ms | ~0 |
  | aqe | 7,9 s | **1** | — | — | **2 293 ms** | 2,0 MB · 5 000 016 dòng (100 %) |

  Đọc bảng: `naive` đúng kịch bản skew — Max gấp **4,5× Median**, task đó ôm 2,33 M dòng (SG01 40 % + vài cửa hàng HN rơi cùng hash bucket). `salted` chia SG01 thành 8 → Max chỉ còn 1,5× Median nhưng shuffle nhiều hơn (dim nhân 8). `broadcast` không shuffle fact cho join, nhanh nhất. Và **`aqe` tệ hơn `naive`**: AQE thấy toàn bộ shuffle chỉ ~2 MB (< `advisoryPartitionSizeInBytes` 64 MB) nên *gộp* 8 partition thành **1 task** làm hết 5 M dòng; còn tách skew thì không xảy ra vì partition SG01 (1,2 MB) xa ngưỡng `skewedPartitionThresholdInBytes` 256 MB. Với dữ liệu nhỏ, AQE tối ưu cho "ít task" chứ không cho "cân task". Muốn thấy AQE tách skew trên laptop: `--conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=100k --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=100k` (hoặc `--rows 200000000` nếu máy chịu được). Tổng thời gian 4 mode gần nhau (6–9 s) vì 5 M dòng quá nhỏ — phần lớn là khởi động JVM/executor; sự khác biệt nằm ở **hình dạng task**, đúng cái bạn phải đọc được trên UI.
- Bước 4 (`--files 500`, máy mình): `one_file` 36 227 bytes / 0,46 ms; `many_files` 500 file, **2 074 824 bytes (×57)**, 19,9 ms Polars (×43) và 55 ms pyarrow; `compacted` về lại 36 227 bytes. `pytest videos/11-toi-uu-spark -q` → **2 passed**.

## Câu hỏi phỏng vấn của video
> "Bạn join bảng giao dịch 2 TB với bảng khách hàng 50 GB, key `customer_id`, và 3 % khách 'vãng lai' có `customer_id = NULL`. Job treo ở 1 task cuối. Giải thích chuyện gì xảy ra và đưa ra 2 cách sửa — broadcast có dùng được không?"

Trả lời dưới comment video — mình sẽ ghim câu trả lời hay nhất.

## Bài tập
1. Thêm mode `salted_bad` vào bản copy của `skew_demo.py` với `SALT_BUCKETS = 2` rồi `= 64`. Vẽ (hoặc mô tả) task dài nhất theo số bucket. Vì sao nhiều bucket hơn không luôn tốt hơn?
2. Chạy `small_files.py --files 50 / 500 / 5000` và lập bảng `bytes_overhead_x`, `polars_slowdown_x`. Từ số liệu, ước lượng "mỗi file nhỏ tốn thêm bao nhiêu byte và bao nhiêu ms" trên máy bạn.
3. (Discussion) Pipeline streaming (#15) ghi micro-batch mỗi 1 phút → 1 440 file/ngày/bảng. Đề xuất chiến lược compaction: chạy lúc nào, gom theo gì, làm sao để query đang đọc không bị lỗi khi file bị thay? (Gợi ý: đây là lý do Iceberg/Delta tồn tại — #17.)
