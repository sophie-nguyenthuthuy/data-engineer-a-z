# Lời giải lab #11 — checklist tối ưu Spark

## 1. Bảng đo

Số đo thật (cluster compose, worker 2 core / 2 GB, `--rows 5000000`, đọc từ Spark event log; máy bạn sẽ khác về ms, không khác về hình dạng):

| mode | tổng thời gian job | task dài nhất | task ngắn nhất | median | số task stage join | shuffle read của task dài nhất |
|------|-------------------:|--------------:|---------------:|-------:|-------------------:|-------------------------------:|
| naive | 8,9 s | **1 115 ms** (4,5× median) | 15 ms | 250 ms | 8 | 1,2 MB · 2 333 336 dòng ≈ 47 % |
| salted | 8,3 s | 807 ms (1,5× median) | 257 ms | 530 ms | 8 (+ stage groupBy 8 task, 2–27 ms) | 6,6 MB · 1 082 443 dòng ≈ 22 % |
| broadcast | **6,4 s** | 88 ms | 4 ms | 6 ms | 8 (chỉ còn groupBy, 10 dòng/task) | ~0 (không shuffle cho join) |
| aqe | 7,9 s | **2 293 ms** | — | — | **1** (AQE gộp 8 → 1) | 2,0 MB · 5 000 016 dòng = 100 % |

Bất ngờ cần nhớ: với 5 M dòng, **AQE không tách skew mà còn gộp hết về 1 task** — tổng shuffle ~2 MB nhỏ hơn `advisoryPartitionSizeInBytes` (64 MB) nên `coalescePartitions` thắng, và partition SG01 1,2 MB xa ngưỡng tách 256 MB. AQE tối ưu theo *byte*, không theo *số dòng*; trên dữ liệu thật (GB/partition) nó hành xử đúng như lý thuyết bên dưới.

Cách nhận ra skew trên UI: Stages → cột **Duration** của task: *Min / 25th / Median / 75th / Max*. Skew = Max lớn hơn Median hàng chục lần, và *Shuffle Read Size / Records* của task đó cũng lớn tương ứng.

## 2. Trả lời

| Câu hỏi | Trả lời |
|---------|---------|
| Ở mode `naive`, task nào dài nhất, xử lý store nào? | Task nhận partition của key `SG01` (`i % 10 < 4` → 40 % dòng). Biết vì hash(`SG01`) rơi vào đúng 1 trong 8 shuffle partition; Shuffle Read của task đó ≈ 40 % tổng. Bấm vào stage → *Tasks* → sort theo Duration. |
| `salted` thêm gì? | Fact thêm cột `salt = rand() * 8` (int 0–7); dim `crossJoin` với `range(8)` để mỗi store có đủ 8 bản. Join trên (`store_id`, `salt`) → SG01 chia thành 8 key, mỗi key ~5 % dữ liệu. Giá: dim to gấp 8, code khó đọc hơn, phải nhớ `groupBy` lại theo `store_id` sau join (ở đây `groupBy("city","store_id")` làm việc đó). |
| `broadcast` bỏ gì? | Bỏ **shuffle của fact** cho join: dim (16 dòng) được gửi nguyên bản tới mọi executor, mỗi partition fact join tại chỗ (BroadcastHashJoin). Chỉ dùng khi dim nhỏ vì toàn bộ dim phải nằm trong RAM của *từng* executor và driver phải thu gom nó. Ngưỡng tự động mặc định `spark.sql.autoBroadcastJoinThreshold = 10 MB`. Phép `groupBy` phía sau vẫn shuffle nhưng dữ liệu đã được partial-aggregate trước nên nhẹ. |
| `aqe` sửa skew lúc nào? | **Runtime**: sau khi stage map của shuffle chạy xong, Spark đọc kích thước thật của từng shuffle partition (map output statistics). Partition nào > `skewedPartitionFactor` (5×) × median **và** > `skewedPartitionThresholdInBytes` (256 MB) thì tách nhỏ, phía dim được nhân bản tương ứng. Đo thật: partition SG01 chỉ **1,2 MB** → không tách; tệ hơn, `coalescePartitions` gộp 8 partition thành 1 task 2,3 s. Muốn thấy tách skew trên laptop: hạ cả hai ngưỡng (`skewedPartitionThresholdInBytes=100k`, `advisoryPartitionSizeInBytes=100k`) hoặc tăng `--rows` lên hàng trăm triệu. Bài học: AQE không phải phép màu, phải biết ngưỡng — và ngưỡng tính bằng byte. |
| Chọn mode nào cho Phin Phố? | `broadcast` — dim `stores` 30 dòng, `menu_items` 23 dòng: nhỏ hơn 10 MB hàng nghìn lần, Spark tự broadcast mà không cần `F.broadcast`. Salting chỉ dành cho join **hai bảng lớn** bị skew; AQE để bật mặc định như lưới an toàn. Thứ tự ưu tiên thực tế: broadcast → AQE → salting (thủ công, cuối cùng). |

## 3. Config → tác dụng

| Config | Giá trị | Tác dụng |
|--------|---------|----------|
| `spark.sql.shuffle.partitions` | 8 | Số partition **sau mỗi shuffle** (join, groupBy, window). Mặc định Spark là 200 — với dữ liệu KB/MB sinh 200 task tí hon, overhead lập lịch > tính toán. Quy tắc tay: ~128 MB shuffle data/partition, hoặc 2–3× số core. AQE `coalescePartitions` tự gộp lại khi bật. |
| `spark.sql.autoBroadcastJoinThreshold` | -1 ở naive/salted/aqe | -1 = **tắt** tự broadcast để demo thấy được shuffle join. Production: để mặc định 10 MB hoặc tăng lên 50–100 MB nếu executor có RAM. |
| `spark.sql.adaptive.enabled` | true ở defaults, skew_demo ghi đè | Bật AQE: re-plan ở runtime theo thống kê thật — gộp partition nhỏ, đổi sort-merge join thành broadcast nếu một bên hoá ra nhỏ, tách partition skew. Spark 3.2+ mặc định bật. |
| `spark.sql.adaptive.skewJoin.enabled` | chỉ aqe | Phần tách partition skew của AQE (cần `adaptive.enabled`). Hai ngưỡng đi kèm: `skewedPartitionFactor` (5) và `skewedPartitionThresholdInBytes` (256 MB). |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | Sau shuffle, gộp các partition nhỏ lại cho tới ~`advisoryPartitionSizeInBytes` (64 MB). Chống "200 file nhỏ" khi ghi. |
| `.coalesce(1)` | bronze_to_silver_pos | Giảm số partition **không shuffle** (gộp partition cùng executor) → ghi đúng 1 file/partition ngày. Chỉ hợp lý khi output nhỏ (< vài trăm MB); với dữ liệu lớn `coalesce(1)` ép 1 task ghi hết → chậm. |
| `.repartition(k)` | — | Shuffle **đầy đủ** để chia đều thành k partition (có thể theo cột: `repartition("store_id")`). Dùng để cân lại skew trước một phép tính nặng hoặc tăng parallelism; đắt hơn `coalesce` vì shuffle. `coalesce` chỉ giảm, `repartition` tăng/giảm đều được. |
| `.cache()` / `.persist()` | — | Giữ DataFrame trong RAM/disk của executor để **dùng lại nhiều action** (vd 4 lần `count()` trong bronze_to_silver_pos → bài tập #10). Không cache khi chỉ dùng 1 lần, hoặc khi dữ liệu lớn hơn RAM (spill → chậm hơn không cache). Nhớ `unpersist()`. |
| `write.target-file-size-bytes` | 128 MB | Iceberg gom dữ liệu khi ghi để mỗi file ≈ 128 MB — đúng khoảng "64–256 MB" trong `platform/lake/README.md`. Cách giải quyết small files **ở tầng bảng** thay vì nhớ `coalesce` ở từng job. |

## 4. Small files (số đo thật, Apple M-series, `--files 500`)

```json
{
  "rows": 2481,
  "one_file":   {"files": 1,   "bytes": 36227,   "polars_ms": 0.46,  "pyarrow_ms": 0.52},
  "many_files": {"files": 500, "bytes": 2074824, "polars_ms": 19.91, "pyarrow_ms": 55.12},
  "compacted":  {"files": 1,   "bytes": 36227,   "polars_ms": 0.59,  "pyarrow_ms": 0.47},
  "bytes_overhead_x": 57.3,
  "polars_slowdown_x": 43.3
}
```

Vì sao 500 file **nặng hơn 57 lần** dù cùng 2 481 dòng? Mỗi file Parquet mang footer (schema, statistics từng cột, offset) + dictionary page riêng cho từng cột ≈ 4 KB — với 5 dòng/file thì phần "bao bì" lớn hơn hàng hoá vài chục lần; nén zstd cũng không có gì để nén. Thời gian chậm hơn 43× vì phải mở 500 file, đọc 500 footer, lập 500 kế hoạch đọc — trên S3 mỗi lần mở là 1–2 request HTTP (và tốn tiền request). Cách sửa: `compact` (đọc tất cả, ghi lại 1 file) — trong Spark là `.coalesce(k)`/`.repartition(k)` trước `write`, trong Iceberg/Delta là `rewrite_data_files` / `OPTIMIZE` chạy định kỳ.
