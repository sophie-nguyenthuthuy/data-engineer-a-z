# Lab #11 — checklist tối ưu Spark (điền số đo của bạn)

Chạy 4 mode của `platform/spark/skew_demo.py` rồi điền bảng. Lấy số từ Spark UI (http://localhost:8085 → application → **Stages** → stage có shuffle → cột *Duration* của task dài nhất, *Shuffle Read*; **Jobs** → tổng thời gian).

```bash
for m in naive salted broadcast aqe; do
  docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
    /opt/jobs/skew_demo.py --mode $m --rows 5000000
done
```

## 1. Bảng đo

| mode | tổng thời gian job (s) | task dài nhất (s) | task ngắn nhất (s) | số task stage join | shuffle read (MB) của task dài nhất |
|------|-----------------------:|------------------:|-------------------:|-------------------:|------------------------------------:|
| naive | TODO | TODO | TODO | TODO | TODO |
| salted | TODO | TODO | TODO | TODO | TODO |
| broadcast | TODO | TODO | TODO | TODO | TODO |
| aqe | TODO | TODO | TODO | TODO | TODO |

## 2. Trả lời ngắn (1–2 câu mỗi ô)

| Câu hỏi | Trả lời của bạn |
|---------|-----------------|
| Ở mode `naive`, task nào dài nhất và nó xử lý store nào? Vì sao bạn biết? | TODO |
| `salted` thêm cột gì vào fact và làm gì với dim? Dữ liệu của SG01 được chia thành bao nhiêu phần? | TODO |
| `broadcast` bỏ hẳn bước gì? Vì sao chỉ dùng được khi dim nhỏ? Ngưỡng mặc định là bao nhiêu MB? | TODO |
| `aqe` sửa skew **lúc nào** (compile hay runtime)? Dựa vào thông tin gì? | TODO |
| Nếu dim stores có 30 dòng thật (như Phin Phố), bạn chọn mode nào cho production? Vì sao? | TODO |

## 3. Điền config → tác dụng

| Config | Giá trị trong repo | Tác dụng (1 câu) |
|--------|--------------------|------------------|
| `spark.sql.shuffle.partitions` | 8 (spark-defaults.conf) | TODO |
| `spark.sql.autoBroadcastJoinThreshold` | -1 trong mode naive/salted/aqe | TODO |
| `spark.sql.adaptive.enabled` | true (defaults) nhưng skew_demo tắt ở 3 mode | TODO |
| `spark.sql.adaptive.skewJoin.enabled` | chỉ bật ở mode aqe | TODO |
| `spark.sql.adaptive.coalescePartitions.enabled` | mặc định true | TODO |
| `.coalesce(1)` trong `bronze_to_silver_pos.py` | — | TODO |
| `.repartition(k)` | không dùng trong repo | TODO — khác `coalesce` ở điểm gì? |
| `.cache()` / `.persist()` | không dùng trong repo | TODO — khi nào nên, khi nào không? |
| `write.target-file-size-bytes` | 128 MB (Iceberg DDL) | TODO |

## 4. Small files (chạy offline)

```bash
uv run python videos/11-toi-uu-spark/lab/small_files.py --out /tmp/small_files --files 500
```

| | files | bytes | polars_ms | pyarrow_ms |
|--|------:|------:|----------:|-----------:|
| one_file | 1 | TODO | TODO | TODO |
| many_files | 500 | TODO | TODO | TODO |
| compacted | 1 | TODO | TODO | TODO |

Vì sao 500 file lại **nặng hơn** 1 file dù cùng dữ liệu? TODO
