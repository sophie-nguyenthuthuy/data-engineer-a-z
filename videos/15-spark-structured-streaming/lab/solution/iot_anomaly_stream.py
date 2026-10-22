"""Lời giải lab #15 — Structured Streaming: cảnh báo boiler quá nóng theo cửa sổ 1 phút / máy.

Chạy (cần PROFILE=streaming + spark; file nằm ngoài /opt/jobs nên copy vào container trước):
    docker compose cp videos/15-spark-structured-streaming/lab/solution/iot_anomaly_stream.py spark-master:/tmp/
    docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
        --py-files /opt/jobs/common.py /tmp/iot_anomaly_stream.py --bootstrap kafka:29092 --watermark "2 minutes"
Producer (terminal khác):
    uv run --with confluent-kafka coffeegen stream --source iot --topic iot.readings --start 2026-02-16 --rate 100 --limit 20000

Cùng ngưỡng với solution/anomaly_rules.py (95.5 °C) — bản Spark của hàm alert_windows().
"""

from __future__ import annotations

from common import base_parser, build_session
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

SCHEMA = StructType(
    [
        StructField("reading_id", StringType()),
        StructField("machine_id", StringType()),
        StructField("store_id", StringType()),
        StructField("ts", StringType()),
        StructField("ingested_at", StringType()),
        StructField("boiler_temp_c", DoubleType()),
        StructField("pump_pressure_bar", DoubleType()),
        StructField("water_flow_ml_s", DoubleType()),
        StructField("shots_total", LongType()),
        StructField("status", StringType()),
        StructField("firmware", StringType()),
    ]
)


def main() -> None:
    p = base_parser(__doc__)
    p.add_argument("--bootstrap", default="kafka:29092")
    p.add_argument("--topic", default="iot.readings")
    p.add_argument("--watermark", default="2 minutes", help='thử "1 minute" và "10 minutes"')
    p.add_argument("--window", default="1 minute")
    p.add_argument("--threshold", type=float, default=95.5)
    p.add_argument("--output-mode", choices=["update", "append", "complete"], default="update")
    args = p.parse_args()

    spark = build_session("iot_anomaly_stream", args.master)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )
    readings = (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("r"))
        .select("r.*")
        .withColumn("event_time", F.to_timestamp("ts"))
        .withColumn("ingest_time", F.to_timestamp("ingested_at"))
        .withWatermark("event_time", args.watermark)
        .dropDuplicates(["reading_id"])  # ~0.5% duplicate; state giữ trong phạm vi watermark
    )

    hot = F.when(F.col("boiler_temp_c") > args.threshold, 1).otherwise(0)
    agg = readings.groupBy(F.window("event_time", args.window), "machine_id", "store_id").agg(
        F.max("boiler_temp_c").alias("max_temp_c"),
        F.count("*").alias("readings"),
        F.sum(hot).alias("hot_readings"),
        # ingested_at − ts: bao nhiêu giây bản ghi tới muộn (coffeegen: ~1% muộn 1–10 phút)
        F.max(F.unix_timestamp("ingest_time") - F.unix_timestamp("event_time")).alias("max_lag_s"),
    )
    alerts = agg.filter(F.col("max_temp_c") > args.threshold)

    q = (
        alerts.writeStream.outputMode(args.output_mode)
        .format("console")
        .option("truncate", "false")
        .option("numRows", 60)
        .start()
    )
    q.awaitTermination()


if __name__ == "__main__":
    main()
