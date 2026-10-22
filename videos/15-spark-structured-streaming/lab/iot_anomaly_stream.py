"""Lab #15 — Structured Streaming: cảnh báo boiler_temp_c > 95.5 °C theo cửa sổ 1 phút cho từng máy.

Chạy (PROFILE=streaming + spark; file nằm ngoài /opt/jobs nên copy vào container trước):
    docker compose cp videos/15-spark-structured-streaming/lab/iot_anomaly_stream.py spark-master:/tmp/
    docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
        --py-files /opt/jobs/common.py /tmp/iot_anomaly_stream.py --bootstrap kafka:29092
Producer (terminal khác):
    uv run --with confluent-kafka coffeegen stream --source iot --topic iot.readings --start 2026-02-16 --rate 100 --limit 20000

Mẫu để chép: platform/spark/streaming_pos_agg.py. Ngưỡng và luật thuần Python: lab/solution/anomaly_rules.py.
"""

from __future__ import annotations

from common import base_parser, build_session
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# TODO 1: hoàn thiện schema message IoT theo data/generator/SCHEMA.md (xem message thật:
#   uv run coffeegen stream --source iot --dry-run --start 2026-02-16 --limit 2).
#   boiler_temp_c là DoubleType (có NULL), shots_total là LongType, ts/ingested_at để StringType rồi to_timestamp.
SCHEMA = StructType(
    [
        StructField("reading_id", StringType()),
        StructField("machine_id", StringType()),
        StructField("ts", StringType()),
        StructField("boiler_temp_c", DoubleType()),
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
        # TODO 2: .withColumn("ingest_time", F.to_timestamp("ingested_at"))
        # TODO 3: .withWatermark("event_time", args.watermark) rồi .dropDuplicates(["reading_id"])
        #   (thứ tự quan trọng: watermark trước để state dedup không phình mãi)
    )

    # TODO 4: groupBy(F.window("event_time", args.window), "machine_id", "store_id") và agg:
    #   max_temp_c = max(boiler_temp_c), readings = count(*),
    #   hot_readings = sum(when(boiler_temp_c > threshold, 1).otherwise(0)),
    #   max_lag_s = max(unix_timestamp(ingest_time) − unix_timestamp(event_time))
    agg = readings

    # TODO 5: chỉ giữ cửa sổ có max_temp_c > args.threshold
    alerts = agg

    # TODO 6: writeStream console, outputMode = args.output_mode, truncate=false. So sánh 3 mode.
    q = alerts.writeStream.format("console").start()
    q.awaitTermination()


if __name__ == "__main__":
    main()
