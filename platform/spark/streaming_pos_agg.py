"""Video #15 — Structured Streaming: Kafka pos.transactions → doanh thu mỗi cửa hàng theo cửa sổ 1 phút.

    coffeegen stream --topic pos.transactions --rate 20        # terminal 1 (producer)
    spark-submit streaming_pos_agg.py --bootstrap kafka:29092   # terminal 2

Điểm cần hiểu: event time (ts trong message) ≠ processing time; watermark 10 phút cho phép bản ghi tới muộn
(coffeegen cố ý tạo ~1%) vẫn được tính, nhưng muộn hơn thì bỏ.
"""

from __future__ import annotations

from common import base_parser, build_session
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType, StructField, StructType

ITEM = StructType(
    [
        StructField("item_id", StringType()),
        StructField("size", StringType()),
        StructField("qty", IntegerType()),
        StructField("unit_price_vnd", LongType()),
    ]
)
SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("store_id", StringType()),
        StructField("ts", StringType()),
        StructField("channel", StringType()),
        StructField("payment_method", StringType()),
        StructField("customer_id", StringType()),
        StructField("total_amount_vnd", LongType()),
        StructField("discount_vnd", LongType()),
        StructField("items", ArrayType(ITEM)),
    ]
)


def main() -> None:
    p = base_parser(__doc__)
    p.add_argument("--bootstrap", default="kafka:29092")
    p.add_argument("--topic", default="pos.transactions")
    p.add_argument("--sink", choices=["console", "parquet"], default="console")
    p.add_argument("--checkpoint", default="/tmp/chk/pos_agg")
    args = p.parse_args()

    spark = build_session("streaming_pos_agg", args.master)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )
    events = (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
        .select("e.*")
        .filter(F.col("store_id").isNotNull())
        .withColumn("event_time", F.to_timestamp("ts"))
        .dropDuplicates(
            ["transaction_id"]
        )  # at-least-once producer → dedup by key (state bounded by watermark)
    )
    agg = (
        events.withWatermark("event_time", "10 minutes")
        .groupBy(F.window("event_time", "1 minute"), "store_id")
        .agg(F.count("*").alias("orders"), F.sum("total_amount_vnd").alias("revenue_vnd"))
    )

    if args.sink == "console":
        q = (
            agg.writeStream.outputMode("update")
            .format("console")
            .option("truncate", "false")
            .start()
        )
    else:
        q = (
            agg.writeStream.outputMode("append")
            .format("parquet")
            .option("path", f"{args.lake}/gold/rt_store_revenue_1m")
            .option("checkpointLocation", args.checkpoint)
            .start()
        )
    q.awaitTermination()


if __name__ == "__main__":
    main()
