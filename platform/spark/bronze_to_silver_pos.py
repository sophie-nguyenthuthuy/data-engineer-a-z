"""Video #10 — the same bronze → silver rules as platform/ingestion, written in PySpark.

Mục đích: bạn thấy cùng một logic (dedup, tz drift, quarantine) trông thế nào khi dữ liệu không còn vừa một máy.

    spark-submit bronze_to_silver_pos.py --date 2026-02-15 --lake s3a://lake
"""

from __future__ import annotations

from common import base_parser, build_session, layer_path
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def parse_vn_timestamp(col: str):
    """ISO string with or without offset → UTC timestamp. Missing offset ⇒ Vietnam local time."""
    has_offset = F.col(col).rlike(r"[+-]\d{2}:\d{2}$")
    fixed = F.when(has_offset, F.col(col)).otherwise(
        F.concat(F.regexp_replace(F.col(col), " ", "T"), F.lit("+07:00"))
    )
    return F.to_timestamp(fixed)  # Spark normalises to session tz = UTC (set below)


def clean_transactions(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = bronze.withColumn("ts_raw", F.col("ts")).withColumn("ts", parse_vn_timestamp("ts"))
    quarantine = typed.filter(F.col("store_id").isNull()).withColumn("_reason", F.lit("null_store"))
    good = typed.filter(F.col("store_id").isNotNull())
    w = Window.partitionBy("transaction_id").orderBy("ts")
    deduped = good.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    silver = deduped.select(
        "transaction_id",
        "store_id",
        "ts",
        "channel",
        "payment_method",
        "customer_id",
        "staff_id",
        F.col("total_amount_vnd").cast("long"),
        F.col("discount_vnd").cast("long"),
        (F.lower("is_rainy_day") == "true").alias("is_rainy_day"),
        (~F.col("ts_raw").rlike(r"[+-]\d{2}:\d{2}$")).alias("_tz_fixed"),
    )
    return silver, quarantine.select("transaction_id", "ts_raw", "store_id", "_reason")


def main() -> None:
    args = base_parser(__doc__).parse_args()
    spark = build_session("bronze_to_silver_pos", args.master)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    bronze = spark.read.parquet(layer_path(args.lake, "bronze", "pos_transactions", args.date))
    silver, quarantine = clean_transactions(bronze)

    # overwrite exactly one partition — idempotent re-runs (same rule as the Python pipeline)
    silver.coalesce(1).write.mode("overwrite").parquet(
        layer_path(args.lake, "silver", "pos_transactions", args.date)
    )
    quarantine.coalesce(1).write.mode("overwrite").parquet(
        layer_path(args.lake, "silver", "quarantine_pos_transactions", args.date)
    )

    print(
        f"bronze={bronze.count()} silver={silver.count()} quarantine={quarantine.count()} tz_fixed={silver.filter('_tz_fixed').count()}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
