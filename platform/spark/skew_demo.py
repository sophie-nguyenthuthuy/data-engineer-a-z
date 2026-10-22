"""Video #11 — data skew, broadcast join, AQE. Run with --mode naive|salted|broadcast|aqe and compare the Spark UI.

Kịch bản: 1 cửa hàng (SG01) chiếm 40% số dòng. Join với dim stores + group by store_id → 1 task làm hết việc.
"""

from __future__ import annotations

from common import base_parser, build_session
from pyspark.sql import functions as F

SALT_BUCKETS = 8


def skewed_dataset(spark, n_rows: int):
    """Synthetic in-memory dataset so the demo runs without any lake."""
    base = spark.range(n_rows).withColumnRenamed("id", "i")
    store = F.when(F.col("i") % 10 < 4, F.lit("SG01")).otherwise(
        F.concat(F.lit("HN"), F.lpad((F.col("i") % 15 + 1).cast("string"), 2, "0"))
    )
    return base.withColumn("store_id", store).withColumn("amount", (F.col("i") % 90 + 1) * 1000)


def main() -> None:
    p = base_parser(__doc__)
    p.add_argument("--mode", choices=["naive", "salted", "broadcast", "aqe"], default="naive")
    p.add_argument("--rows", type=int, default=5_000_000)
    args = p.parse_args()

    spark = build_session(f"skew_demo_{args.mode}", args.master)
    spark.conf.set("spark.sql.adaptive.enabled", str(args.mode == "aqe").lower())
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", str(args.mode == "aqe").lower())
    if args.mode != "broadcast":
        spark.conf.set(
            "spark.sql.autoBroadcastJoinThreshold", "-1"
        )  # force shuffle join to show the problem

    fact = skewed_dataset(spark, args.rows)
    dim = spark.createDataFrame(
        [(f"HN{i:02d}", "Hà Nội") for i in range(1, 16)] + [("SG01", "TP.HCM")],
        ["store_id", "city"],
    )

    if args.mode == "salted":
        fact = fact.withColumn("salt", (F.rand(seed=1) * SALT_BUCKETS).cast("int"))
        dim = dim.crossJoin(spark.range(SALT_BUCKETS).withColumnRenamed("id", "salt"))
        joined = fact.join(dim, ["store_id", "salt"])
    elif args.mode == "broadcast":
        joined = fact.join(F.broadcast(dim), "store_id")
    else:
        joined = fact.join(dim, "store_id")

    result = (
        joined.groupBy("city", "store_id")
        .agg(F.sum("amount").alias("revenue"))
        .orderBy(F.desc("revenue"))
    )
    result.show(5, truncate=False)
    print(
        "👉 Mở Spark UI (http://localhost:8085 → application → Stages) và so sánh thời gian task dài nhất giữa các mode."
    )
    spark.stop()


if __name__ == "__main__":
    main()
