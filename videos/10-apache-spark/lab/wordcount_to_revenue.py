"""Lab #10 — doanh thu theo cửa hàng, viết 3 cách: RDD → DataFrame API → Spark SQL.

CHỈ chạy được nơi có pyspark (Jupyter của repo hoặc container spark) — .venv trên máy KHÔNG có pyspark.
Trong Jupyter (http://localhost:8888, token dea2z) → New → Terminal:
    python work/videos/10-apache-spark/lab/wordcount_to_revenue.py \
        --lake /home/jovyan/work/lake-local --date 2026-02-15

Cả 3 cách phải cho cùng một kết quả: 30 cửa hàng, tổng 76 939 000 VND cho ngày 15/02.
Bài này là "word count" của Data Engineer: thay (word, 1) bằng (store_id, total_amount_vnd).
"""

from __future__ import annotations

import sys
from operator import add
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "platform" / "spark"))  # dùng lại common.py của repo

from common import base_parser, build_session, layer_path  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def revenue_rdd(df: DataFrame) -> list[tuple[str, int]]:
    """Cách 1 — RDD: map thành (key, value) rồi reduceByKey. Giống word count, chỉ khác value."""
    # TODO 1: pairs = df.rdd.map(lambda row: (row["store_id"], int(row["total_amount_vnd"])))
    # TODO 2: totals = pairs.reduceByKey(add)   ← đây là bước shuffle
    # TODO 3: trả về totals.sortBy(lambda kv: -kv[1]).collect()
    return []


def revenue_dataframe(df: DataFrame) -> DataFrame:
    """Cách 2 — DataFrame API: Catalyst tối ưu, code ngắn hơn, có schema."""
    # TODO 4: df.groupBy("store_id").agg(F.count("*").alias("transactions"),
    #           F.sum("total_amount_vnd").alias("revenue_vnd")).orderBy(F.desc("revenue_vnd"))
    return df.limit(0)


def revenue_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Cách 3 — Spark SQL: cùng plan với cách 2 nhưng ai biết SQL cũng đọc được."""
    df.createOrReplaceTempView("silver_pos")
    # TODO 5: viết câu SQL GROUP BY store_id với 2 cột transactions, revenue_vnd; ORDER BY revenue_vnd DESC
    return spark.sql("SELECT store_id, 0 AS transactions, 0 AS revenue_vnd FROM silver_pos LIMIT 0")


def main() -> None:
    p = base_parser(__doc__)
    p.set_defaults(lake="/home/jovyan/work/lake-local", date="2026-02-15", master="local[*]")
    args = p.parse_args()

    spark = build_session("wordcount_to_revenue", args.master)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    df = spark.read.parquet(layer_path(args.lake, "silver", "pos_transactions", args.date))
    print(
        f"rows={df.count()} partitions={df.rdd.getNumPartitions()}"
    )  # count() là ACTION → job đầu tiên

    print("\n=== 1. RDD ===")
    rdd_result = revenue_rdd(df)
    for store_id, revenue in rdd_result[:5]:
        print(f"{store_id}  {revenue:>12,}")

    print("\n=== 2. DataFrame API ===")
    df_result = revenue_dataframe(df)
    df_result.show(5)
    df_result.explain()  # tìm 'Exchange hashpartitioning' = shuffle

    print("\n=== 3. Spark SQL ===")
    sql_result = revenue_sql(spark, df)
    sql_result.show(5)

    # TODO 6: kiểm tra 3 cách khớp nhau: tổng revenue và số store bằng nhau (dùng assert)
    spark.stop()


if __name__ == "__main__":
    main()
