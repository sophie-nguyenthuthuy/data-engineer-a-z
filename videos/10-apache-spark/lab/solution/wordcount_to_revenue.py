"""Lời giải lab #10 — doanh thu theo cửa hàng, viết 3 cách: RDD → DataFrame API → Spark SQL.

CHỈ chạy được nơi có pyspark (Jupyter của repo hoặc container spark) — .venv trên máy KHÔNG có pyspark.
Trong Jupyter (http://localhost:8888, token dea2z) → New → Terminal:
    python work/videos/10-apache-spark/lab/solution/wordcount_to_revenue.py \
        --lake /home/jovyan/work/lake-local --date 2026-02-15

Cả 3 cách phải cho cùng một kết quả: 30 cửa hàng, tổng 76 939 000 VND cho ngày 15/02.
Bài này là "word count" của Data Engineer: thay (word, 1) bằng (store_id, total_amount_vnd).
"""

from __future__ import annotations

import sys
from operator import add
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO / "platform" / "spark"))  # dùng lại common.py của repo

from common import base_parser, build_session, layer_path  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def revenue_rdd(df: DataFrame) -> list[tuple[str, int]]:
    """Cách 1 — RDD: map thành (key, value) rồi reduceByKey. Giống word count, chỉ khác value."""
    pairs = df.rdd.map(lambda row: (row["store_id"], int(row["total_amount_vnd"])))
    totals = pairs.reduceByKey(add)  # shuffle: mọi dòng cùng store_id phải về cùng một partition
    return totals.sortBy(lambda kv: -kv[1]).collect()


def revenue_dataframe(df: DataFrame) -> DataFrame:
    """Cách 2 — DataFrame API: Catalyst tối ưu (partial aggregate trước shuffle), code ngắn hơn, có schema."""
    return (
        df.groupBy("store_id")
        .agg(
            F.count("*").alias("transactions"),
            F.sum("total_amount_vnd").alias("revenue_vnd"),
        )
        .orderBy(F.desc("revenue_vnd"))
    )


def revenue_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Cách 3 — Spark SQL: cùng một plan với cách 2 (xem explain), nhưng người biết SQL đọc được ngay."""
    df.createOrReplaceTempView("silver_pos")
    return spark.sql(
        """
        SELECT store_id,
               count(*)              AS transactions,
               sum(total_amount_vnd) AS revenue_vnd
        FROM silver_pos
        GROUP BY store_id
        ORDER BY revenue_vnd DESC
        """
    )


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
    df_result.explain()  # tìm 'Exchange hashpartitioning' = shuffle, 'HashAggregate' xuất hiện 2 lần = partial + final

    print("\n=== 3. Spark SQL ===")
    sql_result = revenue_sql(spark, df)
    sql_result.show(5)

    # Ba cách phải khớp nhau từng đồng.
    total_rdd = sum(v for _, v in rdd_result)
    total_df = df_result.agg(F.sum("revenue_vnd")).first()[0]
    total_sql = sql_result.agg(F.sum("revenue_vnd")).first()[0]
    assert total_rdd == total_df == total_sql, (total_rdd, total_df, total_sql)
    assert len(rdd_result) == df_result.count() == sql_result.count()
    print(f"\nOK: {len(rdd_result)} stores, total revenue {total_rdd:,} VND (3 cách khớp nhau)")
    spark.stop()


if __name__ == "__main__":
    main()
