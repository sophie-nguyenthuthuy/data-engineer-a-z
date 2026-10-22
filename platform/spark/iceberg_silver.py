"""Video #17 — silver as Iceberg tables: MERGE INTO (upsert), snapshots, time travel.

    spark-submit iceberg_silver.py --date 2026-02-15 --lake s3a://lake
Catalog `lake` (Iceberg, hadoop, warehouse s3a://warehouse/) is configured in spark-defaults.conf.
"""

from __future__ import annotations

from common import base_parser, build_session, layer_path


def main() -> None:
    args = base_parser(__doc__).parse_args()
    spark = build_session("iceberg_silver", args.master)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    incoming = spark.read.parquet(layer_path(args.lake, "silver", "pos_transactions", args.date))
    incoming.createOrReplaceTempView("incoming")

    if not spark.catalog.tableExists("lake.silver.pos_transactions"):
        spark.sql(
            "CREATE TABLE lake.silver.pos_transactions USING iceberg PARTITIONED BY (days(ts)) AS SELECT * FROM incoming WHERE 1 = 0"
        )

    # Upsert: cùng transaction_id → cập nhật; mới → thêm. Chạy lại cùng ngày không sinh duplicate.
    spark.sql(
        """
        MERGE INTO lake.silver.pos_transactions t
        USING incoming s
        ON t.transaction_id = s.transaction_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.sql(
        "SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added FROM lake.silver.pos_transactions.snapshots ORDER BY committed_at"
    ).show(truncate=False)
    print(
        "Time travel: SELECT count(*) FROM lake.silver.pos_transactions VERSION AS OF <snapshot_id>"
    )
    spark.stop()


if __name__ == "__main__":
    main()
