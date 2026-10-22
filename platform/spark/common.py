"""Shared helpers for the PySpark jobs. Keep this file tiny — learners copy it into notebooks."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def build_session(app_name: str, master: str | None = None) -> SparkSession:
    """Cluster when SPARK_MASTER_URL / --master is given, otherwise local[*] (Jupyter, CI)."""
    b = SparkSession.builder.appName(app_name)
    if master:
        b = b.master(master)
    # S3A/Iceberg settings come from spark-defaults.conf in the image; nothing to repeat here.
    return b.getOrCreate()


def base_parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--lake", default="s3a://lake", help="s3a://lake or /path/to/lake-local")
    p.add_argument("--date", required=False, help="business date YYYY-MM-DD")
    p.add_argument("--master", default=None, help="spark://spark-master:7077 or local[*]")
    return p


def layer_path(lake: str, layer: str, table: str, dt: str | None = None) -> str:
    p = f"{lake.rstrip('/')}/{layer}/{table}"
    return f"{p}/dt={dt}" if dt else p
