"""Tiny abstraction over "where the lake lives": a local folder or s3://bucket/prefix (MinIO or AWS).

Lab phần 2 dùng thư mục local (./lake-local) để bạn mở file Parquet ra xem.
Đổi sang MinIO chỉ cần: --lake s3://lake  (endpoint lấy từ biến môi trường S3_ENDPOINT).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

LAYERS = ("bronze", "silver", "gold")


@dataclass(frozen=True)
class Lake:
    root: str  # "./lake-local" or "s3://lake"

    @property
    def is_s3(self) -> bool:
        return self.root.startswith("s3://")

    def path(
        self, layer: str, table: str, partition: str | None = None, filename: str | None = None
    ) -> str:
        """bronze/pos_transactions/dt=2026-02-16/part-000.parquet — Hive-style partition folders."""
        assert layer in LAYERS, layer
        parts = [self.root.rstrip("/"), layer, table]
        if partition:
            parts.append(partition)
        if filename:
            parts.append(filename)
        return "/".join(parts)

    def _fs(self):
        if not self.is_s3:
            return None
        from pyarrow import fs

        endpoint = os.environ.get("S3_ENDPOINT")  # http://localhost:9000 for MinIO
        return fs.S3FileSystem(
            endpoint_override=endpoint,
            access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
            secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            scheme="http" if endpoint and endpoint.startswith("http://") else "https",
            region=os.environ.get("AWS_REGION", "us-east-1"),
        )

    def _strip(self, uri: str) -> str:
        return uri.removeprefix("s3://")

    def write_parquet(
        self,
        df: pl.DataFrame,
        layer: str,
        table: str,
        partition: str,
        filename: str = "part-000.parquet",
    ) -> str:
        uri = self.path(layer, table, partition, filename)
        if self.is_s3:
            pq.write_table(
                df.to_arrow(), self._strip(uri), filesystem=self._fs(), compression="zstd"
            )
        else:
            Path(uri).parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(uri, compression="zstd")
        self._remove_other_files(layer, table, partition, keep=uri)
        return uri

    def _remove_other_files(self, layer: str, table: str, partition: str, keep: str) -> None:
        """Overwrite = sau khi ghi file mới, xoá mọi file khác còn lại trong partition.

        Chỉ ghi đè `part-000.parquet` là chưa đủ: Spark (#10) đặt tên `part-00000-<uuid>.parquet` + `_SUCCESS`,
        nên hai writer luân phiên sẽ để lại hai bản của cùng dữ liệu → dbt báo duplicate. Partition là đơn vị
        ghi đè, không phải file. Ghi trước rồi xoá sau để không có lúc nào partition trống.
        """
        prefix = self.path(layer, table, partition)
        if self.is_s3:
            from pyarrow import fs as pafs

            fs = self._fs()
            # từng object một: MinIO từ chối DeleteObjects (bulk) thiếu header Content-MD5 mà pyarrow không gửi
            for info in fs.get_file_info(pafs.FileSelector(self._strip(prefix), recursive=True)):
                if info.type == pafs.FileType.File and info.path != self._strip(keep):
                    fs.delete_file(info.path)
            return
        for f in Path(prefix).iterdir():
            if f.is_file() and str(f) != keep:
                f.unlink()

    def read_parquet(self, layer: str, table: str, partition: str | None = None) -> pl.DataFrame:
        uri = self.path(layer, table, partition)
        if self.is_s3:
            table_ = pq.read_table(self._strip(uri), filesystem=self._fs())
            return pl.from_arrow(table_)
        files = sorted(Path(uri).rglob("*.parquet"))
        if not files:
            return pl.DataFrame()
        return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")

    def exists(self, layer: str, table: str, partition: str) -> bool:
        uri = self.path(layer, table, partition)
        if self.is_s3:
            info = self._fs().get_file_info(self._strip(uri))
            return info.type != pa.fs.FileType.NotFound
        return Path(uri).exists()
