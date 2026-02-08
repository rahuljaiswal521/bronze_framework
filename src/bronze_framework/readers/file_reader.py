"""File reader with Auto Loader and batch support."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from bronze_framework.config.models import SourceConfig
from bronze_framework.readers.base_reader import BaseReader


class FileReader(BaseReader):
    """Reads data from cloud storage files (Auto Loader or batch)."""

    def __init__(self, spark: SparkSession, config: SourceConfig):
        super().__init__(spark, config)

    def read(self) -> DataFrame:
        extract = self.config.extract

        if extract.auto_loader:
            return self._read_auto_loader()
        return self._read_batch()

    def _read_auto_loader(self) -> DataFrame:
        """Use Databricks Auto Loader (cloudFiles) for incremental file ingestion."""
        extract = self.config.extract

        options = {
            "cloudFiles.format": extract.format,
            "cloudFiles.schemaLocation": f"{extract.checkpoint_path}_schema/",
        }
        options.update(extract.format_options)

        self.logger.info(
            f"Reading files via Auto Loader: {extract.path} "
            f"(format={extract.format})"
        )

        return (
            self.spark.readStream
            .format("cloudFiles")
            .options(**options)
            .load(extract.path)
        )

    def _read_batch(self) -> DataFrame:
        """Standard batch read for full loads."""
        extract = self.config.extract

        reader = self.spark.read.format(extract.format)

        if extract.format_options:
            reader = reader.options(**extract.format_options)

        self.logger.info(
            f"Reading files in batch mode: {extract.path} "
            f"(format={extract.format})"
        )

        return reader.load(extract.path)
