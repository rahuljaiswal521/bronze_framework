"""Abstract base reader with watermark lookup."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from bronze_framework.config.models import SourceConfig
from bronze_framework.utils.logging import get_logger


class BaseReader(ABC):
    """Base class for all data source readers."""

    def __init__(self, spark: SparkSession, config: SourceConfig):
        self.spark = spark
        self.config = config
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def read(self) -> DataFrame:
        """Read data from the source and return a DataFrame."""
        ...

    def get_current_watermark(self) -> Optional[str]:
        """Query the audit table for the last successful watermark value.

        Returns None if no prior successful ingestion exists (first load).
        """
        audit_table = self.config.audit_table_name
        source_name = self.config.name

        try:
            if not self.spark.catalog.tableExists(audit_table):
                self.logger.info(
                    f"Audit table {audit_table} does not exist; treating as first load"
                )
                return None

            result = self.spark.sql(f"""
                SELECT MAX(watermark_value) AS last_watermark
                FROM {audit_table}
                WHERE source_name = '{source_name}'
                  AND status = 'SUCCESS'
                  AND watermark_value IS NOT NULL
            """)
            row = result.first()
            if row and row["last_watermark"]:
                self.logger.info(
                    f"Found watermark for {source_name}: {row['last_watermark']}"
                )
                return str(row["last_watermark"])
        except Exception as e:
            self.logger.warning(
                f"Could not read watermark from audit table: {e}; treating as first load"
            )

        return None

    def _get_watermark_value(self) -> Optional[str]:
        """Determine the watermark value to use for incremental read.

        Priority: audit table > config default > None (full load).
        """
        wm = self.config.extract.watermark
        if wm is None:
            return None

        current = self.get_current_watermark()
        if current is not None:
            return current

        if wm.default_value:
            self.logger.info(
                f"Using default watermark for {self.config.name}: {wm.default_value}"
            )
            return wm.default_value

        return None
