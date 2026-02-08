"""Audit logging for ingestion runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

from bronze_framework.config.models import SourceConfig
from bronze_framework.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class AuditRecord:
    """Single audit log entry for an ingestion run."""
    source_name: str
    source_type: str
    target_table: str
    status: str  # SUCCESS, FAILURE
    start_time: str
    end_time: str
    records_read: int = 0
    records_written: int = 0
    records_quarantined: int = 0
    watermark_value: Optional[str] = None
    error_message: Optional[str] = None
    load_type: str = "full"
    run_id: str = ""
    environment: str = ""


class AuditLogger:
    """Writes audit records to a Delta table in Unity Catalog."""

    DDL = """
    CREATE TABLE IF NOT EXISTS {table} (
        source_name STRING,
        source_type STRING,
        target_table STRING,
        status STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        records_read LONG,
        records_written LONG,
        records_quarantined LONG,
        watermark_value STRING,
        error_message STRING,
        load_type STRING,
        run_id STRING,
        environment STRING
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """

    def __init__(self, spark: SparkSession, config: SourceConfig):
        self.spark = spark
        self.config = config
        self.audit_table = config.audit_table_name

    def ensure_table_exists(self) -> None:
        """Create the audit table if it doesn't exist."""
        catalog = self.config.target.catalog
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze_meta")
        self.spark.sql(self.DDL.format(table=self.audit_table))
        logger.info(f"Audit table ensured: {self.audit_table}")

    def log(self, record: AuditRecord) -> None:
        """Write an audit record to the audit table."""
        self.ensure_table_exists()

        record_dict = asdict(record)
        df = self.spark.createDataFrame([record_dict])

        df.write.format("delta").mode("append").saveAsTable(self.audit_table)

        logger.info(
            f"Audit log written: source={record.source_name}, "
            f"status={record.status}, "
            f"rows_read={record.records_read}, "
            f"rows_written={record.records_written}"
        )

    @staticmethod
    def create_record(
        config: SourceConfig,
        status: str,
        start_time: datetime,
        end_time: datetime,
        records_read: int = 0,
        records_written: int = 0,
        records_quarantined: int = 0,
        watermark_value: Optional[str] = None,
        error_message: Optional[str] = None,
        run_id: str = "",
        environment: str = "",
    ) -> AuditRecord:
        """Factory method to create an AuditRecord from a SourceConfig."""
        return AuditRecord(
            source_name=config.name,
            source_type=config.source_type.value,
            target_table=config.full_table_name,
            status=status,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            records_read=records_read,
            records_written=records_written,
            records_quarantined=records_quarantined,
            watermark_value=watermark_value,
            error_message=error_message,
            load_type=config.extract.load_type.value,
            run_id=run_id,
            environment=environment,
        )
