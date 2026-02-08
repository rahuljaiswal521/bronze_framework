"""Core ingestion engine: read -> quality check -> write (CDC merge) -> audit -> cleanup."""

from __future__ import annotations

import traceback
import uuid
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from bronze_framework.audit.audit_logger import AuditLogger
from bronze_framework.config.models import (
    LoadType,
    SchemaEvolutionMode,
    SourceConfig,
    SourceType,
)
from bronze_framework.quality.dead_letter import DeadLetterHandler
from bronze_framework.readers.reader_factory import create_reader
from bronze_framework.utils.file_cleanup import LandingFileCleanup
from bronze_framework.utils.logging import get_logger
from bronze_framework.utils.notifications import send_failure_alert
from bronze_framework.writers.delta_writer import DeltaWriter

logger = get_logger(__name__)


class IngestionResult:
    """Result of an ingestion run."""

    def __init__(
        self,
        source_name: str,
        success: bool,
        records_read: int = 0,
        records_written: int = 0,
        records_quarantined: int = 0,
        error: Optional[str] = None,
    ):
        self.source_name = source_name
        self.success = success
        self.records_read = records_read
        self.records_written = records_written
        self.records_quarantined = records_quarantined
        self.error = error


class IngestionEngine:
    """Orchestrates the ingestion pipeline for a single source."""

    def __init__(
        self,
        spark: SparkSession,
        environment: str = "dev",
        run_id: Optional[str] = None,
    ):
        self.spark = spark
        self.environment = environment
        self.run_id = run_id or str(uuid.uuid4())

    def ingest(self, config: SourceConfig) -> IngestionResult:
        """Execute the full ingestion pipeline for a source config.

        Pipeline: read -> quality check -> write -> audit log.
        """
        start_time = datetime.utcnow()
        audit_logger = AuditLogger(self.spark, config)
        records_read = 0
        records_written = 0
        records_quarantined = 0
        watermark_value: Optional[str] = None

        try:
            logger.info(f"Starting ingestion for source: {config.name}")

            # Step 1: Read
            reader = create_reader(self.spark, config)
            df = reader.read()

            # Step 2: Handle streaming vs batch
            is_streaming = self._is_streaming(config, df)

            if is_streaming:
                return self._ingest_streaming(
                    config, df, audit_logger, start_time
                )

            # Step 3: Quality check (batch only)
            records_read = df.count()
            logger.info(f"Read {records_read} records from {config.name}")

            dead_letter = DeadLetterHandler(self.spark, config)
            quality_result = dead_letter.check_quality(df)

            if quality_result.threshold_exceeded:
                raise RuntimeError(
                    f"Bad record threshold exceeded: "
                    f"{quality_result.bad_pct:.1f}% > "
                    f"{config.target.quality.quarantine_threshold_pct}%"
                )

            # Step 4: Quarantine bad records
            if quality_result.bad_count > 0:
                records_quarantined = dead_letter.quarantine(
                    quality_result.bad_df
                )

            # Step 5: Write good records
            writer = DeltaWriter(self.spark, config)
            records_written = writer.write_batch(quality_result.good_df)

            # Step 6: Compute watermark
            if (
                config.extract.load_type == LoadType.INCREMENTAL
                and config.extract.watermark
            ):
                watermark_value = self._compute_watermark(
                    quality_result.good_df, config.extract.watermark.column
                )

            # Step 7: Audit log
            end_time = datetime.utcnow()
            record = AuditLogger.create_record(
                config=config,
                status="SUCCESS",
                start_time=start_time,
                end_time=end_time,
                records_read=records_read,
                records_written=records_written,
                records_quarantined=records_quarantined,
                watermark_value=watermark_value,
                run_id=self.run_id,
                environment=self.environment,
            )
            audit_logger.log(record)

            # Step 8: Landing file cleanup
            self._cleanup_landing_files(config)

            logger.info(
                f"Ingestion complete for {config.name}: "
                f"read={records_read}, written={records_written}, "
                f"quarantined={records_quarantined}"
            )

            return IngestionResult(
                source_name=config.name,
                success=True,
                records_read=records_read,
                records_written=records_written,
                records_quarantined=records_quarantined,
            )

        except Exception as e:
            end_time = datetime.utcnow()
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(
                f"Ingestion failed for {config.name}: {error_msg}\n"
                f"{traceback.format_exc()}"
            )

            try:
                record = AuditLogger.create_record(
                    config=config,
                    status="FAILURE",
                    start_time=start_time,
                    end_time=end_time,
                    records_read=records_read,
                    records_written=records_written,
                    records_quarantined=records_quarantined,
                    error_message=error_msg[:4000],
                    run_id=self.run_id,
                    environment=self.environment,
                )
                audit_logger.log(record)
            except Exception as audit_err:
                logger.error(f"Failed to write audit log: {audit_err}")

            send_failure_alert(config.name, error_msg, self.environment)

            return IngestionResult(
                source_name=config.name,
                success=False,
                records_read=records_read,
                records_written=records_written,
                records_quarantined=records_quarantined,
                error=error_msg,
            )

    def _ingest_streaming(
        self,
        config: SourceConfig,
        df: DataFrame,
        audit_logger: AuditLogger,
        start_time: datetime,
    ) -> IngestionResult:
        """Handle streaming ingestion with availableNow trigger."""
        writer = DeltaWriter(self.spark, config)
        query = writer.write_stream(df)

        end_time = datetime.utcnow()
        record = AuditLogger.create_record(
            config=config,
            status="SUCCESS",
            start_time=start_time,
            end_time=end_time,
            run_id=self.run_id,
            environment=self.environment,
        )
        audit_logger.log(record)

        logger.info(f"Streaming ingestion complete for {config.name}")

        return IngestionResult(
            source_name=config.name,
            success=True,
        )

    def _is_streaming(self, config: SourceConfig, df: DataFrame) -> bool:
        """Determine if this is a streaming pipeline."""
        return df.isStreaming

    def _compute_watermark(self, df: DataFrame, watermark_col: str) -> Optional[str]:
        """Compute the max watermark value from the ingested data."""
        try:
            if watermark_col not in df.columns:
                return None
            row = df.agg({watermark_col: "max"}).first()
            if row:
                max_val = row[0]
                return str(max_val) if max_val is not None else None
        except Exception as e:
            logger.warning(f"Could not compute watermark: {e}")
        return None

    def _cleanup_landing_files(self, config: SourceConfig) -> None:
        """Run landing file cleanup if configured."""
        landing = config.target.landing
        if not landing.cleanup_enabled:
            return
        try:
            cleaner = LandingFileCleanup(self.spark)
            deleted = cleaner.cleanup(config)
            if deleted > 0:
                logger.info(
                    f"Landing cleanup for {config.name}: {deleted} files deleted"
                )
        except Exception as e:
            # Cleanup failure should not fail the ingestion
            logger.warning(f"Landing file cleanup failed for {config.name}: {e}")
