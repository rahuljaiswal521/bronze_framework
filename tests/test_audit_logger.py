"""Tests for audit logger."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from bronze_framework.audit.audit_logger import AuditLogger, AuditRecord


class TestAuditRecord:
    """Tests for AuditRecord dataclass."""

    def test_create_record(self, sample_jdbc_config):
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 1, 0, 5, 0)

        record = AuditLogger.create_record(
            config=sample_jdbc_config,
            status="SUCCESS",
            start_time=start,
            end_time=end,
            records_read=1000,
            records_written=950,
            records_quarantined=50,
            watermark_value="2024-01-01T00:00:00",
            run_id="test-run-123",
            environment="dev",
        )

        assert record.source_name == "test_orders"
        assert record.source_type == "jdbc"
        assert record.target_table == "test_catalog.bronze.test_orders"
        assert record.status == "SUCCESS"
        assert record.records_read == 1000
        assert record.records_written == 950
        assert record.records_quarantined == 50
        assert record.watermark_value == "2024-01-01T00:00:00"
        assert record.run_id == "test-run-123"
        assert record.environment == "dev"
        assert record.load_type == "incremental"

    def test_create_failure_record(self, sample_jdbc_config):
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 1, 0, 0, 30)

        record = AuditLogger.create_record(
            config=sample_jdbc_config,
            status="FAILURE",
            start_time=start,
            end_time=end,
            error_message="Connection refused",
        )

        assert record.status == "FAILURE"
        assert record.error_message == "Connection refused"
        assert record.records_read == 0


class TestAuditLogger:
    """Tests for AuditLogger class."""

    def test_ensure_table_exists(self, sample_jdbc_config):
        mock_spark = MagicMock()
        logger = AuditLogger(mock_spark, sample_jdbc_config)
        logger.ensure_table_exists()

        assert mock_spark.sql.call_count == 2
        calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert any("CREATE SCHEMA" in c for c in calls)
        assert any("CREATE TABLE" in c for c in calls)

    def test_log_writes_to_delta(self, sample_jdbc_config):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df

        logger = AuditLogger(mock_spark, sample_jdbc_config)
        record = AuditRecord(
            source_name="test",
            source_type="jdbc",
            target_table="cat.schema.table",
            status="SUCCESS",
            start_time="2024-01-01T00:00:00",
            end_time="2024-01-01T00:05:00",
            records_read=100,
            records_written=100,
        )

        logger.log(record)
        mock_df.write.format.assert_called_with("delta")

    def test_audit_table_name(self, sample_jdbc_config):
        mock_spark = MagicMock()
        logger = AuditLogger(mock_spark, sample_jdbc_config)
        assert logger.audit_table == "test_catalog.bronze_meta.ingestion_audit_log"
