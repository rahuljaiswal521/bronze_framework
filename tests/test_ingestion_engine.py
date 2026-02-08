"""Tests for ingestion engine."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from bronze_framework.config.models import LoadType
from bronze_framework.engine.ingestion_engine import IngestionEngine, IngestionResult


class TestIngestionEngine:
    """Tests for IngestionEngine class."""

    @patch("bronze_framework.engine.ingestion_engine.AuditLogger")
    @patch("bronze_framework.engine.ingestion_engine.DeltaWriter")
    @patch("bronze_framework.engine.ingestion_engine.DeadLetterHandler")
    @patch("bronze_framework.engine.ingestion_engine.create_reader")
    def test_successful_batch_ingestion(
        self,
        mock_create_reader,
        mock_dead_letter_cls,
        mock_writer_cls,
        mock_audit_cls,
        sample_jdbc_config,
    ):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.isStreaming = False
        mock_df.count.return_value = 100

        mock_reader = MagicMock()
        mock_reader.read.return_value = mock_df
        mock_create_reader.return_value = mock_reader

        mock_quality_result = MagicMock()
        mock_quality_result.threshold_exceeded = False
        mock_quality_result.bad_count = 0
        mock_quality_result.good_df = mock_df
        mock_quality_result.bad_df = MagicMock()
        mock_dead_letter = MagicMock()
        mock_dead_letter.check_quality.return_value = mock_quality_result
        mock_dead_letter_cls.return_value = mock_dead_letter

        mock_writer = MagicMock()
        mock_writer.write_batch.return_value = 100
        mock_writer_cls.return_value = mock_writer

        engine = IngestionEngine(mock_spark, environment="dev")
        result = engine.ingest(sample_jdbc_config)

        assert result.success is True
        assert result.records_read == 100
        assert result.records_written == 100

    @patch("bronze_framework.engine.ingestion_engine.send_failure_alert")
    @patch("bronze_framework.engine.ingestion_engine.AuditLogger")
    @patch("bronze_framework.engine.ingestion_engine.create_reader")
    def test_failed_ingestion_logs_error(
        self,
        mock_create_reader,
        mock_audit_cls,
        mock_alert,
        sample_jdbc_config,
    ):
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_reader.read.side_effect = RuntimeError("Connection refused")
        mock_create_reader.return_value = mock_reader

        engine = IngestionEngine(mock_spark, environment="dev")
        result = engine.ingest(sample_jdbc_config)

        assert result.success is False
        assert "Connection refused" in result.error

    @patch("bronze_framework.engine.ingestion_engine.send_failure_alert")
    @patch("bronze_framework.engine.ingestion_engine.AuditLogger")
    @patch("bronze_framework.engine.ingestion_engine.DeltaWriter")
    @patch("bronze_framework.engine.ingestion_engine.DeadLetterHandler")
    @patch("bronze_framework.engine.ingestion_engine.create_reader")
    def test_threshold_exceeded_fails(
        self,
        mock_create_reader,
        mock_dead_letter_cls,
        mock_writer_cls,
        mock_audit_cls,
        mock_alert,
        sample_jdbc_config,
    ):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.isStreaming = False
        mock_df.count.return_value = 100

        mock_reader = MagicMock()
        mock_reader.read.return_value = mock_df
        mock_create_reader.return_value = mock_reader

        mock_quality_result = MagicMock()
        mock_quality_result.threshold_exceeded = True
        mock_quality_result.bad_pct = 15.0
        mock_dead_letter = MagicMock()
        mock_dead_letter.check_quality.return_value = mock_quality_result
        mock_dead_letter_cls.return_value = mock_dead_letter

        engine = IngestionEngine(mock_spark, environment="dev")
        result = engine.ingest(sample_jdbc_config)

        assert result.success is False
        assert "threshold" in result.error.lower()

    @patch("bronze_framework.engine.ingestion_engine.AuditLogger")
    @patch("bronze_framework.engine.ingestion_engine.DeltaWriter")
    @patch("bronze_framework.engine.ingestion_engine.create_reader")
    def test_streaming_ingestion(
        self,
        mock_create_reader,
        mock_writer_cls,
        mock_audit_cls,
        sample_file_config,
    ):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.isStreaming = True
        mock_df.withColumn.return_value = mock_df

        mock_reader = MagicMock()
        mock_reader.read.return_value = mock_df
        mock_create_reader.return_value = mock_reader

        mock_writer = MagicMock()
        mock_writer.write_stream.return_value = MagicMock()
        mock_writer_cls.return_value = mock_writer

        engine = IngestionEngine(mock_spark, environment="dev")
        result = engine.ingest(sample_file_config)

        assert result.success is True
        mock_writer.write_stream.assert_called_once()


class TestIngestionResult:
    """Tests for IngestionResult dataclass."""

    def test_success_result(self):
        result = IngestionResult(
            source_name="test",
            success=True,
            records_read=100,
            records_written=95,
            records_quarantined=5,
        )
        assert result.success is True
        assert result.error is None

    def test_failure_result(self):
        result = IngestionResult(
            source_name="test",
            success=False,
            error="Connection timeout",
        )
        assert result.success is False
        assert result.error == "Connection timeout"
