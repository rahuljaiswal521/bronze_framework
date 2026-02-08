"""Tests for JDBC reader."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from bronze_framework.config.models import LoadType, SourceConfig
from bronze_framework.readers.jdbc_reader import JDBCReader


class TestJDBCReader:
    """Tests for JDBCReader class."""

    @patch("bronze_framework.readers.jdbc_reader.get_secret")
    def test_build_jdbc_url(self, mock_secret, sample_jdbc_config):
        mock_spark = MagicMock()
        reader = JDBCReader(mock_spark, sample_jdbc_config)
        url = reader._build_jdbc_url()
        assert "jdbc:sqlserver://localhost:5432" in url
        assert "databaseName=testdb" in url

    @patch("bronze_framework.readers.jdbc_reader.get_secret")
    def test_build_query_full_load(self, mock_secret, sample_jdbc_config):
        sample_jdbc_config.extract.load_type = LoadType.FULL
        mock_spark = MagicMock()
        reader = JDBCReader(mock_spark, sample_jdbc_config)
        query = reader._build_query()
        assert query == "SELECT * FROM public.orders"

    @patch("bronze_framework.readers.jdbc_reader.get_secret")
    def test_build_query_incremental_with_watermark(
        self, mock_secret, sample_jdbc_config
    ):
        mock_spark = MagicMock()
        # Simulate no audit table
        mock_spark.catalog.tableExists.return_value = False
        reader = JDBCReader(mock_spark, sample_jdbc_config)
        query = reader._build_query()
        assert "updated_at > '2020-01-01T00:00:00'" in query

    @patch("bronze_framework.readers.jdbc_reader.get_secret")
    def test_build_query_incremental_no_watermark_config(
        self, mock_secret, sample_jdbc_config
    ):
        sample_jdbc_config.extract.watermark = None
        mock_spark = MagicMock()
        reader = JDBCReader(mock_spark, sample_jdbc_config)
        query = reader._build_query()
        assert query == "SELECT * FROM public.orders"

    @patch("bronze_framework.readers.jdbc_reader.get_secret")
    def test_read_calls_spark_jdbc(self, mock_secret, sample_jdbc_config):
        mock_secret.return_value = "test_value"
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False

        mock_reader = MagicMock()
        mock_spark.read.format.return_value.options.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = MagicMock()

        reader = JDBCReader(mock_spark, sample_jdbc_config)
        reader.read()

        mock_spark.read.format.assert_called_with("jdbc")

    @patch("bronze_framework.readers.jdbc_reader.get_secret")
    def test_custom_jdbc_url(self, mock_secret, sample_jdbc_config):
        sample_jdbc_config.connection.url = "jdbc:postgresql://custom:5432/db"
        mock_secret.return_value = "test_value"
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False

        mock_reader = MagicMock()
        mock_spark.read.format.return_value.options.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = MagicMock()

        reader = JDBCReader(mock_spark, sample_jdbc_config)
        reader.read()

        call_kwargs = mock_spark.read.format.return_value.options.call_args
        assert call_kwargs[1]["url"] == "jdbc:postgresql://custom:5432/db"
