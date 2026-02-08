"""Tests for file reader."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from bronze_framework.readers.file_reader import FileReader


class TestFileReader:
    """Tests for FileReader class."""

    def test_read_auto_loader_uses_readstream(self, sample_file_config):
        mock_spark = MagicMock()
        mock_stream = MagicMock()
        mock_spark.readStream.format.return_value.options.return_value.load.return_value = (
            mock_stream
        )

        reader = FileReader(mock_spark, sample_file_config)
        result = reader.read()

        mock_spark.readStream.format.assert_called_with("cloudFiles")

    def test_read_batch_uses_read(self, sample_file_config):
        sample_file_config.extract.auto_loader = False
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = MagicMock()

        reader = FileReader(mock_spark, sample_file_config)
        result = reader.read()

        mock_spark.read.format.assert_called_with("json")

    def test_auto_loader_sets_schema_location(self, sample_file_config):
        mock_spark = MagicMock()
        mock_options = MagicMock()
        mock_spark.readStream.format.return_value.options.return_value = mock_options
        mock_options.load.return_value = MagicMock()

        reader = FileReader(mock_spark, sample_file_config)
        reader._read_auto_loader()

        call_kwargs = mock_spark.readStream.format.return_value.options.call_args
        options = call_kwargs[1]
        assert "cloudFiles.schemaLocation" in options

    def test_batch_passes_format_options(self, sample_file_config):
        sample_file_config.extract.auto_loader = False
        sample_file_config.extract.format_options = {"multiLine": "true"}

        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = MagicMock()

        reader = FileReader(mock_spark, sample_file_config)
        reader.read()

        mock_reader.options.assert_called_with(multiLine="true")

    def test_read_loads_from_correct_path(self, sample_file_config):
        sample_file_config.extract.auto_loader = False
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = MagicMock()

        reader = FileReader(mock_spark, sample_file_config)
        reader.read()

        mock_reader.load.assert_called_with("/tmp/storage/clickstream/")
