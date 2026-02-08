"""Factory pattern to instantiate the appropriate reader."""

from __future__ import annotations

from pyspark.sql import SparkSession

from bronze_framework.config.models import SourceConfig, SourceType
from bronze_framework.readers.api_reader import APIReader
from bronze_framework.readers.base_reader import BaseReader
from bronze_framework.readers.file_reader import FileReader
from bronze_framework.readers.jdbc_reader import JDBCReader
from bronze_framework.readers.stream_reader import StreamReader

_READER_MAP = {
    SourceType.JDBC: JDBCReader,
    SourceType.FILE: FileReader,
    SourceType.API: APIReader,
    SourceType.STREAM: StreamReader,
}


def create_reader(spark: SparkSession, config: SourceConfig) -> BaseReader:
    """Create the appropriate reader instance based on source type.

    Raises:
        ValueError: If the source type is not supported.
    """
    reader_cls = _READER_MAP.get(config.source_type)
    if reader_cls is None:
        raise ValueError(
            f"Unsupported source type: {config.source_type}. "
            f"Supported types: {list(_READER_MAP.keys())}"
        )
    return reader_cls(spark, config)
