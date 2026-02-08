"""JDBC reader with full and incremental load support."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from bronze_framework.config.models import LoadType, SourceConfig
from bronze_framework.readers.base_reader import BaseReader
from bronze_framework.utils.secrets import get_secret


class JDBCReader(BaseReader):
    """Reads data from JDBC sources (SQL Server, Oracle, PostgreSQL, etc.)."""

    def __init__(self, spark: SparkSession, config: SourceConfig):
        super().__init__(spark, config)

    def read(self) -> DataFrame:
        conn = self.config.connection
        extract = self.config.extract

        jdbc_url = conn.url or self._build_jdbc_url()
        username = get_secret(self.spark, conn.secret_scope, conn.secret_key_user)
        password = get_secret(self.spark, conn.secret_scope, conn.secret_key_password)

        jdbc_options = {
            "url": jdbc_url,
            "user": username,
            "password": password,
            "driver": conn.driver or "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "fetchsize": str(extract.fetch_size),
        }
        jdbc_options.update(conn.properties)

        if extract.query:
            query = extract.query
        else:
            query = self._build_query()

        jdbc_options["dbtable"] = f"({query}) AS src"

        reader = self.spark.read.format("jdbc").options(**jdbc_options)

        if extract.partition_column and extract.num_partitions > 1:
            bounds = self._get_partition_bounds(jdbc_options, extract.table)
            if bounds:
                reader = reader.option("partitionColumn", extract.partition_column)
                reader = reader.option("lowerBound", str(bounds[0]))
                reader = reader.option("upperBound", str(bounds[1]))
                reader = reader.option("numPartitions", str(extract.num_partitions))

        self.logger.info(
            f"Reading JDBC source: {self.config.name} "
            f"(load_type={extract.load_type.value})"
        )
        return reader.load()

    def _build_jdbc_url(self) -> str:
        conn = self.config.connection
        return (
            f"jdbc:sqlserver://{conn.host}:{conn.port};"
            f"databaseName={conn.database}"
        )

    def _build_query(self) -> str:
        extract = self.config.extract
        table = extract.table

        if extract.load_type == LoadType.INCREMENTAL and extract.watermark:
            watermark_value = self._get_watermark_value()
            if watermark_value:
                wm_col = extract.watermark.column
                self.logger.info(
                    f"Incremental load: {wm_col} > '{watermark_value}'"
                )
                return f"SELECT * FROM {table} WHERE {wm_col} > '{watermark_value}'"

        return f"SELECT * FROM {table}"

    def _get_partition_bounds(self, jdbc_options: dict, table: str) -> tuple | None:
        """Query source for min/max of partition column."""
        partition_col = self.config.extract.partition_column
        try:
            bounds_query = (
                f"(SELECT MIN({partition_col}) AS min_val, "
                f"MAX({partition_col}) AS max_val FROM {table}) AS bounds"
            )
            bounds_df = (
                self.spark.read.format("jdbc")
                .options(**{**jdbc_options, "dbtable": bounds_query})
                .load()
            )
            row = bounds_df.first()
            if row and row["min_val"] is not None:
                return (row["min_val"], row["max_val"])
        except Exception as e:
            self.logger.warning(f"Could not determine partition bounds: {e}")
        return None
