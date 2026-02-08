"""Tests for Delta writer."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from bronze_framework.config.models import (
    CdcConfig,
    CdcMode,
    MetadataColumn,
    SchemaEvolutionConfig,
    SchemaEvolutionMode,
)
from bronze_framework.writers.delta_writer import DeltaWriter, SCD2_COLUMNS

# Patch all PySpark functions used in _add_metadata_columns
_PYSPARK_PATCHES = {
    "bronze_framework.writers.delta_writer.current_timestamp": MagicMock,
    "bronze_framework.writers.delta_writer.current_date": MagicMock,
    "bronze_framework.writers.delta_writer.input_file_name": MagicMock,
    "bronze_framework.writers.delta_writer.lit": MagicMock,
    "bronze_framework.writers.delta_writer.col": MagicMock,
    "bronze_framework.writers.delta_writer.expr": MagicMock,
}


class TestDeltaWriter:
    """Tests for DeltaWriter class."""

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(return_value="lit_mock"),
                    col=MagicMock(return_value="col_mock"),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"))
    def test_add_metadata_columns_current_timestamp(self, sample_jdbc_config):
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_jdbc_config)
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df

        result = writer._add_metadata_columns(mock_df)
        assert mock_df.withColumn.call_count == 2

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(return_value="lit_mock"),
                    col=MagicMock(return_value="col_mock"),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"))
    def test_add_metadata_columns_lit(self, sample_jdbc_config):
        sample_jdbc_config.target.metadata_columns = [
            MetadataColumn(name="_source", expression="lit('test_value')"),
        ]
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_jdbc_config)
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df

        result = writer._add_metadata_columns(mock_df)
        mock_df.withColumn.assert_called_once()

    @patch("bronze_framework.writers.delta_writer.lit")
    def test_schema_evolution_rescue_adds_column(self, mock_lit, sample_jdbc_config):
        sample_jdbc_config.target.schema_evolution = SchemaEvolutionConfig(
            mode=SchemaEvolutionMode.RESCUE,
            rescued_data_column="_rescued_data",
        )
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_jdbc_config)
        mock_df = MagicMock()
        mock_df.columns = ["col1", "col2"]  # no _rescued_data
        mock_df.withColumn.return_value = mock_df

        result = writer._apply_schema_evolution_options(mock_df)
        mock_df.withColumn.assert_called_once()

    def test_schema_evolution_rescue_skips_if_exists(self, sample_jdbc_config):
        sample_jdbc_config.target.schema_evolution = SchemaEvolutionConfig(
            mode=SchemaEvolutionMode.RESCUE,
            rescued_data_column="_rescued_data",
        )
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_jdbc_config)
        mock_df = MagicMock()
        mock_df.columns = ["col1", "_rescued_data"]

        result = writer._apply_schema_evolution_options(mock_df)
        mock_df.withColumn.assert_not_called()

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(return_value="lit_mock"),
                    col=MagicMock(return_value="col_mock"),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"))
    def test_write_batch_calls_save_as_table(self, sample_jdbc_config):
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_jdbc_config)

        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = ["col1"]
        mock_df.count.return_value = 100

        mock_writer = MagicMock()
        mock_df.write.format.return_value.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        result = writer.write_batch(mock_df)
        assert result == 100

    def test_write_stream_uses_available_now(self, sample_file_config):
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_file_config)

        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = ["col1"]

        mock_stream_writer = MagicMock()
        mock_df.writeStream.format.return_value = mock_stream_writer
        mock_stream_writer.outputMode.return_value = mock_stream_writer
        mock_stream_writer.trigger.return_value = mock_stream_writer
        mock_stream_writer.option.return_value = mock_stream_writer
        mock_stream_writer.partitionBy.return_value = mock_stream_writer
        mock_stream_writer.toTable.return_value = MagicMock()

        writer.write_stream(mock_df)
        mock_stream_writer.trigger.assert_called_with(availableNow=True)


class TestWriteBatchRouting:
    """Tests that write_batch routes to the correct method based on CDC config."""

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"))
    def test_routes_to_scd2_when_enabled(self, sample_scd2_config):
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_scd2_config)
        writer.write_merge_scd2 = MagicMock(return_value=10)

        mock_df = MagicMock()
        result = writer.write_batch(mock_df)

        writer.write_merge_scd2.assert_called_once_with(mock_df)
        assert result == 10

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"))
    def test_routes_to_upsert_when_enabled(self, sample_upsert_config):
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_upsert_config)
        writer.write_upsert = MagicMock(return_value=5)

        mock_df = MagicMock()
        result = writer.write_batch(mock_df)

        writer.write_upsert.assert_called_once_with(mock_df)
        assert result == 5

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"))
    def test_routes_to_append_when_cdc_disabled(self, sample_jdbc_config):
        """Default config has cdc.enabled=False, should append."""
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_jdbc_config)
        writer._write_append = MagicMock(return_value=20)

        mock_df = MagicMock()
        result = writer.write_batch(mock_df)

        writer._write_append.assert_called_once_with(mock_df)
        assert result == 20

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"))
    def test_routes_to_append_when_cdc_mode_is_append(self, sample_scd2_config):
        """Even with cdc.enabled=True, mode=APPEND should plain-append."""
        sample_scd2_config.target.cdc.mode = CdcMode.APPEND
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_scd2_config)
        writer._write_append = MagicMock(return_value=15)

        mock_df = MagicMock()
        result = writer.write_batch(mock_df)

        writer._write_append.assert_called_once_with(mock_df)
        assert result == 15


class TestWriteMergeScd2:
    """Tests for SCD2 merge logic."""

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"),
                    md5=MagicMock(return_value="hash_mock"),
                    concat_ws=MagicMock(return_value="concat_mock"),
                    coalesce=MagicMock(return_value="coalesce_mock"))
    def test_scd2_raises_without_primary_keys(self, sample_scd2_config):
        sample_scd2_config.target.cdc.primary_keys = []
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_scd2_config)

        with pytest.raises(ValueError, match="primary_keys"):
            writer.write_merge_scd2(MagicMock())

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"),
                    md5=MagicMock(return_value="hash_mock"),
                    concat_ws=MagicMock(return_value="concat_mock"),
                    coalesce=MagicMock(return_value="coalesce_mock"))
    def test_scd2_returns_zero_for_empty_df(self, sample_scd2_config):
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_scd2_config)

        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = ["order_id", "amount", "updated_at"]
        mock_df.count.return_value = 0

        result = writer.write_merge_scd2(mock_df)
        assert result == 0

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"),
                    md5=MagicMock(return_value="hash_mock"),
                    concat_ws=MagicMock(return_value="concat_mock"),
                    coalesce=MagicMock(return_value="coalesce_mock"))
    def test_scd2_initial_load_when_table_missing(self, sample_scd2_config):
        """When target table doesn't exist, should do initial load."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        writer = DeltaWriter(mock_spark, sample_scd2_config)

        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = ["order_id", "amount", "updated_at", "_record_hash"]
        mock_df.count.return_value = 5

        # Mock the write chain for initial load
        mock_writer = MagicMock()
        mock_df.write.format.return_value.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        result = writer.write_merge_scd2(mock_df)

        assert result == 5
        # Should have added SCD2 columns (_effective_from, _effective_to, etc.)
        with_column_calls = [c[0][0] for c in mock_df.withColumn.call_args_list]
        assert "_effective_from" in with_column_calls
        assert "_effective_to" in with_column_calls
        assert "_is_current" in with_column_calls
        assert "_cdc_operation" in with_column_calls

    @patch("pyspark.sql.window.Window")
    @patch("pyspark.sql.functions.row_number")
    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"),
                    md5=MagicMock(return_value="hash_mock"),
                    concat_ws=MagicMock(return_value="concat_mock"),
                    coalesce=MagicMock(return_value="coalesce_mock"))
    def test_scd2_merge_executes_sql_when_table_exists(
        self, mock_row_number, mock_window, sample_scd2_config
    ):
        """When target table exists, should execute MERGE SQL."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        writer = DeltaWriter(mock_spark, sample_scd2_config)

        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = ["order_id", "amount", "updated_at",
                           "_ingest_timestamp", "_source_system", "_record_hash"]
        mock_df.count.return_value = 3
        mock_df.filter.return_value = mock_df
        mock_df.drop.return_value = mock_df

        result = writer.write_merge_scd2(mock_df)

        assert result == 3
        # Should have registered temp view and executed SQL
        mock_df.createOrReplaceTempView.assert_called_once_with("_bronze_incoming")
        # Two SQL calls: MERGE + INSERT for changed records
        assert mock_spark.sql.call_count == 2
        merge_sql = mock_spark.sql.call_args_list[0][0][0]
        assert "MERGE INTO" in merge_sql
        assert "target.order_id = source.order_id" in merge_sql
        assert "_record_hash" in merge_sql
        assert "_is_current" in merge_sql
        # Temp view cleaned up
        mock_spark.catalog.dropTempView.assert_called_once_with("_bronze_incoming")

    @patch("pyspark.sql.window.Window")
    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"),
                    md5=MagicMock(return_value="hash_mock"),
                    concat_ws=MagicMock(return_value="concat_mock"),
                    coalesce=MagicMock(return_value="coalesce_mock"))
    def test_scd2_merge_sql_includes_delete_clause(self, mock_window, sample_scd2_config):
        """When delete condition is set, MERGE SQL should include DELETE clause."""
        sample_scd2_config.target.cdc.delete_condition_column = "is_deleted"
        sample_scd2_config.target.cdc.delete_condition_value = "true"

        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        writer = DeltaWriter(mock_spark, sample_scd2_config)

        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = ["order_id", "amount", "is_deleted",
                           "_ingest_timestamp", "_source_system", "_record_hash"]
        mock_df.count.return_value = 2
        mock_df.filter.return_value = mock_df
        mock_df.drop.return_value = mock_df

        writer.write_merge_scd2(mock_df)

        merge_sql = mock_spark.sql.call_args_list[0][0][0]
        assert "source.is_deleted = 'true'" in merge_sql
        assert "_cdc_operation = 'DELETE'" in merge_sql

    @patch("pyspark.sql.window.Window")
    @patch("pyspark.sql.functions.row_number")
    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"),
                    md5=MagicMock(return_value="hash_mock"),
                    concat_ws=MagicMock(return_value="concat_mock"),
                    coalesce=MagicMock(return_value="coalesce_mock"))
    def test_scd2_deduplicates_by_sequence_column(
        self, mock_row_number, mock_window, sample_scd2_config
    ):
        """When sequence_column is set, incoming data should be deduplicated."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        writer = DeltaWriter(mock_spark, sample_scd2_config)

        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = ["order_id", "amount", "updated_at",
                           "_ingest_timestamp", "_source_system", "_record_hash"]
        mock_df.count.return_value = 5
        mock_df.filter.return_value = mock_df
        mock_df.drop.return_value = mock_df

        writer.write_merge_scd2(mock_df)

        # Should have called filter("_rn = 1") for dedup
        mock_df.filter.assert_called_once_with("_rn = 1")
        mock_df.drop.assert_called_once_with("_rn")


class TestWriteUpsert:
    """Tests for upsert merge logic."""

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"))
    def test_upsert_raises_without_primary_keys(self, sample_upsert_config):
        sample_upsert_config.target.cdc.primary_keys = []
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_upsert_config)

        with pytest.raises(ValueError, match="primary_keys"):
            writer.write_upsert(MagicMock())

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"))
    def test_upsert_returns_zero_for_empty_df(self, sample_upsert_config):
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_upsert_config)

        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = ["transaction_id", "amount"]
        mock_df.count.return_value = 0

        result = writer.write_upsert(mock_df)
        assert result == 0

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"))
    def test_upsert_initial_load_when_table_missing(self, sample_upsert_config):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        writer = DeltaWriter(mock_spark, sample_upsert_config)

        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = ["transaction_id", "amount"]
        mock_df.count.return_value = 10

        mock_writer = MagicMock()
        mock_df.write.format.return_value.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer

        result = writer.write_upsert(mock_df)
        assert result == 10
        mock_writer.saveAsTable.assert_called_once()

    @patch.multiple("bronze_framework.writers.delta_writer",
                    current_timestamp=MagicMock(return_value="ts_mock"),
                    current_date=MagicMock(return_value="date_mock"),
                    lit=MagicMock(), col=MagicMock(),
                    input_file_name=MagicMock(return_value="file_mock"),
                    expr=MagicMock(return_value="expr_mock"))
    def test_upsert_merge_executes_sql_when_table_exists(self, sample_upsert_config):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        writer = DeltaWriter(mock_spark, sample_upsert_config)

        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = ["transaction_id", "amount", "status", "_ingest_timestamp"]
        mock_df.count.return_value = 4

        result = writer.write_upsert(mock_df)

        assert result == 4
        mock_df.createOrReplaceTempView.assert_called_once_with("_bronze_incoming")
        merge_sql = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO" in merge_sql
        assert "target.transaction_id = source.transaction_id" in merge_sql
        assert "WHEN MATCHED THEN UPDATE SET" in merge_sql
        assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
        mock_spark.catalog.dropTempView.assert_called_once_with("_bronze_incoming")


class TestAddRecordHash:
    """Tests for _add_record_hash column computation."""

    @patch("bronze_framework.writers.delta_writer.md5", return_value="hash_mock")
    @patch("bronze_framework.writers.delta_writer.concat_ws", return_value="concat_mock")
    @patch("bronze_framework.writers.delta_writer.coalesce", return_value="coalesce_mock")
    @patch("bronze_framework.writers.delta_writer.col")
    @patch("bronze_framework.writers.delta_writer.lit")
    def test_excludes_primary_keys_from_hash(
        self, mock_lit, mock_col, mock_coalesce, mock_concat, mock_md5,
        sample_scd2_config,
    ):
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_scd2_config)

        mock_df = MagicMock()
        mock_df.columns = [
            "order_id", "amount", "status", "updated_at",
            "_ingest_timestamp", "_source_system",
        ]
        mock_df.withColumn.return_value = mock_df

        result = writer._add_record_hash(
            mock_df,
            primary_keys=["order_id"],
            exclude_columns=["_ingest_timestamp", "_source_system", "updated_at"],
        )

        # concat_ws should have been called with only the hash-eligible columns
        # Eligible: amount, status (order_id=PK, updated_at+_ingest_timestamp+_source_system=excluded)
        mock_concat.assert_called_once()
        concat_args = mock_concat.call_args
        # First arg is separator "||", rest are coalesce expressions
        assert concat_args[0][0] == "||"
        # 2 columns to hash: amount, status (sorted)
        assert len(concat_args[0]) == 3  # separator + 2 col expressions

        mock_df.withColumn.assert_called_once_with("_record_hash", "hash_mock")

    @patch("bronze_framework.writers.delta_writer.md5", return_value="hash_mock")
    @patch("bronze_framework.writers.delta_writer.concat_ws", return_value="concat_mock")
    @patch("bronze_framework.writers.delta_writer.coalesce", return_value="coalesce_mock")
    @patch("bronze_framework.writers.delta_writer.col")
    @patch("bronze_framework.writers.delta_writer.lit")
    def test_excludes_scd2_system_columns(
        self, mock_lit, mock_col, mock_coalesce, mock_concat, mock_md5,
        sample_scd2_config,
    ):
        """SCD2 system columns like _effective_from should never be hashed."""
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_scd2_config)

        mock_df = MagicMock()
        mock_df.columns = [
            "order_id", "amount",
            "_effective_from", "_effective_to", "_is_current",
            "_record_hash", "_cdc_operation",
        ]
        mock_df.withColumn.return_value = mock_df

        writer._add_record_hash(mock_df, primary_keys=["order_id"], exclude_columns=[])

        mock_concat.assert_called_once()
        concat_args = mock_concat.call_args
        # Only "amount" should be hashed (order_id=PK, rest=SCD2 system cols)
        assert len(concat_args[0]) == 2  # separator + 1 col expression

    @patch("bronze_framework.writers.delta_writer.md5", return_value="hash_mock")
    @patch("bronze_framework.writers.delta_writer.concat_ws", return_value="concat_mock")
    @patch("bronze_framework.writers.delta_writer.coalesce", return_value="coalesce_mock")
    @patch("bronze_framework.writers.delta_writer.col")
    @patch("bronze_framework.writers.delta_writer.lit")
    def test_falls_back_to_all_non_key_columns_when_no_hash_cols(
        self, mock_lit, mock_col, mock_coalesce, mock_concat, mock_md5,
        sample_scd2_config,
    ):
        """If all non-key columns are excluded, fall back to all non-key columns."""
        mock_spark = MagicMock()
        writer = DeltaWriter(mock_spark, sample_scd2_config)

        mock_df = MagicMock()
        # Only PK + system cols — no "real" data columns left after exclusion
        mock_df.columns = ["order_id", "_ingest_timestamp", "_source_system"]
        mock_df.withColumn.return_value = mock_df

        writer._add_record_hash(
            mock_df,
            primary_keys=["order_id"],
            exclude_columns=["_ingest_timestamp", "_source_system"],
        )

        mock_concat.assert_called_once()
        concat_args = mock_concat.call_args
        # Fallback: hash all non-key columns = _ingest_timestamp, _source_system (sorted)
        assert len(concat_args[0]) == 3  # separator + 2 col expressions


class TestTableExists:
    """Tests for _table_exists helper."""

    def test_returns_true_when_table_exists(self, sample_scd2_config):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        writer = DeltaWriter(mock_spark, sample_scd2_config)
        assert writer._table_exists("test_catalog.bronze.orders") is True

    def test_returns_false_when_table_missing(self, sample_scd2_config):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        writer = DeltaWriter(mock_spark, sample_scd2_config)
        assert writer._table_exists("test_catalog.bronze.orders") is False

    def test_returns_false_on_exception(self, sample_scd2_config):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.side_effect = Exception("catalog error")
        writer = DeltaWriter(mock_spark, sample_scd2_config)
        assert writer._table_exists("test_catalog.bronze.orders") is False
