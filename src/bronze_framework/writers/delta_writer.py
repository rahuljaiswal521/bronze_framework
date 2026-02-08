"""Delta Lake writer with schema evolution, CDC/SCD2 merge, and streaming support."""

from __future__ import annotations

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_date,
    current_timestamp,
    expr,
    input_file_name,
    lit,
    md5,
    when,
)
from pyspark.sql.streaming import StreamingQuery

from bronze_framework.config.models import (
    CdcMode,
    SchemaEvolutionMode,
    SourceConfig,
)
from bronze_framework.utils.logging import get_logger

logger = get_logger(__name__)

# SCD2 system columns added automatically when CDC is enabled
SCD2_COLUMNS = [
    "_effective_from",   # TIMESTAMP — when this version became active
    "_effective_to",     # TIMESTAMP — when superseded (NULL = current)
    "_is_current",       # BOOLEAN — easy filter for latest version
    "_record_hash",      # STRING — MD5 hash of non-key columns for change detection
    "_cdc_operation",    # STRING — INSERT / UPDATE / DELETE
]


class DeltaWriter:
    """Writes DataFrames to Delta Lake tables with schema evolution and CDC."""

    def __init__(self, spark: SparkSession, config: SourceConfig):
        self.spark = spark
        self.config = config

    # ── Public write methods ─────────────────────────────────────────────

    def write_batch(self, df: DataFrame) -> int:
        """Write a batch DataFrame to Delta.

        Routes to SCD2 merge or plain append based on CDC config.
        Returns row count written/affected.
        """
        cdc = self.config.target.cdc

        if cdc.enabled and cdc.mode == CdcMode.SCD2:
            return self.write_merge_scd2(df)
        if cdc.enabled and cdc.mode == CdcMode.UPSERT:
            return self.write_upsert(df)
        return self._write_append(df)

    def write_merge_scd2(self, df: DataFrame) -> int:
        """SCD Type 2 merge — maintains full history in the bronze table.

        Logic:
        1. Add metadata + _record_hash to incoming data
        2. Join with existing current records on primary keys
        3. New keys       → INSERT with _is_current=True
        4. Changed keys   → close old record (_is_current=False, _effective_to=now)
                            + INSERT new version (_is_current=True)
        5. Unchanged keys → skip (no write)
        6. Soft deletes   → close record if delete condition matched
        """
        cdc = self.config.target.cdc
        target = self.config.target
        table_name = self.config.full_table_name
        pk_cols = cdc.primary_keys

        if not pk_cols:
            raise ValueError(
                f"CDC SCD2 requires primary_keys for source: {self.config.name}"
            )

        # Prepare incoming data
        df = self._add_metadata_columns(df)
        df = self._apply_schema_evolution_options(df)
        df = self._add_record_hash(df, pk_cols, cdc.exclude_columns_from_hash)

        row_count = df.count()
        if row_count == 0:
            logger.info(f"No incoming records for {self.config.name}; skipping merge")
            return 0

        # Ensure target table exists (first run)
        if not self._table_exists(table_name):
            logger.info(f"Target table {table_name} does not exist; creating with first load")
            return self._initial_scd2_load(df, table_name, target)

        # Deduplicate incoming by primary keys (keep latest by sequence column)
        if cdc.sequence_column and cdc.sequence_column in df.columns:
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number
            w = Window.partitionBy(*pk_cols).orderBy(col(cdc.sequence_column).desc())
            df = df.withColumn("_rn", row_number().over(w)).filter("_rn = 1").drop("_rn")

        # Register incoming as temp view
        df.createOrReplaceTempView("_bronze_incoming")

        # Build merge key condition
        key_condition = " AND ".join(
            f"target.{k} = source.{k}" for k in pk_cols
        )

        # Get all columns for insert (exclude SCD2 system cols — we set them explicitly)
        data_columns = [c for c in df.columns if c not in SCD2_COLUMNS]
        insert_cols_str = ", ".join(data_columns)
        insert_vals_str = ", ".join(f"source.{c}" for c in data_columns)

        # Handle soft deletes
        delete_close_clause = ""
        if cdc.delete_condition_column and cdc.delete_condition_value:
            delete_close_clause = f"""
                WHEN MATCHED
                    AND target._is_current = true
                    AND source.{cdc.delete_condition_column} = '{cdc.delete_condition_value}'
                THEN UPDATE SET
                    target._is_current = false,
                    target._effective_to = current_timestamp(),
                    target._cdc_operation = 'DELETE'
            """

        # SCD2 Merge SQL
        merge_sql = f"""
            MERGE INTO {table_name} AS target
            USING _bronze_incoming AS source
            ON {key_condition} AND target._is_current = true

            {delete_close_clause}

            WHEN MATCHED
                AND target._is_current = true
                AND target._record_hash <> source._record_hash
            THEN UPDATE SET
                target._is_current = false,
                target._effective_to = current_timestamp(),
                target._cdc_operation = 'UPDATE'

            WHEN NOT MATCHED THEN INSERT (
                {insert_cols_str},
                _effective_from, _effective_to, _is_current, _record_hash, _cdc_operation
            ) VALUES (
                {insert_vals_str},
                current_timestamp(), NULL, true, source._record_hash, 'INSERT'
            )
        """

        logger.info(
            f"Executing SCD2 merge on {table_name} with {row_count} incoming records"
        )

        if target.schema_evolution.mode == SchemaEvolutionMode.MERGE:
            self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        self.spark.sql(merge_sql)

        # Now insert new versions for changed records (the MERGE above only closed them)
        source_vals_str = ", ".join(f"source.{c}" for c in data_columns)
        changed_insert_sql = f"""
            INSERT INTO {table_name} (
                {insert_cols_str},
                _effective_from, _effective_to, _is_current, _record_hash, _cdc_operation
            )
            SELECT
                {source_vals_str},
                current_timestamp(),
                CAST(NULL AS TIMESTAMP),
                true,
                source._record_hash,
                'UPDATE'
            FROM _bronze_incoming source
            INNER JOIN {table_name} target
                ON {key_condition}
                AND target._is_current = false
                AND target._effective_to IS NOT NULL
                AND target._cdc_operation = 'UPDATE'
                AND target._record_hash <> source._record_hash
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_name} t2
                WHERE {"AND ".join(f"t2.{k} = source.{k} " for k in pk_cols)}
                AND t2._is_current = true
            )
        """
        self.spark.sql(changed_insert_sql)

        self.spark.catalog.dropTempView("_bronze_incoming")

        logger.info(f"SCD2 merge complete for {self.config.name}")
        return row_count

    def write_upsert(self, df: DataFrame) -> int:
        """Simple upsert — update matched rows, insert new ones (no history)."""
        cdc = self.config.target.cdc
        target = self.config.target
        table_name = self.config.full_table_name
        pk_cols = cdc.primary_keys

        if not pk_cols:
            raise ValueError(
                f"CDC upsert requires primary_keys for source: {self.config.name}"
            )

        df = self._add_metadata_columns(df)
        df = self._apply_schema_evolution_options(df)

        row_count = df.count()
        if row_count == 0:
            return 0

        if not self._table_exists(table_name):
            return self._write_initial_load(df, table_name, target)

        df.createOrReplaceTempView("_bronze_incoming")

        key_condition = " AND ".join(
            f"target.{k} = source.{k}" for k in pk_cols
        )

        update_cols = [c for c in df.columns if c not in pk_cols]
        update_set = ", ".join(f"target.{c} = source.{c}" for c in update_cols)
        all_cols = ", ".join(df.columns)
        all_vals = ", ".join(f"source.{c}" for c in df.columns)

        merge_sql = f"""
            MERGE INTO {table_name} AS target
            USING _bronze_incoming AS source
            ON {key_condition}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({all_cols}) VALUES ({all_vals})
        """

        if target.schema_evolution.mode == SchemaEvolutionMode.MERGE:
            self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        self.spark.sql(merge_sql)
        self.spark.catalog.dropTempView("_bronze_incoming")

        logger.info(f"Upsert complete for {self.config.name}: {row_count} records processed")
        return row_count

    def write_stream(self, df: DataFrame) -> StreamingQuery:
        """Write a streaming DataFrame to Delta using availableNow trigger."""
        df = self._add_metadata_columns(df)

        target = self.config.target
        table_name = self.config.full_table_name
        checkpoint = self.config.extract.checkpoint_path

        writer = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint)
        )

        if target.schema_evolution.mode == SchemaEvolutionMode.MERGE:
            writer = writer.option("mergeSchema", "true")

        if target.partition_by:
            writer = writer.partitionBy(*target.partition_by)

        logger.info(f"Starting streaming write to {table_name}")

        query = writer.toTable(table_name)
        query.awaitTermination()

        return query

    # ── Private helpers ──────────────────────────────────────────────────

    def _write_append(self, df: DataFrame) -> int:
        """Plain append write (no CDC)."""
        df = self._add_metadata_columns(df)
        df = self._apply_schema_evolution_options(df)

        target = self.config.target
        table_name = self.config.full_table_name

        writer = df.write.format("delta").mode("append")

        if target.partition_by:
            writer = writer.partitionBy(*target.partition_by)

        if target.schema_evolution.mode == SchemaEvolutionMode.MERGE:
            writer = writer.option("mergeSchema", "true")

        for key, value in target.table_properties.items():
            writer = writer.option(key, value)

        row_count = df.count()
        logger.info(f"Appending {row_count} rows to {table_name}")

        writer.saveAsTable(table_name)

        if target.z_order_by:
            z_cols = ", ".join(target.z_order_by)
            self.spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({z_cols})")

        return row_count

    def _initial_scd2_load(
        self, df: DataFrame, table_name: str, target
    ) -> int:
        """First load for SCD2 — insert all rows as current records."""
        df = (
            df.withColumn("_effective_from", current_timestamp())
            .withColumn("_effective_to", lit(None).cast("timestamp"))
            .withColumn("_is_current", lit(True))
            .withColumn("_cdc_operation", lit("INSERT"))
        )

        row_count = df.count()

        writer = df.write.format("delta").mode("overwrite")

        if target.partition_by:
            writer = writer.partitionBy(*target.partition_by)

        if target.schema_evolution.mode == SchemaEvolutionMode.MERGE:
            writer = writer.option("mergeSchema", "true")

        for key, value in target.table_properties.items():
            writer = writer.option(key, value)

        writer.saveAsTable(table_name)

        logger.info(f"Initial SCD2 load: {row_count} rows into {table_name}")
        return row_count

    def _write_initial_load(
        self, df: DataFrame, table_name: str, target
    ) -> int:
        """First load for upsert — plain write."""
        row_count = df.count()
        writer = df.write.format("delta").mode("overwrite")
        if target.partition_by:
            writer = writer.partitionBy(*target.partition_by)
        writer.saveAsTable(table_name)
        logger.info(f"Initial upsert load: {row_count} rows into {table_name}")
        return row_count

    def _add_record_hash(
        self,
        df: DataFrame,
        primary_keys: List[str],
        exclude_columns: List[str],
    ) -> DataFrame:
        """Add _record_hash column — MD5 hash of all non-key, non-system columns.

        Used to detect whether a record has actually changed.
        """
        system_cols = set(SCD2_COLUMNS + [
            "_ingest_timestamp", "_ingest_date", "_source_system",
            "_source_file", "_rescued_data",
        ])
        exclude = set(primary_keys) | system_cols | set(exclude_columns)

        hash_cols = sorted([c for c in df.columns if c not in exclude])

        if not hash_cols:
            logger.warning("No columns to hash; using all non-key columns")
            hash_cols = sorted([c for c in df.columns if c not in set(primary_keys)])

        hash_expr = md5(
            concat_ws("||", *[coalesce(col(c).cast("string"), lit("__NULL__")) for c in hash_cols])
        )

        return df.withColumn("_record_hash", hash_expr)

    def _add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """Inject metadata columns defined in config."""
        for meta_col in self.config.target.metadata_columns:
            name = meta_col.name
            expression = meta_col.expression

            if expression == "current_timestamp()":
                df = df.withColumn(name, current_timestamp())
            elif expression == "current_date()":
                df = df.withColumn(name, current_date())
            elif expression == "input_file_name()":
                df = df.withColumn(name, input_file_name())
            elif expression.startswith("lit(") and expression.endswith(")"):
                value = expression[4:-1].strip("'\"")
                df = df.withColumn(name, lit(value))
            elif expression.startswith("col(") and expression.endswith(")"):
                col_name = expression[4:-1].strip("'\"")
                if col_name in df.columns:
                    df = df.withColumn(name, col(col_name))
            else:
                try:
                    df = df.withColumn(name, expr(expression))
                except Exception as e:
                    logger.warning(
                        f"Could not apply metadata expression '{expression}' "
                        f"for column '{name}': {e}"
                    )
        return df

    def _apply_schema_evolution_options(self, df: DataFrame) -> DataFrame:
        """Handle rescue mode by preserving _rescued_data column."""
        schema_evo = self.config.target.schema_evolution

        if schema_evo.mode == SchemaEvolutionMode.RESCUE:
            rescued_col = schema_evo.rescued_data_column
            if rescued_col not in df.columns:
                df = df.withColumn(rescued_col, lit(None).cast("string"))

        return df

    def _table_exists(self, table_name: str) -> bool:
        """Check if a Delta table exists."""
        try:
            return self.spark.catalog.tableExists(table_name)
        except Exception:
            return False
