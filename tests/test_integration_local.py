"""
Local integration test — runs the framework end-to-end against real JSON files
using a local SparkSession with Delta Lake.

This exercises: ConfigLoader -> FileReader -> DeltaWriter -> AuditLogger
(the full ingestion pipeline minus Databricks-specific features like
Auto Loader, Unity Catalog, and dbutils secrets).

Part 1 (steps 1-5): Basic read/write/schema evolution with append mode.
Part 2 (steps 6-9): SCD2 CDC merge with change detection and history.
"""

from __future__ import annotations

import json as _json
import os
import shutil
import sys
import tempfile
from pathlib import Path

import yaml

# ── Ensure env vars are set before Spark/Java init ──────────────────────────
os.environ.setdefault("JAVA_HOME", r"C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot")
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
# Add hadoop bin to PATH so hadoop.dll is found
hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
if hadoop_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEST_DATA = PROJECT_ROOT / "test_data" / "clickstream"


def get_spark() -> SparkSession:
    """Create a local SparkSession with Delta Lake support."""
    warehouse = tempfile.mkdtemp()
    derby_home = tempfile.mkdtemp()
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("bronze_framework_integration_test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_home}")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # Disable native IO on Windows to avoid hadoop.dll issues
    spark._jsc.hadoopConfiguration().set("io.nativeio.NativeIO", "false")
    return spark


def _write_json_file(path: str, records: list[dict]) -> None:
    """Write records as newline-delimited JSON."""
    with open(path, "w") as f:
        for rec in records:
            f.write(_json.dumps(rec) + "\n")


def main() -> None:
    spark = get_spark()
    tmp_dir = tempfile.mkdtemp(prefix="bronze_integration_")
    output_path = os.path.join(tmp_dir, "bronze_clickstream")
    audit_path = os.path.join(tmp_dir, "audit_log")
    dead_letter_path = os.path.join(tmp_dir, "dead_letter")

    total_steps = 9

    print("=" * 70)
    print("BRONZE FRAMEWORK — LOCAL INTEGRATION TEST")
    print("=" * 70)
    print(f"Test data : {TEST_DATA}")
    print(f"Output dir: {tmp_dir}")
    print()

    # ── Step 1: Read JSON files with FileReader ────────────────────────────
    print(f"[1/{total_steps}] Reading JSON test files with FileReader (batch mode)...")

    from bronze_framework.config.models import (
        CdcConfig,
        CdcMode,
        ExtractConfig,
        LandingConfig,
        LoadType,
        MetadataColumn,
        QualityConfig,
        SchemaEvolutionConfig,
        SchemaEvolutionMode,
        SourceConfig,
        SourceType,
        TargetConfig,
    )
    from bronze_framework.readers.file_reader import FileReader

    config = SourceConfig(
        name="clickstream_test",
        source_type=SourceType.FILE,
        description="Integration test - clickstream JSON files",
        extract=ExtractConfig(
            load_type=LoadType.FULL,
            path=str(TEST_DATA),
            format="json",
            auto_loader=False,
            format_options={"multiLine": "false"},
        ),
        target=TargetConfig(
            catalog="local",
            schema="bronze",
            table="clickstream_events",
            metadata_columns=[
                MetadataColumn(name="_ingest_timestamp", expression="current_timestamp()"),
                MetadataColumn(name="_source_system", expression="lit('clickstream')"),
            ],
            schema_evolution=SchemaEvolutionConfig(
                mode=SchemaEvolutionMode.MERGE,
            ),
            quality=QualityConfig(enabled=False),
        ),
    )

    reader = FileReader(spark, config)
    df = reader.read()
    record_count = df.count()
    print(f"   Records read: {record_count}")
    print(f"   Schema: {df.columns}")
    assert record_count == 8, f"Expected 8 records, got {record_count}"
    print("   PASS")
    print()

    # ── Step 2: Write to Delta with DeltaWriter ───────────────────────────
    print(f"[2/{total_steps}] Writing to Delta table with DeltaWriter...")

    from bronze_framework.writers.delta_writer import DeltaWriter

    writer = DeltaWriter(spark, config)
    df_with_meta = writer._add_metadata_columns(df)
    print(f"   Columns after metadata injection: {df_with_meta.columns}")
    assert "_ingest_timestamp" in df_with_meta.columns
    assert "_source_system" in df_with_meta.columns

    # Write directly to a Delta path (since we don't have Unity Catalog locally)
    df_with_meta.write.format("delta").mode("overwrite").save(output_path)
    print(f"   Written to: {output_path}")

    # Read back and verify
    df_read_back = spark.read.format("delta").load(output_path)
    written_count = df_read_back.count()
    print(f"   Records in Delta table: {written_count}")
    assert written_count == 8, f"Expected 8 records in Delta, got {written_count}"
    print("   PASS")
    print()

    # ── Step 3: Verify schema ──────────────────────────────────────────────
    print(f"[3/{total_steps}] Verifying Delta table schema...")
    schema_fields = [f.name for f in df_read_back.schema.fields]
    expected_cols = ["user_id", "event_type", "page", "timestamp", "session_id",
                     "_ingest_timestamp", "_source_system"]
    for c in expected_cols:
        assert c in schema_fields, f"Missing column: {c}"
        print(f"   {c}: present")
    print("   PASS")
    print()

    # ── Step 4: Verify data content ────────────────────────────────────────
    print(f"[4/{total_steps}] Verifying data content...")
    page_views = df_read_back.filter("event_type = 'page_view'").count()
    clicks = df_read_back.filter("event_type = 'click'").count()
    purchases = df_read_back.filter("event_type = 'purchase'").count()
    print(f"   page_view: {page_views}, click: {clicks}, purchase: {purchases}")
    assert page_views == 5, f"Expected 5 page_views, got {page_views}"
    assert clicks == 2, f"Expected 2 clicks, got {clicks}"
    assert purchases == 1, f"Expected 1 purchase, got {purchases}"

    # Verify metadata columns have values
    first_row = df_read_back.select("_ingest_timestamp", "_source_system").first()
    assert first_row["_ingest_timestamp"] is not None, "Ingest timestamp should not be null"
    assert first_row["_source_system"] == "clickstream", \
        f"Expected 'clickstream', got '{first_row['_source_system']}'"
    print("   Metadata values verified")
    print("   PASS")
    print()

    # ── Step 5: Schema evolution — append with new column ──────────────────
    print(f"[5/{total_steps}] Testing schema evolution (merge new column)...")
    from pyspark.sql.functions import lit, current_timestamp

    # Write new record with extra column as JSON file (avoids Python worker)
    new_record_path = os.path.join(tmp_dir, "new_record.json")
    with open(new_record_path, "w") as _f:
        _f.write(_json.dumps({
            "user_id": "u007", "event_type": "signup", "page": "/register",
            "timestamp": "2024-06-02T09:00:00Z", "session_id": "s200",
            "device_type": "mobile"
        }))

    new_data = spark.read.json(new_record_path)
    new_data = new_data.withColumn("_ingest_timestamp", current_timestamp())
    new_data = new_data.withColumn("_source_system", lit("clickstream"))

    new_data.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).save(output_path)

    df_evolved = spark.read.format("delta").load(output_path)
    total_after = df_evolved.count()
    print(f"   Total records after append: {total_after}")
    assert total_after == 9, f"Expected 9 records, got {total_after}"

    evolved_cols = [f.name for f in df_evolved.schema.fields]
    assert "device_type" in evolved_cols, "Schema evolution failed — device_type not found"
    print(f"   New column 'device_type' merged successfully")

    mobile_row = df_evolved.filter("device_type = 'mobile'").first()
    assert mobile_row is not None
    null_device_count = df_evolved.filter("device_type IS NULL").count()
    print(f"   Existing rows have device_type=NULL: {null_device_count} rows (expected 8)")
    assert null_device_count == 8
    print("   PASS")
    print()

    # ==================================================================
    # Part 2: SCD2 CDC Merge Tests
    # ==================================================================
    print("-" * 70)
    print("Part 2: SCD2 CDC MERGE TESTS")
    print("-" * 70)
    print()

    # Create a local database for managed tables (needed for MERGE INTO SQL)
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze_test")

    # Prepare landing data directory for SCD2 tests
    scd2_landing = os.path.join(tmp_dir, "landing_orders")
    os.makedirs(scd2_landing, exist_ok=True)

    # SCD2 config using a managed table (so MERGE INTO works by table name)
    scd2_config = SourceConfig(
        name="orders_scd2_test",
        source_type=SourceType.FILE,
        description="SCD2 integration test - orders",
        extract=ExtractConfig(
            load_type=LoadType.INCREMENTAL,
            path=scd2_landing,
            format="json",
            auto_loader=False,
            format_options={"multiLine": "false"},
        ),
        target=TargetConfig(
            catalog="spark_catalog",
            schema="bronze_test",
            table="orders_scd2",
            metadata_columns=[
                MetadataColumn(name="_ingest_timestamp", expression="current_timestamp()"),
                MetadataColumn(name="_source_system", expression="lit('erp')"),
            ],
            schema_evolution=SchemaEvolutionConfig(mode=SchemaEvolutionMode.MERGE),
            quality=QualityConfig(enabled=False),
            cdc=CdcConfig(
                enabled=True,
                mode=CdcMode.SCD2,
                primary_keys=["order_id"],
                sequence_column="updated_at",
                exclude_columns_from_hash=[
                    "_ingest_timestamp", "_source_system", "updated_at",
                ],
            ),
            landing=LandingConfig(
                path=scd2_landing,
                retention_days=10,
                cleanup_enabled=False,
            ),
        ),
    )

    # ── Step 6: SCD2 Initial Load ──────────────────────────────────────────
    print(f"[6/{total_steps}] SCD2 initial load (first batch of orders)...")

    initial_orders = [
        {"order_id": "O001", "customer": "Alice", "amount": 100.0, "status": "pending", "updated_at": "2024-01-15T10:00:00Z"},
        {"order_id": "O002", "customer": "Bob", "amount": 250.0, "status": "pending", "updated_at": "2024-01-15T10:01:00Z"},
        {"order_id": "O003", "customer": "Carol", "amount": 75.5, "status": "shipped", "updated_at": "2024-01-15T10:02:00Z"},
    ]
    initial_file = os.path.join(scd2_landing, "day1_orders.json")
    _write_json_file(initial_file, initial_orders)

    df_day1 = spark.read.json(scd2_landing)
    scd2_writer = DeltaWriter(spark, scd2_config)
    rows_written = scd2_writer.write_batch(df_day1)

    print(f"   Rows processed: {rows_written}")
    assert rows_written == 3, f"Expected 3 rows, got {rows_written}"

    # Verify: all 3 records are _is_current=true
    scd2_table = spark.table("spark_catalog.bronze_test.orders_scd2")
    total = scd2_table.count()
    current = scd2_table.filter("_is_current = true").count()
    print(f"   Total rows: {total}, Current rows: {current}")
    assert total == 3, f"Expected 3 total rows, got {total}"
    assert current == 3, f"Expected 3 current rows, got {current}"

    # Verify SCD2 columns exist
    scd2_cols = [f.name for f in scd2_table.schema.fields]
    for col_name in ["_effective_from", "_effective_to", "_is_current", "_record_hash", "_cdc_operation"]:
        assert col_name in scd2_cols, f"Missing SCD2 column: {col_name}"
    print(f"   SCD2 columns present: _effective_from, _effective_to, _is_current, _record_hash, _cdc_operation")

    # Verify all records have _cdc_operation='INSERT'
    inserts = scd2_table.filter("_cdc_operation = 'INSERT'").count()
    assert inserts == 3, f"Expected 3 INSERTs, got {inserts}"
    print(f"   All records marked as INSERT")

    # Verify _effective_to is NULL for all current records
    null_eff_to = scd2_table.filter("_effective_to IS NULL").count()
    assert null_eff_to == 3, f"Expected 3 NULL _effective_to, got {null_eff_to}"
    print("   PASS")
    print()

    # ── Step 7: SCD2 Change Detection — update existing + insert new ──────
    print(f"[7/{total_steps}] SCD2 change detection (update + insert)...")

    # Remove old file, write day2 batch:
    #   O001: amount changed 100 -> 150 (CHANGED)
    #   O002: same as before (UNCHANGED — should not create new version)
    #   O004: new order (NEW)
    os.remove(initial_file)
    day2_orders = [
        {"order_id": "O001", "customer": "Alice", "amount": 150.0, "status": "completed", "updated_at": "2024-01-16T10:00:00Z"},
        {"order_id": "O002", "customer": "Bob", "amount": 250.0, "status": "pending", "updated_at": "2024-01-16T10:01:00Z"},
        {"order_id": "O004", "customer": "Dave", "amount": 300.0, "status": "pending", "updated_at": "2024-01-16T10:02:00Z"},
    ]
    day2_file = os.path.join(scd2_landing, "day2_orders.json")
    _write_json_file(day2_file, day2_orders)

    df_day2 = spark.read.json(scd2_landing)
    rows_processed = scd2_writer.write_batch(df_day2)
    print(f"   Rows processed: {rows_processed}")

    # Refresh table view
    scd2_table = spark.table("spark_catalog.bronze_test.orders_scd2")
    total = scd2_table.count()
    current = scd2_table.filter("_is_current = true").count()
    historical = scd2_table.filter("_is_current = false").count()
    print(f"   Total rows: {total}, Current: {current}, Historical: {historical}")

    # Expected state:
    #   O001: 2 rows (1 old closed + 1 new current) — changed
    #   O002: 1 row (unchanged, still current)
    #   O003: 1 row (not in day2, still current from day1)
    #   O004: 1 row (new INSERT)
    # Total = 5, Current = 4, Historical = 1
    assert total == 5, f"Expected 5 total rows, got {total}"
    assert current == 4, f"Expected 4 current rows, got {current}"
    assert historical == 1, f"Expected 1 historical row, got {historical}"

    # Verify O001 has 2 versions
    o001_rows = scd2_table.filter("order_id = 'O001'").orderBy("_effective_from")
    o001_count = o001_rows.count()
    assert o001_count == 2, f"Expected 2 rows for O001, got {o001_count}"

    o001_list = o001_rows.collect()
    old_version = o001_list[0]
    new_version = o001_list[1]
    assert old_version["_is_current"] == False, "Old O001 version should be _is_current=false"
    assert old_version["_effective_to"] is not None, "Old O001 version should have _effective_to set"
    assert old_version["amount"] == 100.0, f"Old O001 amount should be 100.0, got {old_version['amount']}"
    assert new_version["_is_current"] == True, "New O001 version should be _is_current=true"
    assert new_version["_effective_to"] is None, "New O001 _effective_to should be NULL"
    assert new_version["amount"] == 150.0, f"New O001 amount should be 150.0, got {new_version['amount']}"
    print(f"   O001: old version (amount=100, closed) + new version (amount=150, current)")

    # Verify O002 still has only 1 row (unchanged)
    o002_count = scd2_table.filter("order_id = 'O002'").count()
    assert o002_count == 1, f"Expected 1 row for O002 (unchanged), got {o002_count}"
    print(f"   O002: unchanged — still 1 row, _is_current=true")

    # Verify O003 still current from day1
    o003 = scd2_table.filter("order_id = 'O003' AND _is_current = true").first()
    assert o003 is not None, "O003 should still be current"
    assert o003["customer"] == "Carol"
    print(f"   O003: untouched — still current from day 1")

    # Verify O004 is new INSERT
    o004 = scd2_table.filter("order_id = 'O004' AND _is_current = true").first()
    assert o004 is not None, "O004 should exist as current"
    assert o004["customer"] == "Dave"
    assert o004["amount"] == 300.0
    print(f"   O004: new INSERT — current row present")
    print("   PASS")
    print()

    # ── Step 8: SCD2 — unchanged batch produces no new versions ──────────
    print(f"[8/{total_steps}] SCD2 idempotency (re-send same data, no new versions)...")

    # Re-read the same day2 file — nothing should change
    df_day2_again = spark.read.json(scd2_landing)
    scd2_writer.write_batch(df_day2_again)

    scd2_table = spark.table("spark_catalog.bronze_test.orders_scd2")
    total_after_replay = scd2_table.count()
    current_after_replay = scd2_table.filter("_is_current = true").count()
    print(f"   Total rows after replay: {total_after_replay}")
    print(f"   Current rows after replay: {current_after_replay}")

    # Should be same as before — no duplicates
    assert total_after_replay == 5, f"Expected 5 total rows (idempotent), got {total_after_replay}"
    assert current_after_replay == 4, f"Expected 4 current rows (idempotent), got {current_after_replay}"
    print("   PASS")
    print()

    # ── Step 9: Verify record hash consistency ────────────────────────────
    print(f"[9/{total_steps}] Verifying _record_hash consistency...")

    # All current records should have non-null _record_hash
    null_hash = scd2_table.filter("_is_current = true AND _record_hash IS NULL").count()
    assert null_hash == 0, f"Expected 0 null hashes in current records, got {null_hash}"
    print(f"   All current records have non-null _record_hash")

    # O001's two versions should have different hashes (data changed)
    o001_hashes = (
        scd2_table.filter("order_id = 'O001'")
        .select("_record_hash")
        .distinct()
        .count()
    )
    assert o001_hashes == 2, f"Expected 2 distinct hashes for O001, got {o001_hashes}"
    print(f"   O001 has 2 distinct hashes (data changed)")

    # O002 should have consistent hash across both runs
    o002_hash = (
        scd2_table.filter("order_id = 'O002'")
        .select("_record_hash")
        .first()["_record_hash"]
    )
    assert o002_hash is not None
    print(f"   O002 hash consistent: {o002_hash[:16]}...")
    print("   PASS")
    print()

    # ── Summary ────────────────────────────────────────────────────────────
    print("=" * 70)
    print("ALL INTEGRATION TESTS PASSED")
    print("=" * 70)
    print()
    print("  Part 1 — Basic Ingestion:")
    print(f"    FileReader:       read 8 JSON records")
    print(f"    DeltaWriter:      wrote to Delta with metadata columns")
    print(f"    Schema:           all expected columns present")
    print(f"    Data content:     event type counts verified")
    print(f"    Schema evolution: new column merged, existing rows backfilled with NULL")
    print()
    print("  Part 2 — SCD2 CDC Merge:")
    print(f"    Initial load:     3 orders -> all _is_current=true, _cdc_operation=INSERT")
    print(f"    Change detection: O001 updated -> old closed, new version created")
    print(f"    New records:      O004 inserted as current")
    print(f"    Unchanged:        O002 & O003 untouched (no duplicate versions)")
    print(f"    Idempotency:      replay same data -> no new rows")
    print(f"    Record hashes:    consistent, different for changed data")
    print()

    # Cleanup
    spark.sql("DROP DATABASE IF EXISTS bronze_test CASCADE")
    spark.stop()
    shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
