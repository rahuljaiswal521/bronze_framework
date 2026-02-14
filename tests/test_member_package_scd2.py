"""
Member Package SCD2 End-to-End Test
====================================

Tests the Bronze Framework's SCD2 pipeline with a `member_package` source
simulating Oracle CDC log files. The source has an `_operation` column
indicating INSERT/UPDATE/DELETE — the framework handles this via:

- INSERT/UPDATE: Automatic detection via record hash comparison
- DELETE: Uses delete_condition_column/_operation + delete_condition_value/DELETE

Composite primary key: member_no + fund_no

Day 1: Initial load (5 records)
Day 2: Update M001/F01 (extend end_date, upgrade to PLATINUM)
Day 3: Delete M002/F01 (soft delete)
Day 4: Insert new M004/F02
"""

from __future__ import annotations

import json as _json
import os
import shutil
import sys
import tempfile
from pathlib import Path

# ── Ensure env vars are set before Spark/Java init ──────────────────────────
os.environ.setdefault("JAVA_HOME", r"C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot")
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
if hadoop_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Create a local SparkSession with Delta Lake support."""
    warehouse = tempfile.mkdtemp()
    derby_home = tempfile.mkdtemp()
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("member_package_scd2_test")
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


def _print_table(spark: SparkSession, table_name: str) -> None:
    """Pretty-print the full SCD2 table state."""
    df = spark.table(table_name)
    print()
    df.select(
        "member_no", "fund_no", "start_date", "end_date", "package_code",
        "_operation", "_is_current", "_cdc_operation",
        "_effective_from", "_effective_to",
    ).orderBy("member_no", "fund_no", "_effective_from").show(truncate=False)


def main() -> None:
    spark = get_spark()
    tmp_dir = tempfile.mkdtemp(prefix="member_pkg_scd2_")
    landing = os.path.join(tmp_dir, "landing_member_package")
    os.makedirs(landing, exist_ok=True)

    total_steps = 4

    print("=" * 70)
    print("MEMBER PACKAGE SCD2 — END-TO-END TEST")
    print("=" * 70)
    print(f"Temp dir: {tmp_dir}")
    print()

    # ── Imports ───────────────────────────────────────────────────────────
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
    from bronze_framework.writers.delta_writer import DeltaWriter

    # ── Config ────────────────────────────────────────────────────────────
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze_test")

    TABLE_NAME = "spark_catalog.bronze_test.member_package"

    config = SourceConfig(
        name="member_package_scd2_test",
        source_type=SourceType.FILE,
        description="SCD2 test — member package with composite PK and soft deletes",
        extract=ExtractConfig(
            load_type=LoadType.INCREMENTAL,
            path=landing,
            format="json",
            auto_loader=False,
            format_options={"multiLine": "false"},
        ),
        target=TargetConfig(
            catalog="spark_catalog",
            schema="bronze_test",
            table="member_package",
            metadata_columns=[
                MetadataColumn(name="_ingest_timestamp", expression="current_timestamp()"),
                MetadataColumn(name="_source_system", expression="lit('oracle_cdc')"),
            ],
            schema_evolution=SchemaEvolutionConfig(mode=SchemaEvolutionMode.MERGE),
            quality=QualityConfig(enabled=False),
            cdc=CdcConfig(
                enabled=True,
                mode=CdcMode.SCD2,
                primary_keys=["member_no", "fund_no"],
                exclude_columns_from_hash=[
                    "_operation", "_ingest_timestamp", "_source_system",
                ],
                delete_condition_column="_operation",
                delete_condition_value="DELETE",
            ),
            landing=LandingConfig(
                path=landing,
                retention_days=10,
                cleanup_enabled=False,
            ),
        ),
    )

    writer = DeltaWriter(spark, config)

    # ==================================================================
    # Day 1 — Initial Load (5 records)
    # ==================================================================
    print(f"[1/{total_steps}] Day 1 — Initial load (5 records)...")

    day1_records = [
        {"member_no": "M001", "fund_no": "F01", "start_date": "2024-01-01", "end_date": "2024-12-31", "package_code": "PKG_GOLD", "_operation": "INSERT"},
        {"member_no": "M001", "fund_no": "F02", "start_date": "2024-03-01", "end_date": "2024-12-31", "package_code": "PKG_SILVER", "_operation": "INSERT"},
        {"member_no": "M002", "fund_no": "F01", "start_date": "2024-01-15", "end_date": "2025-01-14", "package_code": "PKG_BRONZE", "_operation": "INSERT"},
        {"member_no": "M003", "fund_no": "F01", "start_date": "2024-06-01", "end_date": "2025-05-31", "package_code": "PKG_GOLD", "_operation": "INSERT"},
        {"member_no": "M003", "fund_no": "F03", "start_date": "2024-07-01", "end_date": "2025-06-30", "package_code": "PKG_PLATINUM", "_operation": "INSERT"},
    ]
    day1_file = os.path.join(landing, "day1.json")
    _write_json_file(day1_file, day1_records)

    df_day1 = spark.read.json(landing)
    rows = writer.write_batch(df_day1)
    print(f"   Rows processed: {rows}")

    tbl = spark.table(TABLE_NAME)
    total = tbl.count()
    current = tbl.filter("_is_current = true").count()
    inserts = tbl.filter("_cdc_operation = 'INSERT'").count()
    print(f"   Total: {total}, Current: {current}, INSERTs: {inserts}")

    assert total == 5, f"Expected 5 total, got {total}"
    assert current == 5, f"Expected 5 current, got {current}"
    assert inserts == 5, f"Expected 5 INSERTs, got {inserts}"

    # Verify _effective_to is NULL for all
    null_eff_to = tbl.filter("_effective_to IS NULL").count()
    assert null_eff_to == 5, f"Expected 5 NULL _effective_to, got {null_eff_to}"
    print("   All _effective_to = NULL (current records)")

    _print_table(spark, TABLE_NAME)
    print("   PASS")
    print()

    # ==================================================================
    # Day 2 — Update M001/F01 (extend end_date, upgrade to PLATINUM)
    # ==================================================================
    print(f"[2/{total_steps}] Day 2 — Update M001/F01 (end_date + package_code changed)...")

    # Clear landing, write day2 file
    os.remove(day1_file)
    day2_records = [
        {"member_no": "M001", "fund_no": "F01", "start_date": "2024-01-01", "end_date": "2025-12-31", "package_code": "PKG_PLATINUM", "_operation": "UPDATE"},
    ]
    day2_file = os.path.join(landing, "day2.json")
    _write_json_file(day2_file, day2_records)

    df_day2 = spark.read.json(landing)
    rows = writer.write_batch(df_day2)
    print(f"   Rows processed: {rows}")

    tbl = spark.table(TABLE_NAME)
    total = tbl.count()
    current = tbl.filter("_is_current = true").count()
    historical = tbl.filter("_is_current = false").count()
    print(f"   Total: {total}, Current: {current}, Historical: {historical}")

    assert total == 6, f"Expected 6 total, got {total}"
    assert current == 5, f"Expected 5 current, got {current}"
    assert historical == 1, f"Expected 1 historical, got {historical}"

    # Verify M001/F01 has 2 versions
    m001f01 = tbl.filter("member_no = 'M001' AND fund_no = 'F01'").orderBy("_effective_from").collect()
    assert len(m001f01) == 2, f"Expected 2 rows for M001/F01, got {len(m001f01)}"

    old = m001f01[0]
    new = m001f01[1]
    assert old["_is_current"] == False, "Old M001/F01 should be _is_current=false"
    assert old["_effective_to"] is not None, "Old M001/F01 should have _effective_to set"
    assert old["package_code"] == "PKG_GOLD", f"Old package_code should be PKG_GOLD, got {old['package_code']}"
    assert old["end_date"] == "2024-12-31", f"Old end_date should be 2024-12-31, got {old['end_date']}"
    assert old["_cdc_operation"] == "UPDATE", f"Old _cdc_operation should be UPDATE, got {old['_cdc_operation']}"
    print(f"   M001/F01 old: package_code=PKG_GOLD, end_date=2024-12-31, closed (_cdc_operation=UPDATE)")

    assert new["_is_current"] == True, "New M001/F01 should be _is_current=true"
    assert new["_effective_to"] is None, "New M001/F01 _effective_to should be NULL"
    assert new["package_code"] == "PKG_PLATINUM", f"New package_code should be PKG_PLATINUM, got {new['package_code']}"
    assert new["end_date"] == "2025-12-31", f"New end_date should be 2025-12-31, got {new['end_date']}"
    assert new["_cdc_operation"] == "UPDATE", f"New _cdc_operation should be UPDATE, got {new['_cdc_operation']}"
    print(f"   M001/F01 new: package_code=PKG_PLATINUM, end_date=2025-12-31, current (_cdc_operation=UPDATE)")

    # Verify other records untouched
    m001f02 = tbl.filter("member_no = 'M001' AND fund_no = 'F02'").count()
    assert m001f02 == 1, f"M001/F02 should still have 1 row, got {m001f02}"
    m003f01 = tbl.filter("member_no = 'M003' AND fund_no = 'F01'").count()
    assert m003f01 == 1, f"M003/F01 should still have 1 row, got {m003f01}"
    print("   Other records untouched")

    _print_table(spark, TABLE_NAME)
    print("   PASS")
    print()

    # ==================================================================
    # Day 3 — Delete M002/F01 (soft delete)
    # ==================================================================
    print(f"[3/{total_steps}] Day 3 — Delete M002/F01 (soft delete via _operation=DELETE)...")

    os.remove(day2_file)
    day3_records = [
        {"member_no": "M002", "fund_no": "F01", "start_date": "2024-01-15", "end_date": "2025-01-14", "package_code": "PKG_BRONZE", "_operation": "DELETE"},
    ]
    day3_file = os.path.join(landing, "day3.json")
    _write_json_file(day3_file, day3_records)

    df_day3 = spark.read.json(landing)
    rows = writer.write_batch(df_day3)
    print(f"   Rows processed: {rows}")

    tbl = spark.table(TABLE_NAME)
    total = tbl.count()
    current = tbl.filter("_is_current = true").count()
    print(f"   Total: {total}, Current: {current}")

    assert total == 6, f"Expected 6 total, got {total}"
    assert current == 4, f"Expected 4 current, got {current}"

    # Verify M002/F01 is closed with _cdc_operation=DELETE
    m002f01 = tbl.filter("member_no = 'M002' AND fund_no = 'F01'").collect()
    assert len(m002f01) == 1, f"Expected 1 row for M002/F01, got {len(m002f01)}"
    deleted = m002f01[0]
    assert deleted["_is_current"] == False, "M002/F01 should be _is_current=false after delete"
    assert deleted["_effective_to"] is not None, "M002/F01 should have _effective_to set after delete"
    assert deleted["_cdc_operation"] == "DELETE", f"M002/F01 _cdc_operation should be DELETE, got {deleted['_cdc_operation']}"
    print(f"   M002/F01: closed with _cdc_operation=DELETE, _is_current=false")

    # Verify no new version was inserted for the deleted record
    m002_current = tbl.filter("member_no = 'M002' AND _is_current = true").count()
    assert m002_current == 0, f"M002 should have no current rows after delete, got {m002_current}"
    print("   No new version inserted for deleted record")

    _print_table(spark, TABLE_NAME)
    print("   PASS")
    print()

    # ==================================================================
    # Day 4 — Insert new M004/F02
    # ==================================================================
    print(f"[4/{total_steps}] Day 4 — Insert new M004/F02...")

    os.remove(day3_file)
    day4_records = [
        {"member_no": "M004", "fund_no": "F02", "start_date": "2025-01-01", "end_date": "2025-12-31", "package_code": "PKG_SILVER", "_operation": "INSERT"},
    ]
    day4_file = os.path.join(landing, "day4.json")
    _write_json_file(day4_file, day4_records)

    df_day4 = spark.read.json(landing)
    rows = writer.write_batch(df_day4)
    print(f"   Rows processed: {rows}")

    tbl = spark.table(TABLE_NAME)
    total = tbl.count()
    current = tbl.filter("_is_current = true").count()
    print(f"   Total: {total}, Current: {current}")

    assert total == 7, f"Expected 7 total, got {total}"
    assert current == 5, f"Expected 5 current, got {current}"

    # Verify M004/F02 exists and is current
    m004f02 = tbl.filter("member_no = 'M004' AND fund_no = 'F02' AND _is_current = true").first()
    assert m004f02 is not None, "M004/F02 should exist as current"
    assert m004f02["package_code"] == "PKG_SILVER", f"M004/F02 package_code should be PKG_SILVER, got {m004f02['package_code']}"
    assert m004f02["_cdc_operation"] == "INSERT", f"M004/F02 _cdc_operation should be INSERT, got {m004f02['_cdc_operation']}"
    print(f"   M004/F02: package_code=PKG_SILVER, _cdc_operation=INSERT, _is_current=true")

    _print_table(spark, TABLE_NAME)
    print("   PASS")
    print()

    # ==================================================================
    # Final Summary
    # ==================================================================
    print("=" * 70)
    print("FINAL TABLE STATE")
    print("=" * 70)

    tbl = spark.table(TABLE_NAME)
    total = tbl.count()
    current = tbl.filter("_is_current = true").count()
    historical = tbl.filter("_is_current = false").count()

    print(f"Total rows: {total} (current: {current}, historical: {historical})")
    print()
    print("Current records:")
    tbl.filter("_is_current = true").select(
        "member_no", "fund_no", "package_code", "_cdc_operation",
    ).orderBy("member_no", "fund_no").show(truncate=False)

    print("Historical records:")
    tbl.filter("_is_current = false").select(
        "member_no", "fund_no", "package_code", "_cdc_operation", "_effective_to",
    ).orderBy("member_no", "fund_no").show(truncate=False)

    # Final assertions
    assert total == 7, f"Final: expected 7 total, got {total}"
    assert current == 5, f"Final: expected 5 current, got {current}"
    assert historical == 2, f"Final: expected 2 historical, got {historical}"

    # Verify exact current record set
    current_keys = set()
    for row in tbl.filter("_is_current = true").select("member_no", "fund_no").collect():
        current_keys.add((row["member_no"], row["fund_no"]))
    expected_keys = {("M001", "F01"), ("M001", "F02"), ("M003", "F01"), ("M003", "F03"), ("M004", "F02")}
    assert current_keys == expected_keys, f"Current keys mismatch: {current_keys} != {expected_keys}"
    print("Current record keys verified")

    # Verify historical records
    hist_rows = tbl.filter("_is_current = false").select(
        "member_no", "fund_no", "_cdc_operation",
    ).collect()
    hist_ops = {(r["member_no"], r["fund_no"]): r["_cdc_operation"] for r in hist_rows}
    assert hist_ops[("M001", "F01")] == "UPDATE", f"M001/F01 historical should be UPDATE, got {hist_ops[('M001', 'F01')]}"
    assert hist_ops[("M002", "F01")] == "DELETE", f"M002/F01 historical should be DELETE, got {hist_ops[('M002', 'F01')]}"
    print("Historical record operations verified")

    print()
    print("=" * 70)
    print("ALL MEMBER PACKAGE SCD2 TESTS PASSED")
    print("=" * 70)
    print()
    print("  Day 1: 5 records loaded (all INSERT, all current)")
    print("  Day 2: M001/F01 updated (old closed, new version created)")
    print("  Day 3: M002/F01 soft deleted (closed with _cdc_operation=DELETE)")
    print("  Day 4: M004/F02 inserted (new current record)")
    print(f"  Final: {total} total rows, {current} current, {historical} historical")
    print()

    # Cleanup
    spark.sql("DROP DATABASE IF EXISTS bronze_test CASCADE")
    spark.stop()
    shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
