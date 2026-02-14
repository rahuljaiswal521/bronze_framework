# Databricks notebook source
# MAGIC %md
# MAGIC # Member Package SCD2 — End-to-End Test
# MAGIC
# MAGIC Tests the Bronze Framework's SCD2 pipeline with a `member_package` source
# MAGIC simulating Oracle CDC log files with composite primary key (`member_no` + `fund_no`).
# MAGIC
# MAGIC | Day | Action | Expected |
# MAGIC |-----|--------|----------|
# MAGIC | 1 | Initial load (5 records) | 5 current rows |
# MAGIC | 2 | Update M001/F01 | 6 total (5 current + 1 historical) |
# MAGIC | 3 | Delete M002/F01 | 6 total (4 current + 2 historical) |
# MAGIC | 4 | Insert M004/F02 | 7 total (5 current + 2 historical) |

# COMMAND ----------

# MAGIC %pip install /Workspace/bronze_framework/dist/bronze_framework-1.0.0-py3-none-any.whl

# COMMAND ----------

import json

# Configuration
CATALOG = "dev"
SCHEMA = "bronze"
TABLE = "member_package"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing_data/member_package_test"

print(f"Table: {TABLE_NAME}")
print(f"Landing: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Clean slate

# COMMAND ----------

# Drop table if exists from prior runs
spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
print(f"Dropped {TABLE_NAME} (if existed)")

# Clear landing directory
dbutils.fs.rm(VOLUME_PATH, recurse=True)
dbutils.fs.mkdirs(VOLUME_PATH)
print(f"Cleared landing directory: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Framework setup

# COMMAND ----------

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

config = SourceConfig(
    name="member_package_scd2_test",
    source_type=SourceType.FILE,
    description="SCD2 test - member package with composite PK and soft deletes",
    extract=ExtractConfig(
        load_type=LoadType.INCREMENTAL,
        path=VOLUME_PATH,
        format="json",
        auto_loader=False,
        format_options={"multiLine": "false"},
    ),
    target=TargetConfig(
        catalog=CATALOG,
        schema=SCHEMA,
        table=TABLE,
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
            path=VOLUME_PATH,
            retention_days=10,
            cleanup_enabled=False,
        ),
    ),
)

writer = DeltaWriter(spark, config)
print("DeltaWriter initialized")
print(f"  Primary keys: {config.target.cdc.primary_keys}")
print(f"  Delete condition: {config.target.cdc.delete_condition_column} = {config.target.cdc.delete_condition_value}")

# COMMAND ----------

def write_json_to_volume(records, filename):
    """Write records as newline-delimited JSON to the volume landing path."""
    # Clear landing directory first
    dbutils.fs.rm(VOLUME_PATH, recurse=True)
    dbutils.fs.mkdirs(VOLUME_PATH)
    # Write NDJSON
    content = "\n".join(json.dumps(r) for r in records)
    dbutils.fs.put(f"{VOLUME_PATH}/{filename}", content, overwrite=True)
    print(f"  Wrote {len(records)} records to {VOLUME_PATH}/{filename}")


def show_table():
    """Display the full SCD2 table state."""
    df = spark.table(TABLE_NAME)
    df.select(
        "member_no", "fund_no", "start_date", "end_date", "package_code",
        "_operation", "_is_current", "_cdc_operation",
        "_effective_from", "_effective_to",
    ).orderBy("member_no", "fund_no", "_effective_from").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 1 — Initial Load (5 records)

# COMMAND ----------

day1_records = [
    {"member_no": "M001", "fund_no": "F01", "start_date": "2024-01-01", "end_date": "2024-12-31", "package_code": "PKG_GOLD", "_operation": "INSERT"},
    {"member_no": "M001", "fund_no": "F02", "start_date": "2024-03-01", "end_date": "2024-12-31", "package_code": "PKG_SILVER", "_operation": "INSERT"},
    {"member_no": "M002", "fund_no": "F01", "start_date": "2024-01-15", "end_date": "2025-01-14", "package_code": "PKG_BRONZE", "_operation": "INSERT"},
    {"member_no": "M003", "fund_no": "F01", "start_date": "2024-06-01", "end_date": "2025-05-31", "package_code": "PKG_GOLD", "_operation": "INSERT"},
    {"member_no": "M003", "fund_no": "F03", "start_date": "2024-07-01", "end_date": "2025-06-30", "package_code": "PKG_PLATINUM", "_operation": "INSERT"},
]
write_json_to_volume(day1_records, "day1.json")

df_day1 = spark.read.json(VOLUME_PATH)
rows = writer.write_batch(df_day1)
print(f"Rows processed: {rows}")

tbl = spark.table(TABLE_NAME)
total = tbl.count()
current = tbl.filter("_is_current = true").count()
inserts = tbl.filter("_cdc_operation = 'INSERT'").count()

print(f"Total: {total}, Current: {current}, INSERTs: {inserts}")
assert total == 5, f"Expected 5 total, got {total}"
assert current == 5, f"Expected 5 current, got {current}"
assert inserts == 5, f"Expected 5 INSERTs, got {inserts}"
print("Day 1 PASSED")

# COMMAND ----------

show_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 2 — Update M001/F01 (extend end_date, upgrade to PLATINUM)

# COMMAND ----------

day2_records = [
    {"member_no": "M001", "fund_no": "F01", "start_date": "2024-01-01", "end_date": "2025-12-31", "package_code": "PKG_PLATINUM", "_operation": "UPDATE"},
]
write_json_to_volume(day2_records, "day2.json")

df_day2 = spark.read.json(VOLUME_PATH)
rows = writer.write_batch(df_day2)
print(f"Rows processed: {rows}")

tbl = spark.table(TABLE_NAME)
total = tbl.count()
current = tbl.filter("_is_current = true").count()
historical = tbl.filter("_is_current = false").count()

print(f"Total: {total}, Current: {current}, Historical: {historical}")
assert total == 6, f"Expected 6 total, got {total}"
assert current == 5, f"Expected 5 current, got {current}"
assert historical == 1, f"Expected 1 historical, got {historical}"

# Verify M001/F01 has 2 versions
m001f01 = tbl.filter("member_no = 'M001' AND fund_no = 'F01'").orderBy("_effective_from").collect()
assert len(m001f01) == 2, f"Expected 2 rows for M001/F01, got {len(m001f01)}"

old, new = m001f01[0], m001f01[1]
assert old["_is_current"] == False
assert old["package_code"] == "PKG_GOLD"
assert old["_cdc_operation"] == "UPDATE"
assert new["_is_current"] == True
assert new["package_code"] == "PKG_PLATINUM"
assert new["end_date"] == "2025-12-31"
assert new["_cdc_operation"] == "UPDATE"
print("Day 2 PASSED")

# COMMAND ----------

show_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 3 — Delete M002/F01 (soft delete)

# COMMAND ----------

day3_records = [
    {"member_no": "M002", "fund_no": "F01", "start_date": "2024-01-15", "end_date": "2025-01-14", "package_code": "PKG_BRONZE", "_operation": "DELETE"},
]
write_json_to_volume(day3_records, "day3.json")

df_day3 = spark.read.json(VOLUME_PATH)
rows = writer.write_batch(df_day3)
print(f"Rows processed: {rows}")

tbl = spark.table(TABLE_NAME)
total = tbl.count()
current = tbl.filter("_is_current = true").count()

print(f"Total: {total}, Current: {current}")
assert total == 6, f"Expected 6 total, got {total}"
assert current == 4, f"Expected 4 current, got {current}"

# Verify M002/F01 is closed with DELETE
m002f01 = tbl.filter("member_no = 'M002' AND fund_no = 'F01'").collect()
assert len(m002f01) == 1
deleted = m002f01[0]
assert deleted["_is_current"] == False
assert deleted["_cdc_operation"] == "DELETE"
assert deleted["_effective_to"] is not None

# No current row for M002
m002_current = tbl.filter("member_no = 'M002' AND _is_current = true").count()
assert m002_current == 0
print("Day 3 PASSED")

# COMMAND ----------

show_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 4 — Insert new M004/F02

# COMMAND ----------

day4_records = [
    {"member_no": "M004", "fund_no": "F02", "start_date": "2025-01-01", "end_date": "2025-12-31", "package_code": "PKG_SILVER", "_operation": "INSERT"},
]
write_json_to_volume(day4_records, "day4.json")

df_day4 = spark.read.json(VOLUME_PATH)
rows = writer.write_batch(df_day4)
print(f"Rows processed: {rows}")

tbl = spark.table(TABLE_NAME)
total = tbl.count()
current = tbl.filter("_is_current = true").count()

print(f"Total: {total}, Current: {current}")
assert total == 7, f"Expected 7 total, got {total}"
assert current == 5, f"Expected 5 current, got {current}"

# Verify M004/F02
m004f02 = tbl.filter("member_no = 'M004' AND fund_no = 'F02' AND _is_current = true").first()
assert m004f02 is not None
assert m004f02["package_code"] == "PKG_SILVER"
assert m004f02["_cdc_operation"] == "INSERT"
print("Day 4 PASSED")

# COMMAND ----------

show_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Verification

# COMMAND ----------

tbl = spark.table(TABLE_NAME)
total = tbl.count()
current = tbl.filter("_is_current = true").count()
historical = tbl.filter("_is_current = false").count()

print(f"Total rows: {total} (current: {current}, historical: {historical})")

# Verify exact current record set
current_keys = set()
for row in tbl.filter("_is_current = true").select("member_no", "fund_no").collect():
    current_keys.add((row["member_no"], row["fund_no"]))

expected_keys = {("M001", "F01"), ("M001", "F02"), ("M003", "F01"), ("M003", "F03"), ("M004", "F02")}
assert current_keys == expected_keys, f"Current keys mismatch: {current_keys} != {expected_keys}"
print("Current record keys: VERIFIED")

# Verify historical records
hist_rows = tbl.filter("_is_current = false").select("member_no", "fund_no", "_cdc_operation").collect()
hist_ops = {(r["member_no"], r["fund_no"]): r["_cdc_operation"] for r in hist_rows}
assert hist_ops[("M001", "F01")] == "UPDATE"
assert hist_ops[("M002", "F01")] == "DELETE"
print("Historical record operations: VERIFIED")

assert total == 7
assert current == 5
assert historical == 2

print("\n" + "=" * 60)
print("ALL MEMBER PACKAGE SCD2 TESTS PASSED")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional — uncomment to drop test table)

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
# dbutils.fs.rm(VOLUME_PATH, recurse=True)
# print("Cleanup complete")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS: All 4 SCD2 test days passed — 7 rows, 5 current, 2 historical")
