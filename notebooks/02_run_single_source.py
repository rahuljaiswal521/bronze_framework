# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Framework — Single Source Runner
# MAGIC Run a single source for testing or manual reruns.

# COMMAND ----------

# MAGIC %pip install /Workspace/bronze_framework/dist/bronze_framework-1.0.0-py3-none-any.whl

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("source_file", "", "Source YAML filename (e.g. jdbc_erp_orders.yaml)")
dbutils.widgets.text("conf_dir", "/Workspace/bronze_framework/conf", "Config Directory")

env = dbutils.widgets.get("environment")
source_file = dbutils.widgets.get("source_file")
conf_dir = dbutils.widgets.get("conf_dir")

if not source_file:
    dbutils.notebook.exit("ERROR: source_file parameter is required")

print(f"Environment: {env}")
print(f"Source file: {source_file}")

# COMMAND ----------

from bronze_framework.config.loader import ConfigLoader
from bronze_framework.engine.ingestion_engine import IngestionEngine

loader = ConfigLoader(env=env, conf_dir=conf_dir)
config = loader.load_source(source_file)

print(f"Source: {config.name}")
print(f"Type: {config.source_type.value}")
print(f"Target: {config.full_table_name}")
print(f"Load type: {config.extract.load_type.value}")

# COMMAND ----------

engine = IngestionEngine(spark=spark, environment=env)
result = engine.ingest(config)

# COMMAND ----------

print(f"\nResult:")
print(f"  Status: {'SUCCESS' if result.success else 'FAILURE'}")
print(f"  Records read: {result.records_read}")
print(f"  Records written: {result.records_written}")
print(f"  Records quarantined: {result.records_quarantined}")

if result.error:
    print(f"  Error: {result.error}")

# COMMAND ----------

# Show recent audit log entries for this source
display(
    spark.sql(f"""
        SELECT *
        FROM {config.audit_table_name}
        WHERE source_name = '{config.name}'
        ORDER BY start_time DESC
        LIMIT 10
    """)
)

# COMMAND ----------

if not result.success:
    dbutils.notebook.exit(f"FAILURE: {result.error}")
else:
    dbutils.notebook.exit(
        f"SUCCESS: {result.records_written} records written"
    )
