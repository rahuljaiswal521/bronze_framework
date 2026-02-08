# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Framework — Unity Catalog Setup
# MAGIC One-time setup notebook to create catalog, schemas, and audit table.

# COMMAND ----------

# MAGIC %pip install /Workspace/bronze_framework/dist/bronze_framework-1.0.0-py3-none-any.whl

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
env = dbutils.widgets.get("environment")

# COMMAND ----------

from bronze_framework.config.loader import ConfigLoader

loader = ConfigLoader(env=env, conf_dir="/Workspace/bronze_framework/conf")
env_vars = loader.env_vars

catalog = env_vars["catalog"]
print(f"Setting up catalog: {catalog} for environment: {env}")

# COMMAND ----------

# Create catalog (requires account admin or CREATE CATALOG privilege)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_meta")

print(f"Created schemas: {catalog}.bronze, {catalog}.bronze_meta")

# COMMAND ----------

# Create audit log table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze_meta.ingestion_audit_log (
    source_name STRING,
    source_type STRING,
    target_table STRING,
    status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_read LONG,
    records_written LONG,
    records_quarantined LONG,
    watermark_value STRING,
    error_message STRING,
    load_type STRING,
    run_id STRING,
    environment STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")

print(f"Created audit table: {catalog}.bronze_meta.ingestion_audit_log")

# COMMAND ----------

# Verify setup
display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE {catalog}.bronze_meta.ingestion_audit_log"))
