# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Framework — Full Ingestion Orchestrator
# MAGIC Runs all enabled sources (or a filtered subset) through the ingestion pipeline.

# COMMAND ----------

# MAGIC %pip install /Workspace/bronze_framework/dist/bronze_framework-1.0.0-py3-none-any.whl

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("source_filter", "", "Source Filter (optional)")
dbutils.widgets.text("conf_dir", "/Workspace/bronze_framework/conf", "Config Directory")

env = dbutils.widgets.get("environment")
source_filter = dbutils.widgets.get("source_filter") or None
conf_dir = dbutils.widgets.get("conf_dir")

print(f"Environment: {env}")
print(f"Source filter: {source_filter}")

# COMMAND ----------

from bronze_framework.config.loader import ConfigLoader
from bronze_framework.engine.ingestion_engine import IngestionEngine

loader = ConfigLoader(env=env, conf_dir=conf_dir)
configs = loader.load_all_sources(source_filter=source_filter)

print(f"Found {len(configs)} source(s) to ingest:")
for cfg in configs:
    print(f"  - {cfg.name} ({cfg.source_type.value})")

# COMMAND ----------

engine = IngestionEngine(spark=spark, environment=env)
results = []

for config in configs:
    print(f"\n{'='*60}")
    print(f"Processing: {config.name}")
    print(f"{'='*60}")

    result = engine.ingest(config)
    results.append(result)

    status = "SUCCESS" if result.success else "FAILURE"
    print(f"  Status: {status}")
    print(f"  Records read: {result.records_read}")
    print(f"  Records written: {result.records_written}")
    print(f"  Records quarantined: {result.records_quarantined}")
    if result.error:
        print(f"  Error: {result.error}")

# COMMAND ----------

# Summary
print("\n" + "=" * 60)
print("INGESTION SUMMARY")
print("=" * 60)

succeeded = sum(1 for r in results if r.success)
failed = sum(1 for r in results if not r.success)

print(f"Total sources: {len(results)}")
print(f"Succeeded: {succeeded}")
print(f"Failed: {failed}")

if failed > 0:
    print("\nFailed sources:")
    for r in results:
        if not r.success:
            print(f"  - {r.source_name}: {r.error}")

# COMMAND ----------

# Fail the job if any source failed
if failed > 0:
    dbutils.notebook.exit(
        f"FAILURE: {failed}/{len(results)} sources failed"
    )
else:
    dbutils.notebook.exit(
        f"SUCCESS: {succeeded}/{len(results)} sources completed"
    )
