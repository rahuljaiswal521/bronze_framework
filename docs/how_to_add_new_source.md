# How to Add a New Source to the Bronze Framework

The framework is **metadata-driven** — adding a new source requires **zero code changes**. You just create one YAML file.

## Steps to Add a New Source

### Step 1: Create a YAML config file

Create a new file in `conf/sources/` — e.g., `conf/sources/file_inventory.yaml`:

```yaml
name: inventory
source_type: file
description: "Daily inventory snapshot from warehouse system"
enabled: true
tags:
  domain: supply_chain
  source_system: warehouse
  priority: medium

extract:
  load_type: incremental
  path: ${storage_base}/landing/inventory/
  format: json                    # json, parquet, csv, etc.
  auto_loader: false
  format_options:
    multiLine: "false"
    # For CSV: header: "true", delimiter: ","

target:
  catalog: ${catalog}
  schema: bronze
  table: inventory_snapshots      # <- your bronze table name
  partition_by:
    - _ingest_date
  table_properties:
    delta.autoOptimize.optimizeWrite: "true"
  metadata_columns:
    - name: _ingest_timestamp
      expression: "current_timestamp()"
    - name: _ingest_date
      expression: "current_date()"
    - name: _source_system
      expression: "lit('warehouse')"
    - name: _source_file
      expression: "input_file_name()"
  schema_evolution:
    mode: merge                   # merge | strict | rescue
  quality:
    enabled: true
    quarantine_threshold_pct: 5.0
  cdc:
    enabled: true
    mode: scd2                    # scd2 | upsert | append
    primary_keys:
      - item_id                   # <- your primary key column(s)
      - warehouse_id
    sequence_column: updated_at   # <- for dedup within same batch
    exclude_columns_from_hash:
      - _ingest_timestamp
      - _ingest_date
      - _source_file
      - updated_at
  landing:
    path: ${storage_base}/landing/inventory/
    retention_days: 10
    cleanup_enabled: true
```

### Step 2: Drop source files in the landing folder

Place your data files in the landing path (e.g., `${storage_base}/landing/inventory/`).

### Step 3: Run the ingestion

**Single source** (for testing):
```python
# notebooks/02_run_single_source.py
# Set widget: source_name = "inventory", environment = "dev"
```

**All sources** (scheduled):
```python
# notebooks/01_run_ingestion.py — picks up all enabled YAMLs automatically
```

### That's it.

The framework will automatically:
1. Read files from the landing path
2. Add metadata columns (`_ingest_timestamp`, `_source_system`, etc.)
3. Compute `_record_hash` for change detection
4. **First run**: Create the bronze table with SCD2 columns (`_effective_from`, `_effective_to`, `_is_current`, `_record_hash`, `_cdc_operation`)
5. **Subsequent runs**: MERGE — insert new records, close+version changed records, skip unchanged
6. Clean up landing files older than 10 days

---

## Key Decisions Per Source

| Setting | Options | When to use |
|---------|---------|-------------|
| `source_type` | `file`, `jdbc`, `api`, `stream` | Depends on your source system |
| `cdc.mode` | `scd2`, `upsert`, `append` | `scd2` = full history, `upsert` = latest only, `append` = raw append |
| `cdc.primary_keys` | list of columns | The business key that uniquely identifies a record |
| `schema_evolution.mode` | `merge`, `strict`, `rescue` | `merge` = auto-add new columns, `strict` = fail on new columns, `rescue` = put unknown fields in `_rescued_data` |
| `quality.quarantine_threshold_pct` | float | Fail if bad records exceed this % |

## Example for Other Source Types

### JDBC — Database table

Just change the top section:

```yaml
name: erp_customers
source_type: jdbc
description: "ERP customer master table"
enabled: true

connection:
  host: db-server.company.com
  port: 5432
  database: mydb
  driver: org.postgresql.Driver
  secret_scope: ${secret_scope}
  secret_key_user: db-user
  secret_key_password: db-password

extract:
  load_type: incremental
  table: public.my_table
  partition_column: id
  num_partitions: 8
  watermark:
    column: updated_at
    type: timestamp
    default_value: "2020-01-01T00:00:00"

target:
  catalog: ${catalog}
  schema: bronze
  table: erp_customers
  # ... same target section as above
```

### API — REST endpoint

```yaml
name: payment_transactions
source_type: api
description: "Payment gateway REST API"
enabled: true

extract:
  load_type: incremental
  base_url: https://api.example.com
  endpoint: /v2/records
  method: GET
  auth:
    type: oauth2
    secret_scope: ${secret_scope}
    secret_key_client_id: api-client-id
    secret_key_client_secret: api-client-secret
    token_url: https://auth.example.com/token
  pagination:
    type: cursor
    page_size: 500
    cursor_param: cursor
    cursor_response_path: meta.next_cursor

target:
  catalog: ${catalog}
  schema: bronze
  table: payment_transactions
  # ... same target section as above
```

### Stream — Kafka topic

```yaml
name: user_events
source_type: stream
description: "Real-time user activity events from Kafka"
enabled: true

extract:
  load_type: incremental
  kafka_bootstrap_servers: kafka-broker1:9092,kafka-broker2:9092
  kafka_topic: user-activity-events
  kafka_consumer_group: bronze-ingestion
  starting_offsets: earliest
  checkpoint_path: ${checkpoint_base}/user_events/

target:
  catalog: ${catalog}
  schema: bronze
  table: user_activity_events
  # ... same target section as above
  cdc:
    enabled: false                # streaming is typically append-only
    mode: append
```

The `target` section stays the same pattern across all source types.

## Table Creation

You do **not** need to manually create the bronze table. The framework creates it automatically on first run:
- Infers schema from your source data
- Adds all metadata columns (`_ingest_timestamp`, `_ingest_date`, etc.)
- Adds SCD2 columns (`_effective_from`, `_effective_to`, `_is_current`, `_record_hash`, `_cdc_operation`)

The only prerequisite is that the **catalog and schema** exist. Run `notebooks/00_setup_catalog.py` once to set those up.
