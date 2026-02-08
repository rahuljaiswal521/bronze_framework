"""Shared test fixtures for bronze framework tests."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Dict

import pytest
import yaml

from bronze_framework.config.models import (
    CdcConfig,
    CdcMode,
    ConnectionConfig,
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
    WatermarkConfig,
)


@pytest.fixture
def tmp_conf_dir(tmp_path):
    """Create a temporary config directory with env and source files."""
    sources_dir = tmp_path / "sources"
    sources_dir.mkdir()
    env_dir = tmp_path / "environments"
    env_dir.mkdir()

    # Dev environment config
    dev_config = {
        "catalog": "test_catalog",
        "environment": "dev",
        "checkpoint_base": "/tmp/checkpoints",
        "storage_base": "/tmp/storage",
        "secret_scope": "test-secrets",
    }
    with open(env_dir / "dev.yaml", "w") as f:
        yaml.dump(dev_config, f)

    # Sample JDBC source
    jdbc_source = {
        "name": "test_orders",
        "source_type": "jdbc",
        "description": "Test JDBC source",
        "enabled": True,
        "connection": {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "driver": "org.postgresql.Driver",
            "secret_scope": "${secret_scope}",
            "secret_key_user": "db-user",
            "secret_key_password": "db-password",
        },
        "extract": {
            "load_type": "incremental",
            "table": "public.orders",
            "partition_column": "order_id",
            "num_partitions": 4,
            "watermark": {
                "column": "updated_at",
                "type": "timestamp",
                "default_value": "2020-01-01T00:00:00",
            },
        },
        "target": {
            "catalog": "${catalog}",
            "schema": "bronze",
            "table": "test_orders",
            "partition_by": ["_ingest_date"],
            "metadata_columns": [
                {"name": "_ingest_timestamp", "expression": "current_timestamp()"},
                {"name": "_source", "expression": "lit('test')"},
            ],
            "schema_evolution": {"mode": "merge"},
            "quality": {"enabled": True, "quarantine_threshold_pct": 5.0},
        },
    }
    with open(sources_dir / "jdbc_test.yaml", "w") as f:
        yaml.dump(jdbc_source, f)

    # Disabled source
    disabled_source = {
        "name": "disabled_source",
        "source_type": "file",
        "enabled": False,
        "extract": {"path": "/data/disabled"},
        "target": {"catalog": "test_catalog", "schema": "bronze", "table": "disabled"},
    }
    with open(sources_dir / "disabled_source.yaml", "w") as f:
        yaml.dump(disabled_source, f)

    return tmp_path


@pytest.fixture
def sample_jdbc_config() -> SourceConfig:
    """Create a sample JDBC SourceConfig for testing."""
    return SourceConfig(
        name="test_orders",
        source_type=SourceType.JDBC,
        description="Test JDBC source",
        connection=ConnectionConfig(
            host="localhost",
            port=5432,
            database="testdb",
            driver="org.postgresql.Driver",
            secret_scope="test-secrets",
            secret_key_user="db-user",
            secret_key_password="db-password",
        ),
        extract=ExtractConfig(
            load_type=LoadType.INCREMENTAL,
            table="public.orders",
            partition_column="order_id",
            num_partitions=4,
            watermark=WatermarkConfig(
                column="updated_at",
                type="timestamp",
                default_value="2020-01-01T00:00:00",
            ),
        ),
        target=TargetConfig(
            catalog="test_catalog",
            schema="bronze",
            table="test_orders",
            partition_by=["_ingest_date"],
            metadata_columns=[
                MetadataColumn(
                    name="_ingest_timestamp", expression="current_timestamp()"
                ),
                MetadataColumn(name="_source", expression="lit('test')"),
            ],
            schema_evolution=SchemaEvolutionConfig(mode=SchemaEvolutionMode.MERGE),
            quality=QualityConfig(enabled=True, quarantine_threshold_pct=5.0),
        ),
    )


@pytest.fixture
def sample_file_config() -> SourceConfig:
    """Create a sample File SourceConfig for testing."""
    return SourceConfig(
        name="test_clickstream",
        source_type=SourceType.FILE,
        description="Test file source",
        extract=ExtractConfig(
            load_type=LoadType.INCREMENTAL,
            path="/tmp/storage/clickstream/",
            format="json",
            auto_loader=True,
            checkpoint_path="/tmp/checkpoints/clickstream/",
        ),
        target=TargetConfig(
            catalog="test_catalog",
            schema="bronze",
            table="clickstream_events",
        ),
    )


@pytest.fixture
def sample_api_config() -> SourceConfig:
    """Create a sample API SourceConfig for testing."""
    return SourceConfig(
        name="test_payments",
        source_type=SourceType.API,
        description="Test API source",
        extract=ExtractConfig(
            load_type=LoadType.FULL,
            base_url="https://api.example.com",
            endpoint="/v1/payments",
            method="GET",
            response_root_path="data",
            max_retries=2,
            timeout_seconds=10,
        ),
        target=TargetConfig(
            catalog="test_catalog",
            schema="bronze",
            table="payments",
        ),
    )


@pytest.fixture
def sample_scd2_config() -> SourceConfig:
    """Create a sample SourceConfig with SCD2 CDC enabled."""
    return SourceConfig(
        name="test_orders_scd2",
        source_type=SourceType.FILE,
        description="Test SCD2 source",
        extract=ExtractConfig(
            load_type=LoadType.INCREMENTAL,
            path="/tmp/landing/orders/",
            format="json",
            auto_loader=False,
        ),
        target=TargetConfig(
            catalog="test_catalog",
            schema="bronze",
            table="orders_scd2",
            metadata_columns=[
                MetadataColumn(
                    name="_ingest_timestamp", expression="current_timestamp()"
                ),
                MetadataColumn(name="_source_system", expression="lit('erp')"),
            ],
            schema_evolution=SchemaEvolutionConfig(mode=SchemaEvolutionMode.MERGE),
            quality=QualityConfig(enabled=True, quarantine_threshold_pct=5.0),
            cdc=CdcConfig(
                enabled=True,
                mode=CdcMode.SCD2,
                primary_keys=["order_id"],
                sequence_column="updated_at",
                exclude_columns_from_hash=[
                    "_ingest_timestamp", "_ingest_date", "_source_system", "updated_at",
                ],
            ),
            landing=LandingConfig(
                path="/tmp/landing/orders/",
                retention_days=10,
                cleanup_enabled=True,
            ),
        ),
    )


@pytest.fixture
def sample_upsert_config() -> SourceConfig:
    """Create a sample SourceConfig with UPSERT CDC enabled."""
    return SourceConfig(
        name="test_payments_upsert",
        source_type=SourceType.API,
        description="Test upsert source",
        extract=ExtractConfig(
            load_type=LoadType.INCREMENTAL,
            base_url="https://api.example.com",
            endpoint="/v1/payments",
            method="GET",
        ),
        target=TargetConfig(
            catalog="test_catalog",
            schema="bronze",
            table="payments_upsert",
            metadata_columns=[
                MetadataColumn(
                    name="_ingest_timestamp", expression="current_timestamp()"
                ),
            ],
            schema_evolution=SchemaEvolutionConfig(mode=SchemaEvolutionMode.MERGE),
            cdc=CdcConfig(
                enabled=True,
                mode=CdcMode.UPSERT,
                primary_keys=["transaction_id"],
            ),
        ),
    )
