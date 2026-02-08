"""Tests for config loader and YAML parsing."""

from __future__ import annotations

import pytest
import yaml

from bronze_framework.config.loader import ConfigLoader
from bronze_framework.config.models import LoadType, SourceType


class TestConfigLoader:
    """Tests for ConfigLoader class."""

    def test_load_env_config(self, tmp_conf_dir):
        loader = ConfigLoader(env="dev", conf_dir=str(tmp_conf_dir))
        assert loader.env_vars["catalog"] == "test_catalog"
        assert loader.env_vars["environment"] == "dev"

    def test_resolve_variables(self, tmp_conf_dir):
        loader = ConfigLoader(env="dev", conf_dir=str(tmp_conf_dir))
        config = loader.load_source("jdbc_test.yaml")
        assert config.target.catalog == "test_catalog"
        assert config.connection.secret_scope == "test-secrets"

    def test_load_source_parses_types(self, tmp_conf_dir):
        loader = ConfigLoader(env="dev", conf_dir=str(tmp_conf_dir))
        config = loader.load_source("jdbc_test.yaml")
        assert config.source_type == SourceType.JDBC
        assert config.extract.load_type == LoadType.INCREMENTAL
        assert config.name == "test_orders"

    def test_load_source_connection(self, tmp_conf_dir):
        loader = ConfigLoader(env="dev", conf_dir=str(tmp_conf_dir))
        config = loader.load_source("jdbc_test.yaml")
        assert config.connection.host == "localhost"
        assert config.connection.port == 5432
        assert config.connection.database == "testdb"

    def test_load_source_watermark(self, tmp_conf_dir):
        loader = ConfigLoader(env="dev", conf_dir=str(tmp_conf_dir))
        config = loader.load_source("jdbc_test.yaml")
        assert config.extract.watermark is not None
        assert config.extract.watermark.column == "updated_at"
        assert config.extract.watermark.type == "timestamp"

    def test_load_source_target(self, tmp_conf_dir):
        loader = ConfigLoader(env="dev", conf_dir=str(tmp_conf_dir))
        config = loader.load_source("jdbc_test.yaml")
        assert config.target.table == "test_orders"
        assert config.target.partition_by == ["_ingest_date"]
        assert len(config.target.metadata_columns) == 2

    def test_load_all_sources_filters_disabled(self, tmp_conf_dir):
        loader = ConfigLoader(env="dev", conf_dir=str(tmp_conf_dir))
        configs = loader.load_all_sources()
        names = [c.name for c in configs]
        assert "test_orders" in names
        assert "disabled_source" not in names

    def test_load_all_sources_with_filter(self, tmp_conf_dir):
        loader = ConfigLoader(env="dev", conf_dir=str(tmp_conf_dir))
        configs = loader.load_all_sources(source_filter="jdbc")
        assert len(configs) == 1
        assert configs[0].name == "test_orders"

    def test_full_table_name(self, tmp_conf_dir):
        loader = ConfigLoader(env="dev", conf_dir=str(tmp_conf_dir))
        config = loader.load_source("jdbc_test.yaml")
        assert config.full_table_name == "test_catalog.bronze.test_orders"

    def test_schema_evolution_defaults(self, tmp_conf_dir):
        loader = ConfigLoader(env="dev", conf_dir=str(tmp_conf_dir))
        config = loader.load_source("jdbc_test.yaml")
        assert config.target.schema_evolution.mode.value == "merge"

    def test_missing_env_file_uses_empty(self, tmp_path):
        (tmp_path / "sources").mkdir()
        (tmp_path / "environments").mkdir()
        loader = ConfigLoader(env="nonexistent", conf_dir=str(tmp_path))
        assert loader.env_vars == {}

    def test_variable_not_resolved_kept_as_is(self, tmp_conf_dir):
        loader = ConfigLoader(env="dev", conf_dir=str(tmp_conf_dir))
        result = loader._resolve_vars("${undefined_var}")
        assert result == "${undefined_var}"

    def test_load_api_source(self, tmp_conf_dir):
        api_source = {
            "name": "test_api",
            "source_type": "api",
            "extract": {
                "load_type": "full",
                "base_url": "https://api.example.com",
                "endpoint": "/data",
                "auth": {
                    "type": "bearer",
                    "secret_scope": "test-secrets",
                    "secret_key_token": "api-token",
                },
                "pagination": {
                    "type": "offset",
                    "page_size": 200,
                },
            },
            "target": {
                "catalog": "test_catalog",
                "schema": "bronze",
                "table": "api_data",
            },
        }
        sources_dir = tmp_conf_dir / "sources"
        with open(sources_dir / "api_test.yaml", "w") as f:
            yaml.dump(api_source, f)

        loader = ConfigLoader(env="dev", conf_dir=str(tmp_conf_dir))
        config = loader.load_source("api_test.yaml")
        assert config.source_type == SourceType.API
        assert config.extract.auth is not None
        assert config.extract.auth.type.value == "bearer"
        assert config.extract.pagination is not None
        assert config.extract.pagination.page_size == 200
