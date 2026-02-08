"""YAML config loader with environment variable resolution."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from bronze_framework.config.models import (
    AuthConfig,
    AuthType,
    CdcConfig,
    CdcMode,
    ConnectionConfig,
    ExtractConfig,
    LandingConfig,
    LoadType,
    MetadataColumn,
    PaginationConfig,
    PaginationType,
    QualityConfig,
    SchemaEvolutionConfig,
    SchemaEvolutionMode,
    SourceConfig,
    SourceType,
    TargetConfig,
    WatermarkConfig,
)

_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


class ConfigLoader:
    """Loads and resolves YAML-based source configurations."""

    def __init__(self, env: str = "dev", conf_dir: Optional[str] = None):
        self.env = env
        self.conf_dir = Path(conf_dir) if conf_dir else Path("conf")
        self.env_vars: Dict[str, str] = {}
        self._load_env_config()

    def _load_env_config(self) -> None:
        env_file = self.conf_dir / "environments" / f"{self.env}.yaml"
        if env_file.exists():
            with open(env_file, "r") as f:
                self.env_vars = yaml.safe_load(f) or {}

    def _resolve_vars(self, value: Any) -> Any:
        """Recursively resolve ${variable} placeholders from env config."""
        if isinstance(value, str):
            def replacer(match: re.Match) -> str:
                key = match.group(1)
                return str(self.env_vars.get(key, os.environ.get(key, match.group(0))))
            return _VAR_PATTERN.sub(replacer, value)
        if isinstance(value, dict):
            return {k: self._resolve_vars(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._resolve_vars(item) for item in value]
        return value

    def load_source(self, source_path: str) -> SourceConfig:
        """Load a single source YAML and parse into SourceConfig."""
        path = Path(source_path)
        if not path.is_absolute():
            path = self.conf_dir / "sources" / path
        with open(path, "r") as f:
            raw = yaml.safe_load(f)
        resolved = self._resolve_vars(raw)
        return self._parse_source(resolved)

    def load_all_sources(self, source_filter: Optional[str] = None) -> List[SourceConfig]:
        """Load all source YAMLs from the sources directory."""
        sources_dir = self.conf_dir / "sources"
        configs: List[SourceConfig] = []
        for yaml_file in sorted(sources_dir.glob("*.yaml")):
            if source_filter and source_filter not in yaml_file.stem:
                continue
            cfg = self.load_source(str(yaml_file))
            if cfg.enabled:
                configs.append(cfg)
        return configs

    def _parse_source(self, data: Dict[str, Any]) -> SourceConfig:
        """Parse raw dict into typed SourceConfig."""
        connection = self._parse_connection(data.get("connection", {}))
        extract = self._parse_extract(data.get("extract", {}))
        target = self._parse_target(data.get("target", {}))

        return SourceConfig(
            name=data["name"],
            source_type=SourceType(data["source_type"]),
            description=data.get("description", ""),
            enabled=data.get("enabled", True),
            tags=data.get("tags", {}),
            connection=connection,
            extract=extract,
            target=target,
        )

    def _parse_connection(self, data: Dict[str, Any]) -> ConnectionConfig:
        return ConnectionConfig(
            host=data.get("host"),
            port=data.get("port"),
            database=data.get("database"),
            driver=data.get("driver"),
            url=data.get("url"),
            secret_scope=data.get("secret_scope"),
            secret_key_user=data.get("secret_key_user"),
            secret_key_password=data.get("secret_key_password"),
            properties=data.get("properties", {}),
        )

    def _parse_extract(self, data: Dict[str, Any]) -> ExtractConfig:
        watermark = None
        if "watermark" in data:
            w = data["watermark"]
            watermark = WatermarkConfig(
                column=w.get("column", ""),
                type=w.get("type", "timestamp"),
                default_value=w.get("default_value"),
            )

        auth = None
        if "auth" in data:
            a = data["auth"]
            auth = AuthConfig(
                type=AuthType(a.get("type", "none")),
                secret_scope=a.get("secret_scope"),
                secret_key_token=a.get("secret_key_token"),
                secret_key_client_id=a.get("secret_key_client_id"),
                secret_key_client_secret=a.get("secret_key_client_secret"),
                token_url=a.get("token_url"),
                header_name=a.get("header_name", "Authorization"),
                header_prefix=a.get("header_prefix", "Bearer"),
            )

        pagination = None
        if "pagination" in data:
            p = data["pagination"]
            pagination = PaginationConfig(
                type=PaginationType(p.get("type", "offset")),
                page_size=p.get("page_size", 100),
                max_pages=p.get("max_pages"),
                offset_param=p.get("offset_param", "offset"),
                limit_param=p.get("limit_param", "limit"),
                cursor_param=p.get("cursor_param", "cursor"),
                cursor_response_path=p.get("cursor_response_path"),
                data_response_path=p.get("data_response_path", "data"),
            )

        return ExtractConfig(
            load_type=LoadType(data.get("load_type", "full")),
            watermark=watermark,
            query=data.get("query"),
            table=data.get("table"),
            partition_column=data.get("partition_column"),
            num_partitions=data.get("num_partitions", 1),
            fetch_size=data.get("fetch_size", 10000),
            path=data.get("path"),
            format=data.get("format", "parquet"),
            format_options=data.get("format_options", {}),
            auto_loader=data.get("auto_loader", False),
            checkpoint_path=data.get("checkpoint_path"),
            base_url=data.get("base_url"),
            endpoint=data.get("endpoint"),
            method=data.get("method", "GET"),
            headers=data.get("headers", {}),
            params=data.get("params", {}),
            auth=auth,
            pagination=pagination,
            max_retries=data.get("max_retries", 3),
            retry_backoff_factor=data.get("retry_backoff_factor", 2.0),
            timeout_seconds=data.get("timeout_seconds", 30),
            response_root_path=data.get("response_root_path", "data"),
            kafka_bootstrap_servers=data.get("kafka_bootstrap_servers"),
            kafka_topic=data.get("kafka_topic"),
            kafka_consumer_group=data.get("kafka_consumer_group"),
            kafka_options=data.get("kafka_options", {}),
            event_hub_connection_string_key=data.get("event_hub_connection_string_key"),
            event_hub_consumer_group=data.get("event_hub_consumer_group", "$Default"),
            starting_offsets=data.get("starting_offsets", "earliest"),
        )

    def _parse_target(self, data: Dict[str, Any]) -> TargetConfig:
        metadata_columns = [
            MetadataColumn(name=m["name"], expression=m["expression"])
            for m in data.get("metadata_columns", [])
        ]

        schema_evo_data = data.get("schema_evolution", {})
        schema_evolution = SchemaEvolutionConfig(
            mode=SchemaEvolutionMode(schema_evo_data.get("mode", "merge")),
            rescued_data_column=schema_evo_data.get(
                "rescued_data_column", "_rescued_data"
            ),
        )

        quality_data = data.get("quality", {})
        quality = QualityConfig(
            enabled=quality_data.get("enabled", True),
            dead_letter_table_suffix=quality_data.get(
                "dead_letter_table_suffix", "dead_letter"
            ),
            quarantine_threshold_pct=quality_data.get(
                "quarantine_threshold_pct", 10.0
            ),
        )

        cdc_data = data.get("cdc", {})
        cdc = CdcConfig(
            enabled=cdc_data.get("enabled", False),
            mode=CdcMode(cdc_data.get("mode", "append")),
            primary_keys=cdc_data.get("primary_keys", []),
            sequence_column=cdc_data.get("sequence_column"),
            exclude_columns_from_hash=cdc_data.get(
                "exclude_columns_from_hash", []
            ),
            delete_condition_column=cdc_data.get("delete_condition_column"),
            delete_condition_value=cdc_data.get("delete_condition_value"),
        )

        landing_data = data.get("landing", {})
        landing = LandingConfig(
            path=landing_data.get("path"),
            archive_path=landing_data.get("archive_path"),
            retention_days=landing_data.get("retention_days", 10),
            cleanup_enabled=landing_data.get("cleanup_enabled", True),
        )

        return TargetConfig(
            catalog=data.get("catalog", ""),
            schema=data.get("schema", "bronze"),
            table=data.get("table", ""),
            partition_by=data.get("partition_by", []),
            z_order_by=data.get("z_order_by", []),
            table_properties=data.get("table_properties", {}),
            metadata_columns=metadata_columns,
            schema_evolution=schema_evolution,
            quality=quality,
            cdc=cdc,
            landing=landing,
        )
