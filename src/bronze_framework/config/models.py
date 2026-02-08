"""Dataclass models for source configuration schema."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class SourceType(str, Enum):
    JDBC = "jdbc"
    FILE = "file"
    API = "api"
    STREAM = "stream"


class LoadType(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"


class SchemaEvolutionMode(str, Enum):
    MERGE = "merge"
    STRICT = "strict"
    RESCUE = "rescue"


class CdcMode(str, Enum):
    SCD2 = "scd2"           # Full history — close old, insert new
    UPSERT = "upsert"       # Latest only — overwrite matched rows
    APPEND = "append"       # No CDC — append-only (default)


class AuthType(str, Enum):
    OAUTH2 = "oauth2"
    API_KEY = "api_key"
    BEARER = "bearer"
    NONE = "none"


class PaginationType(str, Enum):
    OFFSET = "offset"
    CURSOR = "cursor"
    LINK_HEADER = "link_header"


@dataclass
class ConnectionConfig:
    """Database or service connection details."""
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    driver: Optional[str] = None
    url: Optional[str] = None
    secret_scope: Optional[str] = None
    secret_key_user: Optional[str] = None
    secret_key_password: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)


@dataclass
class WatermarkConfig:
    """Watermark column and type for incremental loads."""
    column: str = ""
    type: str = "timestamp"  # timestamp, integer, date
    default_value: Optional[str] = None  # used for first load


@dataclass
class PaginationConfig:
    """REST API pagination settings."""
    type: PaginationType = PaginationType.OFFSET
    page_size: int = 100
    max_pages: Optional[int] = None
    offset_param: str = "offset"
    limit_param: str = "limit"
    cursor_param: str = "cursor"
    cursor_response_path: Optional[str] = None
    data_response_path: str = "data"


@dataclass
class AuthConfig:
    """REST API authentication settings."""
    type: AuthType = AuthType.NONE
    secret_scope: Optional[str] = None
    secret_key_token: Optional[str] = None
    secret_key_client_id: Optional[str] = None
    secret_key_client_secret: Optional[str] = None
    token_url: Optional[str] = None
    header_name: str = "Authorization"
    header_prefix: str = "Bearer"


@dataclass
class ExtractConfig:
    """Source extraction settings."""
    load_type: LoadType = LoadType.FULL
    watermark: Optional[WatermarkConfig] = None
    # JDBC
    query: Optional[str] = None
    table: Optional[str] = None
    partition_column: Optional[str] = None
    num_partitions: int = 1
    fetch_size: int = 10000
    # File
    path: Optional[str] = None
    format: str = "parquet"
    format_options: Dict[str, str] = field(default_factory=dict)
    auto_loader: bool = False
    checkpoint_path: Optional[str] = None
    # API
    base_url: Optional[str] = None
    endpoint: Optional[str] = None
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, str] = field(default_factory=dict)
    auth: Optional[AuthConfig] = None
    pagination: Optional[PaginationConfig] = None
    max_retries: int = 3
    retry_backoff_factor: float = 2.0
    timeout_seconds: int = 30
    response_root_path: str = "data"
    # Stream
    kafka_bootstrap_servers: Optional[str] = None
    kafka_topic: Optional[str] = None
    kafka_consumer_group: Optional[str] = None
    kafka_options: Dict[str, str] = field(default_factory=dict)
    event_hub_connection_string_key: Optional[str] = None
    event_hub_consumer_group: str = "$Default"
    starting_offsets: str = "earliest"


@dataclass
class MetadataColumn:
    """Metadata column to inject into target."""
    name: str
    expression: str  # SQL expression, e.g. "current_timestamp()"


@dataclass
class SchemaEvolutionConfig:
    """Schema evolution settings for Delta writes."""
    mode: SchemaEvolutionMode = SchemaEvolutionMode.MERGE
    rescued_data_column: str = "_rescued_data"


@dataclass
class QualityConfig:
    """Data quality and dead letter settings."""
    enabled: bool = True
    dead_letter_table_suffix: str = "dead_letter"
    quarantine_threshold_pct: float = 10.0  # fail if > N% bad records


@dataclass
class CdcConfig:
    """Change Data Capture settings for bronze table merge."""
    enabled: bool = False
    mode: CdcMode = CdcMode.APPEND
    primary_keys: List[str] = field(default_factory=list)
    sequence_column: Optional[str] = None  # for ordering within same key
    exclude_columns_from_hash: List[str] = field(default_factory=list)
    delete_condition_column: Optional[str] = None  # column indicating deletes
    delete_condition_value: Optional[str] = None    # value meaning "deleted"


@dataclass
class LandingConfig:
    """Landing zone file management settings."""
    path: Optional[str] = None          # landing folder path
    archive_path: Optional[str] = None  # move processed files here (optional)
    retention_days: int = 10            # delete landing files after N days
    cleanup_enabled: bool = True


@dataclass
class TargetConfig:
    """Delta Lake target table settings."""
    catalog: str = ""
    schema: str = "bronze"
    table: str = ""
    partition_by: List[str] = field(default_factory=list)
    z_order_by: List[str] = field(default_factory=list)
    table_properties: Dict[str, str] = field(default_factory=dict)
    metadata_columns: List[MetadataColumn] = field(default_factory=list)
    schema_evolution: SchemaEvolutionConfig = field(
        default_factory=SchemaEvolutionConfig
    )
    quality: QualityConfig = field(default_factory=QualityConfig)
    cdc: CdcConfig = field(default_factory=CdcConfig)
    landing: LandingConfig = field(default_factory=LandingConfig)


@dataclass
class SourceConfig:
    """Top-level source configuration."""
    name: str
    source_type: SourceType
    description: str = ""
    enabled: bool = True
    tags: Dict[str, str] = field(default_factory=dict)
    connection: ConnectionConfig = field(default_factory=ConnectionConfig)
    extract: ExtractConfig = field(default_factory=ExtractConfig)
    target: TargetConfig = field(default_factory=TargetConfig)

    @property
    def full_table_name(self) -> str:
        return f"{self.target.catalog}.{self.target.schema}.{self.target.table}"

    @property
    def dead_letter_table_name(self) -> str:
        suffix = self.target.quality.dead_letter_table_suffix
        return (
            f"{self.target.catalog}.bronze_meta.{suffix}_{self.target.table}"
        )

    @property
    def audit_table_name(self) -> str:
        return f"{self.target.catalog}.bronze_meta.ingestion_audit_log"
