"""REST API reader with authentication, pagination, and retry."""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from bronze_framework.config.models import (
    AuthType,
    LoadType,
    PaginationType,
    SourceConfig,
)
from bronze_framework.readers.base_reader import BaseReader
from bronze_framework.utils.secrets import get_secret


class APIReader(BaseReader):
    """Reads data from REST APIs with auth, pagination, and retry support."""

    def __init__(self, spark: SparkSession, config: SourceConfig):
        super().__init__(spark, config)
        self._session = requests.Session()
        self._token: Optional[str] = None

    def read(self) -> DataFrame:
        extract = self.config.extract
        self._authenticate()

        all_records = self._fetch_all_pages()

        if not all_records:
            self.logger.warning(f"No records returned from API: {self.config.name}")
            return self.spark.createDataFrame([], StructType([]))

        self.logger.info(
            f"Fetched {len(all_records)} records from API: {self.config.name}"
        )

        json_strings = [json.dumps(record) for record in all_records]
        rdd = self.spark.sparkContext.parallelize(json_strings)
        return self.spark.read.json(rdd)

    def _authenticate(self) -> None:
        """Set up authentication based on config."""
        auth = self.config.extract.auth
        if auth is None or auth.type == AuthType.NONE:
            return

        if auth.type == AuthType.BEARER:
            self._token = get_secret(
                self.spark, auth.secret_scope, auth.secret_key_token
            )
            self._session.headers[auth.header_name] = (
                f"{auth.header_prefix} {self._token}"
            )

        elif auth.type == AuthType.API_KEY:
            api_key = get_secret(
                self.spark, auth.secret_scope, auth.secret_key_token
            )
            self._session.headers[auth.header_name] = api_key

        elif auth.type == AuthType.OAUTH2:
            client_id = get_secret(
                self.spark, auth.secret_scope, auth.secret_key_client_id
            )
            client_secret = get_secret(
                self.spark, auth.secret_scope, auth.secret_key_client_secret
            )
            token_response = requests.post(
                auth.token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                timeout=self.config.extract.timeout_seconds,
            )
            token_response.raise_for_status()
            self._token = token_response.json()["access_token"]
            self._session.headers["Authorization"] = f"Bearer {self._token}"

    def _fetch_all_pages(self) -> List[Dict[str, Any]]:
        """Fetch all pages of data from the API."""
        extract = self.config.extract
        pagination = extract.pagination
        all_records: List[Dict[str, Any]] = []

        params = dict(extract.params)

        # Add watermark filter if incremental
        if extract.load_type == LoadType.INCREMENTAL and extract.watermark:
            watermark_value = self._get_watermark_value()
            if watermark_value:
                params[f"{extract.watermark.column}_after"] = watermark_value

        url = f"{extract.base_url}{extract.endpoint}"

        if pagination is None:
            response_data = self._request_with_retry(url, params)
            records = self._extract_data(response_data, extract.response_root_path)
            all_records.extend(records)
            return all_records

        page = 0
        cursor: Optional[str] = None

        while True:
            if pagination.type == PaginationType.OFFSET:
                params[pagination.limit_param] = str(pagination.page_size)
                params[pagination.offset_param] = str(page * pagination.page_size)
            elif pagination.type == PaginationType.CURSOR and cursor:
                params[pagination.cursor_param] = cursor

            response_data = self._request_with_retry(url, params)
            records = self._extract_data(
                response_data, pagination.data_response_path
            )
            all_records.extend(records)

            page += 1
            if pagination.max_pages and page >= pagination.max_pages:
                break
            if not records or len(records) < pagination.page_size:
                break

            if pagination.type == PaginationType.CURSOR:
                cursor = self._extract_cursor(
                    response_data, pagination.cursor_response_path
                )
                if not cursor:
                    break
            elif pagination.type == PaginationType.LINK_HEADER:
                url = self._extract_next_link(response_data)
                if not url:
                    break
                params = {}  # link header URLs include params

        return all_records

    def _request_with_retry(
        self, url: str, params: Dict[str, str]
    ) -> Dict[str, Any]:
        """Execute HTTP request with exponential backoff retry."""
        extract = self.config.extract

        for attempt in range(extract.max_retries + 1):
            try:
                response = self._session.request(
                    method=extract.method,
                    url=url,
                    params=params if extract.method == "GET" else None,
                    json=params if extract.method == "POST" else None,
                    headers=extract.headers,
                    timeout=extract.timeout_seconds,
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt == extract.max_retries:
                    raise RuntimeError(
                        f"API request failed after {extract.max_retries + 1} "
                        f"attempts: {e}"
                    ) from e
                wait_time = extract.retry_backoff_factor ** attempt
                self.logger.warning(
                    f"API request failed (attempt {attempt + 1}): {e}. "
                    f"Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)

        return {}  # unreachable, but satisfies type checker

    def _extract_data(
        self, response: Dict[str, Any], path: str
    ) -> List[Dict[str, Any]]:
        """Navigate dot-separated path to extract data array from response."""
        result: Any = response
        for key in path.split("."):
            if isinstance(result, dict):
                result = result.get(key, [])
            else:
                return []
        return result if isinstance(result, list) else [result]

    def _extract_cursor(
        self, response: Dict[str, Any], path: Optional[str]
    ) -> Optional[str]:
        """Extract cursor value from response using dot-separated path."""
        if not path:
            return None
        result: Any = response
        for key in path.split("."):
            if isinstance(result, dict):
                result = result.get(key)
            else:
                return None
        return str(result) if result else None

    def _extract_next_link(self, response: Dict[str, Any]) -> Optional[str]:
        """Extract next page URL from Link header-style response."""
        links = response.get("_links", {})
        next_link = links.get("next", {})
        return next_link.get("href") if isinstance(next_link, dict) else None
