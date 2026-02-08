"""Tests for API reader."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from bronze_framework.config.models import (
    AuthConfig,
    AuthType,
    PaginationConfig,
    PaginationType,
)
from bronze_framework.readers.api_reader import APIReader


class TestAPIReader:
    """Tests for APIReader class."""

    @patch("bronze_framework.readers.api_reader.requests.Session")
    def test_fetch_simple_response(self, mock_session_cls, sample_api_config):
        mock_spark = MagicMock()
        mock_rdd = MagicMock()
        mock_spark.sparkContext.parallelize.return_value = mock_rdd
        mock_spark.read.json.return_value = MagicMock()

        reader = APIReader(mock_spark, sample_api_config)
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [{"id": 1, "amount": 100}]
        }
        mock_response.raise_for_status.return_value = None
        reader._session.request.return_value = mock_response

        result = reader.read()
        mock_spark.read.json.assert_called_once()

    @patch("bronze_framework.readers.api_reader.requests.Session")
    def test_empty_response_returns_empty_df(
        self, mock_session_cls, sample_api_config
    ):
        mock_spark = MagicMock()
        reader = APIReader(mock_spark, sample_api_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status.return_value = None
        reader._session.request.return_value = mock_response

        result = reader.read()
        mock_spark.createDataFrame.assert_called_once()

    def test_extract_data_nested_path(self, sample_api_config):
        mock_spark = MagicMock()
        reader = APIReader(mock_spark, sample_api_config)
        response = {"results": {"items": [{"id": 1}]}}
        data = reader._extract_data(response, "results.items")
        assert data == [{"id": 1}]

    def test_extract_data_missing_path(self, sample_api_config):
        mock_spark = MagicMock()
        reader = APIReader(mock_spark, sample_api_config)
        response = {"other": "data"}
        data = reader._extract_data(response, "results.items")
        assert data == []

    def test_extract_cursor(self, sample_api_config):
        mock_spark = MagicMock()
        reader = APIReader(mock_spark, sample_api_config)
        response = {"meta": {"next_cursor": "abc123"}}
        cursor = reader._extract_cursor(response, "meta.next_cursor")
        assert cursor == "abc123"

    def test_extract_cursor_none(self, sample_api_config):
        mock_spark = MagicMock()
        reader = APIReader(mock_spark, sample_api_config)
        cursor = reader._extract_cursor({}, "meta.next_cursor")
        assert cursor is None

    @patch("bronze_framework.readers.api_reader.requests.Session")
    @patch("bronze_framework.readers.api_reader.time.sleep")
    def test_retry_on_failure(
        self, mock_sleep, mock_session_cls, sample_api_config
    ):
        import requests as req

        mock_spark = MagicMock()
        reader = APIReader(mock_spark, sample_api_config)

        mock_fail = MagicMock()
        mock_fail.raise_for_status.side_effect = req.exceptions.HTTPError("500")

        mock_success = MagicMock()
        mock_success.json.return_value = {"data": [{"id": 1}]}
        mock_success.raise_for_status.return_value = None

        reader._session.request.side_effect = [
            req.exceptions.HTTPError("500"),
            mock_success,
        ]

        result = reader._request_with_retry(
            "https://api.example.com/data", {}
        )
        assert result == {"data": [{"id": 1}]}
        assert mock_sleep.call_count == 1

    @patch("bronze_framework.readers.api_reader.get_secret")
    def test_bearer_auth(self, mock_secret, sample_api_config):
        mock_secret.return_value = "test-token"
        sample_api_config.extract.auth = AuthConfig(
            type=AuthType.BEARER,
            secret_scope="test-scope",
            secret_key_token="api-token",
        )

        mock_spark = MagicMock()
        reader = APIReader(mock_spark, sample_api_config)
        reader._authenticate()

        assert "Authorization" in reader._session.headers
        assert reader._session.headers["Authorization"] == "Bearer test-token"
