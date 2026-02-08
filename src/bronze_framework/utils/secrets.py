"""Databricks secrets wrapper."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession


def get_secret(
    spark: SparkSession,
    scope: Optional[str],
    key: Optional[str],
) -> str:
    """Retrieve a secret from Databricks secret scope.

    Args:
        spark: Active SparkSession (used to access dbutils).
        scope: Databricks secret scope name.
        key: Secret key within the scope.

    Returns:
        The secret value as a string.

    Raises:
        ValueError: If scope or key is not provided.
    """
    if not scope or not key:
        raise ValueError(
            f"Secret scope and key must be provided (scope={scope}, key={key})"
        )

    dbutils = _get_dbutils(spark)
    return dbutils.secrets.get(scope=scope, key=key)


def _get_dbutils(spark: SparkSession):
    """Get dbutils from the Spark session."""
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        try:
            import IPython
            return IPython.get_ipython().user_ns["dbutils"]
        except Exception:
            raise RuntimeError(
                "dbutils is not available. "
                "This function must be run on Databricks."
            )
