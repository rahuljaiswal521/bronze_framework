"""Landing zone file cleanup utility.

Deletes files older than the configured retention period from the landing folder.
Designed to run after ingestion to remove processed files.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from pyspark.sql import SparkSession

from bronze_framework.config.models import SourceConfig
from bronze_framework.utils.logging import get_logger

logger = get_logger(__name__)


class LandingFileCleanup:
    """Manages landing zone file retention and cleanup."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def cleanup(self, config: SourceConfig) -> int:
        """Delete landing files older than retention_days.

        Returns the number of files deleted.
        """
        landing = config.target.landing

        if not landing.cleanup_enabled:
            logger.info(f"Cleanup disabled for {config.name}; skipping")
            return 0

        landing_path = landing.path or config.extract.path
        if not landing_path:
            logger.warning(f"No landing path configured for {config.name}; skipping cleanup")
            return 0

        retention_days = landing.retention_days
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

        logger.info(
            f"Cleaning up landing files for {config.name}: "
            f"path={landing_path}, retention={retention_days} days, "
            f"cutoff={cutoff.isoformat()}"
        )

        deleted = self._delete_old_files(landing_path, cutoff)

        logger.info(f"Deleted {deleted} files from {landing_path}")
        return deleted

    def _delete_old_files(self, path: str, cutoff: datetime) -> int:
        """Delete files older than cutoff using dbutils or Hadoop filesystem."""
        deleted = 0
        try:
            dbutils = self._get_dbutils()
            files = dbutils.fs.ls(path)
            cutoff_ms = int(cutoff.timestamp() * 1000)

            for f in files:
                if f.isDir():
                    # Recurse into subdirectories
                    deleted += self._delete_old_files(f.path, cutoff)
                else:
                    file_info = dbutils.fs.ls(f.path)
                    if file_info and file_info[0].modificationTime < cutoff_ms:
                        dbutils.fs.rm(f.path)
                        deleted += 1
                        logger.info(f"Deleted: {f.path}")
        except Exception as e:
            logger.warning(
                f"dbutils cleanup failed, falling back to Hadoop FS: {e}"
            )
            deleted = self._delete_old_files_hadoop(path, cutoff)

        return deleted

    def _delete_old_files_hadoop(self, path: str, cutoff: datetime) -> int:
        """Fallback: delete files using Hadoop FileSystem API."""
        deleted = 0
        try:
            jvm = self.spark.sparkContext._jvm
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs_path = jvm.org.apache.hadoop.fs.Path(path)
            fs = fs_path.getFileSystem(hadoop_conf)

            cutoff_ms = int(cutoff.timestamp() * 1000)

            if not fs.exists(fs_path):
                logger.warning(f"Landing path does not exist: {path}")
                return 0

            file_statuses = fs.listStatus(fs_path)
            for status in file_statuses:
                if status.isDirectory():
                    deleted += self._delete_old_files_hadoop(
                        status.getPath().toString(), cutoff
                    )
                elif status.getModificationTime() < cutoff_ms:
                    fs.delete(status.getPath(), False)
                    deleted += 1
                    logger.info(f"Deleted: {status.getPath()}")

        except Exception as e:
            logger.error(f"Hadoop FS cleanup failed for {path}: {e}")

        return deleted

    def _get_dbutils(self):
        """Get dbutils from the Spark session."""
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(self.spark)
        except ImportError:
            try:
                import IPython
                return IPython.get_ipython().user_ns["dbutils"]
            except Exception:
                raise RuntimeError("dbutils is not available")
