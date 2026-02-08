"""Dead letter handler for quarantining bad records."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from bronze_framework.config.models import SourceConfig
from bronze_framework.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class QualityResult:
    """Result of quality check with good/bad record split."""
    good_df: DataFrame
    bad_df: DataFrame
    total_count: int
    bad_count: int
    bad_pct: float
    threshold_exceeded: bool


class DeadLetterHandler:
    """Splits and quarantines bad records based on _rescued_data column."""

    def __init__(self, spark: SparkSession, config: SourceConfig):
        self.spark = spark
        self.config = config

    def check_quality(self, df: DataFrame) -> QualityResult:
        """Split DataFrame into good and bad records.

        Bad records are identified by a non-null _rescued_data column,
        which is populated when schema evolution mode is 'rescue'.
        """
        quality = self.config.target.quality
        rescued_col = self.config.target.schema_evolution.rescued_data_column

        if not quality.enabled or rescued_col not in df.columns:
            count = df.count()
            return QualityResult(
                good_df=df,
                bad_df=self.spark.createDataFrame([], df.schema),
                total_count=count,
                bad_count=0,
                bad_pct=0.0,
                threshold_exceeded=False,
            )

        # Cache to avoid recomputation
        df.cache()

        total_count = df.count()
        bad_df = df.filter(col(rescued_col).isNotNull())
        good_df = df.filter(col(rescued_col).isNull())

        bad_count = bad_df.count()
        bad_pct = (bad_count / total_count * 100) if total_count > 0 else 0.0
        threshold_exceeded = bad_pct > quality.quarantine_threshold_pct

        if bad_count > 0:
            logger.warning(
                f"Source {self.config.name}: {bad_count}/{total_count} "
                f"({bad_pct:.1f}%) bad records detected"
            )

        if threshold_exceeded:
            logger.error(
                f"Source {self.config.name}: bad record threshold exceeded "
                f"({bad_pct:.1f}% > {quality.quarantine_threshold_pct}%)"
            )

        return QualityResult(
            good_df=good_df,
            bad_df=bad_df,
            total_count=total_count,
            bad_count=bad_count,
            bad_pct=bad_pct,
            threshold_exceeded=threshold_exceeded,
        )

    def quarantine(self, bad_df: DataFrame) -> int:
        """Write bad records to the dead letter table.

        Returns the number of quarantined records.
        """
        if bad_df.isEmpty():
            return 0

        dead_letter_table = self.config.dead_letter_table_name

        quarantine_df = bad_df.select(
            lit(self.config.name).alias("source_name"),
            current_timestamp().alias("quarantined_at"),
            col("*"),
        )

        count = quarantine_df.count()
        logger.info(
            f"Quarantining {count} records to {dead_letter_table}"
        )

        quarantine_df.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(dead_letter_table)

        return count
