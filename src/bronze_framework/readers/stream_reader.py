"""Streaming reader for Kafka and Event Hub sources."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType

from bronze_framework.config.models import SourceConfig
from bronze_framework.readers.base_reader import BaseReader
from bronze_framework.utils.secrets import get_secret


class StreamReader(BaseReader):
    """Reads streaming data from Kafka or Azure Event Hub."""

    def __init__(self, spark: SparkSession, config: SourceConfig):
        super().__init__(spark, config)

    def read(self) -> DataFrame:
        extract = self.config.extract

        if extract.event_hub_connection_string_key:
            return self._read_event_hub()
        return self._read_kafka()

    def _read_kafka(self) -> DataFrame:
        """Read from Kafka topic as a streaming DataFrame."""
        extract = self.config.extract

        options = {
            "kafka.bootstrap.servers": extract.kafka_bootstrap_servers,
            "subscribe": extract.kafka_topic,
            "startingOffsets": extract.starting_offsets,
        }

        if extract.kafka_consumer_group:
            options["kafka.group.id"] = extract.kafka_consumer_group

        options.update(extract.kafka_options)

        self.logger.info(
            f"Reading Kafka stream: topic={extract.kafka_topic}, "
            f"servers={extract.kafka_bootstrap_servers}"
        )

        df = (
            self.spark.readStream
            .format("kafka")
            .options(**options)
            .load()
        )

        # Deserialize key and value from binary to string
        return df.select(
            col("key").cast(StringType()).alias("key"),
            col("value").cast(StringType()).alias("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )

    def _read_event_hub(self) -> DataFrame:
        """Read from Azure Event Hub as a streaming DataFrame."""
        extract = self.config.extract
        conn = self.config.connection

        connection_string = get_secret(
            self.spark,
            conn.secret_scope or extract.kafka_options.get("secret_scope", ""),
            extract.event_hub_connection_string_key,
        )

        eh_conf = {
            "eventhubs.connectionString": (
                self.spark.sparkContext._jvm.org.apache.spark.eventhubs
                .EventHubsUtils.encrypt(connection_string)
            ),
            "eventhubs.consumerGroup": extract.event_hub_consumer_group,
            "eventhubs.startingPosition": (
                f'{{"offset": "-1", "seqNo": -1, '
                f'"enqueuedTime": null, "isInclusive": true}}'
            ),
        }

        self.logger.info(
            f"Reading Event Hub stream: "
            f"consumer_group={extract.event_hub_consumer_group}"
        )

        df = (
            self.spark.readStream
            .format("eventhubs")
            .options(**eh_conf)
            .load()
        )

        return df.select(
            col("body").cast(StringType()).alias("value"),
            col("partition").alias("partition"),
            col("offset").alias("offset"),
            col("enqueuedTime").alias("event_hub_timestamp"),
        )
