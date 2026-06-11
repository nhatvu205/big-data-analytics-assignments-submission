"""Storage consumer: persist detection results from Kafka."""

import json
import logging
from typing import Optional

from kafka import KafkaConsumer

from storage_server.db import StorageDB
from utils.kafka_utils import DETECTION_RESULTS_TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("StorageServer")


def run_storage_consumer(kafka_bootstrap: str = "localhost:9092", db: Optional[StorageDB] = None) -> None:
    storage = db or StorageDB()
    consumer = KafkaConsumer(
        DETECTION_RESULTS_TOPIC,
        bootstrap_servers=kafka_bootstrap,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
        group_id="storage-group",
        auto_offset_reset="earliest",
    )

    logger.info("Storage server started. Listening on topic '%s'", DETECTION_RESULTS_TOPIC)
    try:
        for msg in consumer:
            result = msg.value
            storage.save(result)
            logger.info("Stored result for frame %s", result["frame_id"])
    finally:
        consumer.close()
