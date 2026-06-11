"""Processing server: consume frames, detect people, publish results."""

import json
import logging
import time

from kafka import KafkaConsumer, KafkaProducer

from processing_server.detector import PersonDetector
from utils.kafka_utils import DETECTION_RESULTS_TOPIC, RAW_FRAMES_TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ProcessingServer")


def run_processing_server(kafka_bootstrap: str = "localhost:9092", model_name: str = "yolov8n.pt") -> None:
    detector = PersonDetector(model_name=model_name)
    consumer = KafkaConsumer(
        RAW_FRAMES_TOPIC,
        bootstrap_servers=kafka_bootstrap,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
        max_partition_fetch_bytes=10_000_000,
        group_id="processing-group",
        auto_offset_reset="earliest",
    )
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    logger.info("Processing server started. Listening on topic '%s'", RAW_FRAMES_TOPIC)
    try:
        for msg in consumer:
            frame_message = msg.value
            start_time = time.time()
            boxes, count = detector.detect(frame_message["frame_data"])
            processing_time_ms = round((time.time() - start_time) * 1000, 2)

            result = {
                "run_id": frame_message.get("run_id"),
                "frame_id": frame_message["frame_id"],
                "timestamp": frame_message["timestamp"],
                "processed_at": time.time(),
                "source_id": frame_message.get("source_id", "camera-1"),
                "person_count": count,
                "bounding_boxes": boxes,
                "processing_time_ms": processing_time_ms,
            }
            producer.send(DETECTION_RESULTS_TOPIC, value=result)
            logger.info(
                "Frame %s processed: %s people detected in %sms (run_id=%s)",
                result["frame_id"],
                count,
                processing_time_ms,
                result["run_id"],
            )
    finally:
        consumer.close()
        producer.flush()
        producer.close()
