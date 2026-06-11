import logging
import time
from typing import Iterable

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


logger = logging.getLogger(__name__)

RAW_FRAMES_TOPIC = "raw-frames"
DETECTION_RESULTS_TOPIC = "detection-results"


def wait_for_kafka(bootstrap_servers: str, timeout: int = 60, poll_interval: float = 2.0) -> None:
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            client.close()
            return
        except Exception as exc:  # pragma: no cover
            last_error = exc
            logger.info("Waiting for Kafka at %s: %s", bootstrap_servers, exc)
            time.sleep(poll_interval)
    raise RuntimeError(f"Kafka not ready at {bootstrap_servers}: {last_error}")


def create_topics(
    bootstrap_servers: str,
    topics: Iterable[str] = (RAW_FRAMES_TOPIC, DETECTION_RESULTS_TOPIC),
    partitions: int = 2,
    replication_factor: int = 1,
) -> None:
    client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        topic_defs = [
            NewTopic(name=topic, num_partitions=partitions, replication_factor=replication_factor)
            for topic in topics
        ]
        client.create_topics(new_topics=topic_defs, validate_only=False)
        logger.info("Created Kafka topics: %s", list(topics))
    except TopicAlreadyExistsError:
        logger.info("Kafka topics already exist")
    finally:
        client.close()
