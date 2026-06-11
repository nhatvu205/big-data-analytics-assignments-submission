"""Camera server: read frames from a video and publish them to Kafka."""

import base64
import json
import logging
import time
from typing import Optional

import cv2
from kafka import KafkaProducer

from utils.kafka_utils import RAW_FRAMES_TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CameraServer")


def build_producer(kafka_bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        max_request_size=10_000_000,
    )


def run_camera_server(
    video_path: str,
    kafka_bootstrap: str = "localhost:9092",
    fps_limit: int = 5,
    source_id: str = "camera-1",
    max_frames: Optional[int] = None,
) -> None:
    producer = build_producer(kafka_bootstrap)
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise FileNotFoundError(f"Could not open video source: {video_path}")

    frame_id = 0
    interval = 1.0 / max(fps_limit, 1)
    logger.info("Camera server started. Publishing frames to topic '%s'", RAW_FRAMES_TOPIC)

    try:
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                logger.info("Video ended. Restarting from the beginning.")
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                continue

            frame = cv2.resize(frame, (640, 480))
            ok, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
            if not ok:
                logger.warning("Skipping frame %s because JPEG encoding failed", frame_id)
                continue

            message = {
                "frame_id": frame_id,
                "timestamp": time.time(),
                "source_id": source_id,
                "frame_data": base64.b64encode(buffer).decode("utf-8"),
                "width": 640,
                "height": 480,
            }
            producer.send(RAW_FRAMES_TOPIC, value=message)
            logger.info("Sent frame %s", frame_id)

            frame_id += 1
            if max_frames is not None and frame_id >= max_frames:
                break
            time.sleep(interval)
    finally:
        cap.release()
        producer.flush()
        producer.close()
