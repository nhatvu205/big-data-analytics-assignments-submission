import base64
from pathlib import Path
from typing import Iterable, Mapping

import cv2
import numpy as np


def decode_frame(frame_b64: str):
    image_bytes = base64.b64decode(frame_b64)
    image_array = np.frombuffer(image_bytes, dtype=np.uint8)
    return cv2.imdecode(image_array, cv2.IMREAD_COLOR)


def draw_bounding_boxes(frame, bounding_boxes: Iterable[Mapping[str, float]]):
    annotated = frame.copy()
    count = 0
    for bbox in bounding_boxes:
        count += 1
        x1, y1, x2, y2 = int(bbox["x1"]), int(bbox["y1"]), int(bbox["x2"]), int(bbox["y2"])
        confidence = bbox.get("confidence", 0)
        cv2.rectangle(annotated, (x1, y1), (x2, y2), (0, 0, 255), 2)
        cv2.putText(
            annotated,
            f"person {confidence:.2f}",
            (x1, max(20, y1 - 10)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            (0, 0, 255),
            1,
        )
    cv2.putText(annotated, f"count={count}", (20, 30), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (0, 0, 0), 2)
    return annotated


def save_annotated_frame(frame_b64: str, bounding_boxes: Iterable[Mapping[str, float]], output_path: str) -> str:
    frame = decode_frame(frame_b64)
    if frame is None:
        raise ValueError("Could not decode frame for annotation")
    annotated = draw_bounding_boxes(frame, bounding_boxes)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(output_path, annotated)
    return output_path
