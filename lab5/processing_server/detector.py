"""YOLO-based person detector."""

import base64
from typing import List, Tuple

import cv2
import numpy as np
from ultralytics import YOLO

try:  # pragma: no cover - depends on runtime
    import torch
except Exception:  # pragma: no cover
    torch = None


class PersonDetector:
    def __init__(self, model_name: str = "yolov8n.pt", confidence: float = 0.4, device: str = "cuda"):
        if device == "cuda" and (torch is None or not torch.cuda.is_available()):
            device = "cpu"
        self.model = YOLO(model_name)
        self.confidence = confidence
        self.device = device

    def detect(self, frame_b64: str) -> Tuple[List[dict], int]:
        image_bytes = base64.b64decode(frame_b64)
        image_array = np.frombuffer(image_bytes, dtype=np.uint8)
        frame = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
        if frame is None:
            raise ValueError("Could not decode input frame")

        results = self.model(frame, classes=[0], conf=self.confidence, device=self.device, verbose=False)
        boxes = []
        for result in results:
            for box in result.boxes:
                x1, y1, x2, y2 = box.xyxy[0].tolist()
                boxes.append(
                    {
                        "x1": round(x1),
                        "y1": round(y1),
                        "x2": round(x2),
                        "y2": round(y2),
                        "confidence": round(float(box.conf[0]), 3),
                    }
                )
        return boxes, len(boxes)
