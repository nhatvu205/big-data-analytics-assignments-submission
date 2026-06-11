"""Local live preview with bounding boxes using YOLO."""

import argparse

import cv2

from processing_server.detector import PersonDetector
from utils.visualization import draw_bounding_boxes


def parse_source(source: str):
    if source.isdigit():
        return int(source)
    return source


def run_live_viewer(source="0", model_name: str = "yolov8n.pt", confidence: float = 0.4) -> None:
    detector = PersonDetector(model_name=model_name, confidence=confidence)
    cap = cv2.VideoCapture(parse_source(str(source)))
    if not cap.isOpened():
        raise FileNotFoundError(f"Could not open camera/video source: {source}")

    print("Press 'q' to quit.")
    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                break

            frame = cv2.resize(frame, (480, 360))
            boxes, _ = detector.detect_frame(frame)
            annotated = draw_bounding_boxes(frame, boxes)
            cv2.imshow("People Counting Live Viewer", annotated)

            if cv2.waitKey(1) & 0xFF == ord("q"):
                break
    finally:
        cap.release()
        cv2.destroyAllWindows()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Preview webcam/video with live person detection bounding boxes.")
    parser.add_argument("--source", default="0", help="Camera index like 0, or a video path.")
    parser.add_argument("--model", default="yolov8n.pt", help="YOLO model path.")
    parser.add_argument("--confidence", type=float, default=0.4, help="Detection confidence threshold.")
    args = parser.parse_args()
    run_live_viewer(source=args.source, model_name=args.model, confidence=args.confidence)
