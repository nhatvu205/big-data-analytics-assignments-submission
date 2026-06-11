import logging
import subprocess
from pathlib import Path
from typing import Optional

import cv2
import numpy as np

logger = logging.getLogger(__name__)
DEFAULT_VIDEO_URL = "https://www.pexels.com/video/time-lapse-of-people-walking-on-sidewalk-3048225/download/"


def download_sample_video(output_path: str = "sample_video.mp4", url: str = DEFAULT_VIDEO_URL) -> Optional[str]:
    output = Path(output_path)
    if output.exists():
        logger.info("Sample video already exists: %s", output)
        return str(output)

    command = ["yt-dlp", "-f", "mp4", "-o", str(output), url]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info("Downloaded sample video to %s", output)
        return str(output)
    except Exception as exc:  # pragma: no cover
        logger.warning("Could not download public video: %s", exc)
        return None


def create_synthetic_video(output_path: str = "sample_video.mp4", num_frames: int = 300, fps: int = 15) -> str:
    width, height = 640, 480
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))

    rng = np.random.default_rng(42)
    persons = [
        {
            "x": int(rng.integers(50, 550)),
            "y": int(rng.integers(70, 360)),
            "dx": int(rng.choice([-3, -2, 2, 3])),
            "dy": int(rng.choice([-2, -1, 1, 2])),
        }
        for _ in range(5)
    ]

    for _ in range(num_frames):
        frame = np.ones((height, width, 3), dtype=np.uint8) * 200
        cv2.putText(frame, "Synthetic People Counting Demo", (120, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (50, 50, 50), 2)
        for person in persons:
            person["x"] = (person["x"] + person["dx"]) % (width - 60)
            person["y"] = (person["y"] + person["dy"]) % (height - 140)
            x, y = person["x"], person["y"]
            cv2.circle(frame, (x + 20, y - 15), 15, (200, 150, 100), -1)
            cv2.rectangle(frame, (x, y), (x + 40, y + 100), (0, 100, 200), -1)
        out.write(frame)

    out.release()
    logger.info("Created synthetic video at %s", output_path)
    return output_path


def prepare_demo_video(output_path: str = "sample_video.mp4") -> str:
    downloaded = download_sample_video(output_path=output_path)
    if downloaded:
        return downloaded
    return create_synthetic_video(output_path=output_path)
