"""Storage backends for detection results."""

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

try:  # pragma: no cover - dependency availability varies by environment
    from pymongo import MongoClient
except Exception:  # pragma: no cover
    MongoClient = None


class StorageDB:
    def __init__(
        self,
        use_mongo: bool = True,
        mongo_uri: str = "mongodb://localhost:27017/",
        sqlite_path: str = "detections.db",
    ):
        self.use_mongo = use_mongo and MongoClient is not None
        self.collection = None
        self.conn = None

        if self.use_mongo:
            try:
                self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
                self.client.server_info()
                self.collection = self.client["people_counting"]["detections"]
            except Exception:
                self.use_mongo = False

        if not self.use_mongo:
            sqlite_file = Path(sqlite_path)
            self.conn = sqlite3.connect(sqlite_file, check_same_thread=False)
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS detections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    frame_id INTEGER,
                    timestamp REAL,
                    processed_at REAL,
                    source_id TEXT,
                    person_count INTEGER,
                    bounding_boxes TEXT,
                    processing_time_ms REAL
                )
                """
            )
            columns = {
                row[1]
                for row in self.conn.execute("PRAGMA table_info(detections)").fetchall()
            }
            if "run_id" not in columns:
                self.conn.execute("ALTER TABLE detections ADD COLUMN run_id TEXT")
            self.conn.commit()

    def save(self, result: Dict[str, Any]) -> None:
        if self.use_mongo and self.collection is not None:
            self.collection.insert_one(result)
            return

        assert self.conn is not None
        self.conn.execute(
            """
            INSERT INTO detections (
                run_id, frame_id, timestamp, processed_at, source_id, person_count, bounding_boxes, processing_time_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.get("run_id"),
                result["frame_id"],
                result["timestamp"],
                result.get("processed_at"),
                result.get("source_id", "camera-1"),
                result["person_count"],
                json.dumps(result["bounding_boxes"]),
                result["processing_time_ms"],
            ),
        )
        self.conn.commit()

    def get_latest(self, limit: int = 10) -> List[Dict[str, Any]]:
        if self.use_mongo and self.collection is not None:
            return list(self.collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(limit))

        assert self.conn is not None
        cursor = self.conn.execute(
            """
            SELECT run_id, frame_id, timestamp, processed_at, source_id, person_count, bounding_boxes, processing_time_ms
            FROM detections ORDER BY timestamp DESC LIMIT ?
            """,
            (limit,),
        )
        rows = []
        for row in cursor.fetchall():
            rows.append(
                {
                    "run_id": row[0],
                    "frame_id": row[1],
                    "timestamp": row[2],
                    "processed_at": row[3],
                    "source_id": row[4],
                    "person_count": row[5],
                    "bounding_boxes": json.loads(row[6]),
                    "processing_time_ms": row[7],
                }
            )
        return rows

    def get_stats(self) -> Dict[str, Any]:
        if self.use_mongo and self.collection is not None:
            pipeline = [{"$group": {"_id": None, "avg_count": {"$avg": "$person_count"}, "max_count": {"$max": "$person_count"}, "total_frames": {"$sum": 1}}}]
            result = list(self.collection.aggregate(pipeline))
            if not result:
                return {"avg_count": 0, "max_count": 0, "total_frames": 0}
            data = result[0]
            return {"avg_count": data.get("avg_count", 0), "max_count": data.get("max_count", 0), "total_frames": data.get("total_frames", 0)}

        assert self.conn is not None
        cursor = self.conn.execute("SELECT AVG(person_count), MAX(person_count), COUNT(*) FROM detections")
        avg_count, max_count, total_frames = cursor.fetchone()
        return {"avg_count": avg_count or 0, "max_count": max_count or 0, "total_frames": total_frames or 0}
