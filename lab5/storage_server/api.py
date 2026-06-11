"""FastAPI endpoints for querying stored detections."""

from fastapi import FastAPI

from storage_server.db import StorageDB

app = FastAPI(title="People Counting Storage API")
db = StorageDB()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "storage": "mongo" if db.use_mongo else "sqlite"}


@app.get("/latest")
def get_latest(limit: int = 10) -> dict:
    return {"results": db.get_latest(limit=limit)}


@app.get("/stats")
def get_stats() -> dict:
    return db.get_stats()
