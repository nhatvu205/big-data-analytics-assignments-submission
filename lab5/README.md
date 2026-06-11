# People Counting System — Big Data Architecture

## Overview
This project implements a distributed people-counting pipeline for a single camera feed using a **Big Data streaming architecture**.

```text
[Camera / Video]
      |
      v
Camera Server -> Kafka topic: raw-frames -> Processing Server -> Kafka topic: detection-results -> Storage Server/API
```

Kafka is the Big Data layer: it decouples producers and consumers, supports horizontal scaling, and mirrors how real-time streaming systems ingest data from multiple cameras.

## Tech stack
- Apache Kafka: streaming backbone
- YOLOv8n: person detection
- MongoDB: default storage backend
- SQLite: fallback storage backend
- FastAPI: query API
- Streamlit: lightweight dashboard
- Google Colab: primary demo environment

## Repository structure
- `camera_server/`: frame producer
- `processing_server/`: YOLO inference consumer/producer
- `storage_server/`: persistence consumer + FastAPI API
- `dashboard/`: Streamlit app
- `utils/`: Kafka and visualization helpers
- `notebooks/full_pipeline_colab.ipynb`: end-to-end Colab notebook
- `results/`: sample output artifacts
- `videos/`: place your input video here

## How to run on Google Colab
1. Open `notebooks/full_pipeline_colab.ipynb` in Google Colab.
2. Select **Runtime > Change runtime type > T4 GPU**.
3. Run all cells.
4. Wait for the pipeline to process frames, then inspect `results/sample_output.json` and `results/annotated_frame.jpg`.

## How to run locally
### 1. Start Kafka and MongoDB
```bash
docker compose up -d
```

### 2. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 3. Add your input video
Place your video in the `videos/` folder, for example:
```text
videos/input.mp4
```

### 4. Start the services
In separate terminals:
```bash
python -c "from storage_server.consumer import run_storage_consumer; run_storage_consumer()"
python -m uvicorn storage_server.api:app --host 0.0.0.0 --port 8000
python -c "from processing_server.consumer import run_processing_server; run_processing_server()"
python -c "from camera_server.producer import run_camera_server; run_camera_server('videos/input.mp4')"
```

### 5. Open the dashboard
```bash
streamlit run dashboard/app.py
```

## API endpoints
- `GET /health`
- `GET /latest?limit=10`
- `GET /stats`

## Big Data justification
Although this assignment uses a single camera, the architecture is built for Big Data streaming:
- many camera producers can publish to Kafka simultaneously
- multiple processing workers can scale out by Kafka partitions / consumer groups
- storage and analytics can consume the same stream independently
- this is a simplified Kappa-style streaming pipeline

## Input data
Place a real test video under `videos/` and pass that path to the camera server.

## Sample results
See `results/sample_output.json` and `results/annotated_frame.jpg`.
