# TaxiStream: Real-Time Taxi Ride Streaming & Anomaly Detection Pipeline

## Overview

TaxiStream is a real-time data engineering pipeline that ingests, processes, and analyzes taxi ride events using industry-standard streaming technologies. The system simulates live ride events from the NYC Taxi dataset and processes them continuously to surface real-time analytics — ride demand, fare trends, surge conditions, and location-based hotspots — instead of relying on delayed batch processing.

## Project Motivation

Transportation platforms generate large volumes of ride data every second. Traditional batch systems only analyze this data after it's stored, introducing delays that limit real-time decision-making. This project demonstrates how to build a real-time streaming architecture — using Apache Kafka and Spark Structured Streaming — capable of ingesting, validating, aggregating, and visualizing ride events as they occur.

## Architecture

```
NYC Taxi Dataset
     ↓
Python Kafka Producer (simulates live ride events)
     ↓
Apache Kafka Topic (taxi_rides)
     ↓
Apache Spark Structured Streaming
     ↓
     ├── Validation Layer ──→ Invalid records ──→ Dead Letter Queue
     │                                              (Kafka topic + Cassandra table)
     ↓ (valid records only)
     ├── Bronze Layer (raw events)          → Cassandra
     ├── Silver Layer (cleaned, structured)  → Spark-only (not persisted)
     └── Gold Layer (aggregated analytics)   → Cassandra
              ├── Surge Detection
              ├── Fare Anomaly Detection
              └── Hotspot Detection (location-based)
     ↓
Grafana Dashboards (real-time visualization)
```

## Features

### Current Implementation
- **Kafka Producer**: Streams taxi ride events from the NYC Taxi dataset, simulating real-time arrival
- **Spark Structured Streaming**: Multi-sink streaming consumer processing Kafka events via Spark's micro-batch execution model, with each trigger interval reading new Kafka offsets, parsing JSON, validating records, and writing to all downstream sinks
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned, in-memory) → Gold (aggregated) layers, with Bronze and Gold persisted to Cassandra
- **Stateful Stream Processing**: 
  - **Event-time windowing** — ride events are aggregated into fixed time windows (1-minute windows for Gold metrics, 5-minute windows for hotspot detection) based on the ride's actual pickup timestamp, not arrival time
  - **Watermarking** — a 10-minute watermark tolerates late-arriving or out-of-order events without holding aggregation state indefinitely
  - **Checkpointing** — each streaming sink (Bronze, Gold, Hotspot, DLQ-Kafka, DLQ-Cassandra) maintains its own checkpoint directory, tracking Kafka offsets and aggregation state so the pipeline can resume correctly after a restart without reprocessing or losing data
- **Dead Letter Queue (DLQ)**: Invalid records (negative fares, missing timestamps/locations, etc.) are filtered before reaching Silver/Gold and routed to both a dedicated Kafka topic and a Cassandra table for inspection
- **Real-Time Analytics**:
  - **Surge Detection** — flags high-demand time windows based on ride volume
  - **Fare Anomaly Detection** — flags abnormal average fares per window
  - **Hotspot Detection** — flags high-density pickup locations
- **Grafana Dashboards**: Live visualization connected directly to Cassandra, covering ride volume, fare trends, surge events, hotspots, and DLQ rejections
- **Docker Containerization**: Kafka, Zookeeper, Cassandra, and Grafana all run via Docker Compose

### Planned / Future Work
- **Apache Airflow Orchestration**: Automate the startup sequence (Docker infra → Kafka topics → Spark job → producer) via a scheduled DAG.
  - *Note: Airflow's scheduler requires a POSIX-compliant OS and does not run natively on Windows. This is planned to be completed in a WSL2 or Linux-based environment.*
- Continuous (non-terminating) producer loop instead of a fixed batch size
- Additional dimensional analytics (e.g., rides by payment type)
- Automated tests for producer and stream processor logic

## Technologies Used

| Component | Purpose |
|-----------|---------|
| **Python** | Kafka producer; ride event simulation |
| **Apache Kafka** | Streaming message broker (topics: `taxi_rides`, `taxi_rides_dlq`) |
| **Apache Zookeeper** | Kafka cluster coordination |
| **Apache Spark (Structured Streaming)** | Real-time stream processing, validation, and windowed aggregation |
| **Apache Cassandra** | Distributed NoSQL storage for Bronze, Gold, and DLQ layers |
| **Grafana** | Real-time dashboarding, connected directly to Cassandra |
| **Docker / Docker Compose** | Containerized infrastructure for Kafka, Zookeeper, Cassandra, Grafana |
| **Java (OpenJDK)** | Runtime dependency for Spark and Kafka |

## Project Structure

```
taxistream/
├── airflow/                  # Reserved for future Airflow DAGs
├── checkpoints/               # Spark streaming checkpoints (gitignored — runtime state)
│   ├── bronze/
│   ├── gold/
│   ├── hotspot/
│   ├── dlq_kafka/
│   └── dlq_cassandra/
├── data/                       # NYC taxi dataset (gitignored — large binary file)
├── producer/
│   └── producer.py            # Kafka producer
├── spark/
│   └── stream_processor.py    # Spark Structured Streaming job (Bronze/Silver/Gold + DLQ)
├── .env                        # Local secrets (gitignored) — Grafana admin credentials
├── .gitignore
├── docker-compose.yml          # Kafka, Zookeeper, Cassandra, Grafana services
├── requirements.txt
├── test_data.py                # Ad-hoc dataset inspection script
└── README.md
```

## Prerequisites

- **Python 3.10+**
- **Java (OpenJDK 11+)**
- **Docker & Docker Compose**
- **Apache Spark 3.5+**
- **Apache Kafka 3.5+ client packages** (via `spark-submit --packages`)

## Installation & Setup

### 1. Environment Setup

```bash
conda create -n taxic python=3.10
conda activate taxic
pip install -r requirements.txt
```

### 2. Environment Variables (Windows)

```bash
setx JAVA_HOME "C:\path\to\jdk"
setx SPARK_HOME "C:\path\to\spark"
setx HADOOP_HOME "C:\path\to\hadoop"
```

### 3. Grafana Credentials

Create a `.env` file in the project root (not committed to git):

```env
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=your_password_here
```

### 4. Start Infrastructure

```bash
docker compose up -d
docker ps
```

This starts: **Zookeeper** (2181), **Kafka** (9092), **Cassandra** (9042), **Grafana** (3000).

### 5. Create Kafka Topics

```bash
docker exec -it kafka kafka-topics --create --topic taxi_rides --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic taxi_rides_dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### 6. Create Cassandra Keyspace & Tables

```bash
docker exec -it cassandra cqlsh
```

```sql
CREATE KEYSPACE taxi_streaming WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
USE taxi_streaming;
-- See /docs or stream_processor.py comments for full table DDL
-- (bronze_rides, silver_rides, gold_rides, hotspot_rides, dlq_rides)
```

## Running the Pipeline

Open **three terminals**:

**Terminal 1 — Producer:**
```bash
conda activate taxic
python producer/producer.py
```

**Terminal 2 — Spark Streaming:**
```bash
conda activate taxic
set PYSPARK_PYTHON=<path to your conda env python.exe>
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 spark/stream_processor.py
```

**Terminal 3 — Verification (optional):**
```bash
docker exec -it cassandra cqlsh
USE taxi_streaming;
SELECT * FROM gold_rides LIMIT 5;
SELECT * FROM dlq_rides LIMIT 5;
```

### Monitoring

- **Spark UI**: `http://localhost:4040/jobs/`
- **Grafana Dashboards**: `http://localhost:3000`

## Data Format

The producer sends taxi ride events as JSON with the following schema:

```json
{
  "VendorID": 1,
  "tpep_pickup_datetime": "2025-01-01T12:00:00",
  "tpep_dropoff_datetime": "2025-01-01T12:15:00",
  "passenger_count": 2,
  "trip_distance": 2.5,
  "RatecodeID": 1,
  "store_and_fwd_flag": "N",
  "PULocationID": 142,
  "DOLocationID": 166,
  "payment_type": 1,
  "fare_amount": 12.50,
  "extra": 0.0,
  "mta_tax": 0.50,
  "tip_amount": 2.50,
  "tolls_amount": 0.0,
  "improvement_surcharge": 0.30,
  "total_amount": 15.80,
  "congestion_surcharge": 0.0,
  "Airport_fee": 0.0,
  "cbd_congestion_fee": 0.0
}
```

## Grafana Dashboard

The `Taxi Streaming Analytics` dashboard includes:
1. **Ride Count Per Minute** — time series of ride volume
2. **Average Fare Trend** — time series of average fare per window
3. **Surge Events (High Demand)** — table of windows flagged `HIGH_DEMAND`
4. **Pickup Hotspots** — table of ride density per pickup location
5. **Dead Letter Queue — Rejected Records** — table of invalid records caught pre-processing

> **Note:** Panels filter on `agg_date` (the date the pipeline *processed* the record, not the ride's original date). Update this value in each panel's query when running the pipeline on a new day.

## Configuration

### Producer (`producer/producer.py`)
- **Number of rows**: `df.head(250)` — change to send more/fewer simulated rides per run
- **Streaming delay**: `time.sleep(0.5)` — adjust for faster/slower simulated arrival rate
- **Kafka bootstrap servers**: `localhost:9092`

### Spark (`spark/stream_processor.py`)
- **Kafka bootstrap servers**: `localhost:9092`
- **Kafka starting offset**: `latest` (only processes new messages — avoids reprocessing full history on restart)
- **Cassandra connection**: `spark.cassandra.connection.host` config
- **Watermark**: `10 minutes` (tolerance for late/out-of-order events)
- **Detection thresholds** (tune based on dataset volume):
  - Surge: `ride_count > 5`
  - Fare anomaly: `avg_fare > 100`
  - Hotspot: `ride_count > 10` per location per 5-minute window

## Dead Letter Queue (DLQ) Design

Records are validated in Spark before reaching the Silver/Gold layers. A record is rejected if it has:
- A missing or negative `fare_amount`
- A missing or negative `trip_distance`
- A missing pickup/dropoff timestamp
- A missing pickup/dropoff location ID

Rejected records are written to **both**:
- A Kafka topic (`taxi_rides_dlq`) — for downstream reprocessing or alerting in a production system
- A Cassandra table (`dlq_rides`) — for easy inspection via `cqlsh` or Grafana

## Dependencies

Key packages (see `requirements.txt` for full list):
```
pyspark==4.1.1
kafka-python==2.3.0
pandas==2.3.3
pyarrow==23.0.1
cassandra-driver==3.29.3
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Spark not finding Java | Ensure `JAVA_HOME` is set correctly |
| Kafka connection refused | Verify Docker containers are running: `docker ps` |
| Cassandra driver error | Install: `pip install cassandra-driver` |
| Port conflicts | Change ports in `docker-compose.yml` |
| Hadoop winutils error (Windows) | Set `HADOOP_HOME` environment variable |
| Spark resumes from wrong offset after restart | Delete the `checkpoints/` folder and recreate Kafka topics — see Running the Pipeline |
| `NameError` on Spark functions (e.g. `when`, `to_json`) | Ensure all `pyspark.sql.functions` imports are consolidated at the top of `stream_processor.py` |
| Grafana panel shows "No data" | Check that the panel's `agg_date` filter matches today's date — see Grafana Dashboard notes |

## Dataset

**Source**: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
**Format**: Parquet
**File**: `data/yellow_tripdata_2025-01.parquet`

## License

MIT License.

---

**Last Updated**: July 2026
**Status**: Core pipeline complete (Kafka → Spark → Cassandra → Grafana, with DLQ). Airflow orchestration planned as next step.