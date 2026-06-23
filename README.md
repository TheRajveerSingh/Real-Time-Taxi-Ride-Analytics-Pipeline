# TaxiStream: Real-Time Taxi Ride Streaming Data Engineering Pipeline

## Overview

TaxiStream is a real-time data engineering pipeline that processes taxi ride events using streaming technologies. The system simulates live ride events from NYC taxi data and processes them in real-time to compute analytics such as average fare and trip distance. Instead of traditional batch processing with delays, this architecture provides insights as they occur.

## Project Motivation

Transportation platforms generate large volumes of ride data every second. Traditional batch processing systems analyze data only after it has been stored, introducing delays in decision-making. This project demonstrates how to build a real-time streaming architecture for immediate insights into ride demand, revenue, trip patterns, and system performance.

## Architecture

```
NYC Taxi Dataset
     ↓
Python Kafka Producer (simulates live ride events)
     ↓
Apache Kafka Topic (taxi_rides)
     ↓
Apache Spark Structured Streaming Consumer
     ↓
Real-Time Analytics (average fare, distance, etc.)
     ↓
Console Output (current stage)
Cassandra Storage (future stage: bronze/silver/gold)
Airflow Orchestration (future stage)
```

## Features

### Current Implementation
- **Kafka Producer**: Streams taxi ride events from NYC taxi dataset in real-time (with configurable delays)
- **Spark Streaming**: Processes streaming JSON data from Kafka
- **Real-Time Analytics**: Computes average fare and trip distance on-the-fly
- **Docker Containerization**: Services packaged for easy deployment

### Future Enhancements
- **Cassandra Storage**: Multi-layer data warehouse (bronze/silver/gold)
- **Airflow Orchestration**: Workflow scheduling and monitoring
- **Advanced Analytics**: Window functions, anomaly detection, trend analysis

## Technologies Used

| Component | Purpose |
|-----------|---------|
| **Python** | Kafka producer for streaming taxi events |
| **Apache Kafka** | Streaming message broker (topic: `taxi_rides`) |
| **Apache Spark** | Structured streaming consumer for real-time processing |
| **Cassandra** | Distributed NoSQL storage for results |
| **Apache Airflow** | Workflow orchestration and scheduling |
| **Docker** | Containerization for deployment |
| **Java (OpenJDK)** | Runtime for Spark and Kafka |

## Project Structure

```
taxistream/
├── producer/
│   └── producer.py           # Kafka producer (streams taxi events)
├── spark/
│   └── stream_processor.py   # Spark consumer (real-time analytics)
├── airflow/                  # Airflow DAGs (future)
├── data/                     # Taxi dataset files
├── checkpoints/              # Spark streaming checkpoints
│   ├── bronze/               # Raw layer
│   ├── silver/               # Cleaned layer (future)
│   └── gold/                 # Analytics layer (future)
├── docker/                   # Docker configuration files
├── docker-compose.yml        # Service orchestration
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

## Prerequisites

- **Python 3.10+**
- **Java (OpenJDK 11+)**
- **Docker & Docker Compose**
- **Apache Spark 3.4+**
- **Apache Kafka 3.5+**

## Installation & Setup

### 1. Environment Setup

Clone the repository and create a Python environment:

```bash
cd taxistream
conda create -n taxistream python=3.10
conda activate taxistream
pip install -r requirements.txt
```

### 2. Set Environment Variables

Set up the following environment variables for Spark to work properly:

**Windows:**
```bash
setx JAVA_HOME "C:\Program Files\OpenJDK\jdk-11"
setx SPARK_HOME "C:\path\to\spark"
setx HADOOP_HOME "C:\path\to\hadoop"
```

**Linux/Mac:**
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export SPARK_HOME=/path/to/spark
export PATH=$PATH:$SPARK_HOME/bin
```

### 3. Start Services with Docker

```bash
docker-compose up -d
```

This starts:
- **Zookeeper**: Coordination service (port 2181)
- **Kafka**: Message broker (port 9092)
- **Cassandra**: Data storage (port 9042)

### 4. Verify Services

```bash
# Check Zookeeper
telnet localhost 2181

# Check Kafka
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Check Cassandra
cqlsh localhost 9042
```

## Running the Pipeline

### Step 1: Start the Kafka Producer

The producer reads the NYC taxi dataset and streams events to Kafka:

```bash
cd producer
python producer.py
```

Expected output:
```
Sent ride 0
Sent ride 1
Sent ride 2
...
```

### Step 2: Start the Spark Streaming Consumer

In a new terminal, start the Spark consumer to process streaming data:

```bash
cd spark
python stream_processor.py
```

The consumer will read from Kafka and display real-time analytics.

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

## Configuration

### Producer Configuration
Edit `producer/producer.py` to adjust:
- **Number of rows**: `df.head(250)` - change to desired row count
- **Streaming delay**: `time.sleep(0.5)` - adjust for faster/slower streaming
- **Kafka bootstrap servers**: `localhost:9092` - change if running on different host

### Spark Configuration
Edit `spark/stream_processor.py` to adjust:
- **Kafka bootstrap servers**: `localhost:9092`
- **Processing batch interval**: Modify in `readStream` options
- **Output mode**: Change from `append` to `update` or `complete` as needed
- **Cassandra connection**: Update in `spark.cassandra.connection.host`

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
| Hadoop winutils error (Windows) | Add `HADOOP_HOME` environment variable |

## Future Roadmap

- [ ] Implement bronze/silver/gold data warehouse layers in Cassandra
- [ ] Add Airflow DAGs for pipeline orchestration
- [ ] Implement advanced window functions and aggregations
- [ ] Add anomaly detection for unusual ride patterns
- [ ] Create Grafana dashboards for visualization
- [ ] Add unit tests for producer and consumer
- [ ] Implement error handling and retry logic
- [ ] Deploy to cloud platforms (AWS/GCP/Azure)

## Dataset

**Source**: NYC Taxi Trip Records  
**Data Format**: Parquet  
**Location**: `data/yellow_tripdata_2025-01.parquet`

The dataset contains detailed taxi trip information including:
- Pickup and dropoff times
- Passenger count
- Trip distance
- Fare breakdown
- Payment information

## Performance Notes

- Current setup processes ~250 rides with 0.5s delay per ride
- Spark checkpointing ensures fault tolerance
- Kafka retains messages for 7 days by default
- Cassandra provides distributed storage for redundancy

## Contributing

Feel free to extend this project by:
- Adding new analytics computations
- Implementing additional data sinks
- Improving error handling
- Adding comprehensive tests
- Optimizing performance

## License

This project is open source and available under the MIT License.

## Contact & Support

For questions or issues, please open an issue on the repository or contact the project maintainer.

---

**Last Updated**: March 2026  
**Status**: In Development (MVP Complete)
