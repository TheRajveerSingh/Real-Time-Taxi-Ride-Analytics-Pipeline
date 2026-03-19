from kafka import KafkaProducer
import pandas as pd
import json
import time

# Kafka connection
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load dataset
df = pd.read_parquet('data/yellow_tripdata_2025-01.parquet')

# Limit rows for testing
df = df.head(250)

topic = "taxi_rides"

for index, row in df.iterrows():

    data = row.to_dict()

    # Convert timestamps to string
    for key, value in data.items():
        if hasattr(value, "isoformat"):
            data[key] = value.isoformat()

    producer.send(topic, value=data)

    print(f"Sent ride {index}")

    time.sleep(0.5) # simulate real-time streaming