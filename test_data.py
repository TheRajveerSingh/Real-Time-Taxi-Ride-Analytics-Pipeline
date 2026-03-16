from cassandra.cluster import Cluster
from datetime import datetime

try:
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('taxi_streaming')
    
    session.execute(
        "INSERT INTO bronze_rides (ride_id, created_at, ride_event) VALUES (uuid(), %s, %s)",
        (datetime.now(), '{"test":1}')
    )
    
    print("✅ Insert successful!")
except Exception as e:
    print("❌ Error:", e)