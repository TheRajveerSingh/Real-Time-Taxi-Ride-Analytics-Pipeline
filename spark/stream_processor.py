from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col, from_json, current_timestamp, expr, avg, count, to_timestamp, when, to_json, struct, concat_ws
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import window

# -------------------- Spark Session --------------------
spark = SparkSession.builder \
    .appName("TaxiStreamingProcessor") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
    .getOrCreate()

spark.catalog.clearCache()
spark.sparkContext.setLogLevel("WARN")

# -------------------- Full schema (all fields from your Kafka JSON) --------------------
schema = StructType() \
    .add("VendorID", IntegerType()) \
    .add("tpep_pickup_datetime", StringType()) \
    .add("tpep_dropoff_datetime", StringType()) \
    .add("passenger_count", DoubleType()) \
    .add("trip_distance", DoubleType()) \
    .add("RatecodeID", DoubleType()) \
    .add("store_and_fwd_flag", StringType()) \
    .add("PULocationID", IntegerType()) \
    .add("DOLocationID", IntegerType()) \
    .add("payment_type", IntegerType()) \
    .add("fare_amount", DoubleType()) \
    .add("extra", DoubleType()) \
    .add("mta_tax", DoubleType()) \
    .add("tip_amount", DoubleType()) \
    .add("tolls_amount", DoubleType()) \
    .add("improvement_surcharge", DoubleType()) \
    .add("total_amount", DoubleType()) \
    .add("congestion_surcharge", DoubleType()) \
    .add("Airport_fee", DoubleType()) \
    .add("cbd_congestion_fee", DoubleType())

# -------------------- Read Kafka Stream --------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "taxi_rides") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) AS raw_event") \
            .withColumn("ride_id", expr("uuid()"))
# -------------------- BRONZE --------------------
bronze_df = json_df.withColumn("created_at", current_timestamp())
bronze_query = bronze_df.writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .options(table="bronze_rides", keyspace="taxi_streaming")
        .mode("append")
        .save()
    ) \
    .option("checkpointLocation", "checkpoints/bronze") \
    .outputMode("append") \
    .start()


# -------------------- PARSE + VALIDATE --------------------
parsed_df = json_df.select(
    from_json(col("raw_event"), schema).alias("data"),
    col("ride_id"),
    col("raw_event")
).select("ride_id", "raw_event", "data.*")

validated_df = parsed_df.withColumn(
    "validation_errors",
    concat_ws(", ",
        when(col("fare_amount").isNull() | (col("fare_amount") < 0), "invalid_fare"),
        when(col("trip_distance").isNull() | (col("trip_distance") < 0), "invalid_distance"),
        when(col("tpep_pickup_datetime").isNull(), "missing_pickup_time"),
        when(col("tpep_dropoff_datetime").isNull(), "missing_dropoff_time"),
        when(col("PULocationID").isNull(), "missing_pu_location"),
        when(col("DOLocationID").isNull(), "missing_do_location")
    )
).withColumn("is_valid", col("validation_errors") == "")

valid_df = validated_df.filter(col("is_valid")).drop("is_valid", "validation_errors")
invalid_df = validated_df.filter(~col("is_valid"))

# -------------------- SILVER (from valid records only) --------------------
silver_df = valid_df.select(
    col("ride_id"),
    col("VendorID").alias("vendorid"),
    col("tpep_pickup_datetime"),
    col("tpep_dropoff_datetime"),
    col("passenger_count"),
    col("trip_distance"),
    col("RatecodeID").alias("ratecodeid"),
    col("store_and_fwd_flag"),
    col("PULocationID").alias("pulocationid"),
    col("DOLocationID").alias("dolocationid"),
    col("payment_type"),
    col("fare_amount"),
    col("extra"),
    col("mta_tax"),
    col("tip_amount"),
    col("tolls_amount"),
    col("improvement_surcharge"),
    col("total_amount"),
    col("congestion_surcharge"),
    col("Airport_fee").alias("airport_fee"),
    col("cbd_congestion_fee"),
    to_timestamp("tpep_pickup_datetime").alias("pickup_datetime"),
    to_timestamp("tpep_dropoff_datetime").alias("dropoff_datetime")
)

# -------------------- DLQ SINK 1: Kafka topic --------------------
dlq_kafka_df = invalid_df.select(
    col("ride_id").alias("key"),
    to_json(struct(col("ride_id"), col("validation_errors"), col("raw_event"))).alias("value")
)

dlq_kafka_query = dlq_kafka_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "taxi_rides_dlq") \
    .option("checkpointLocation", "checkpoints/dlq_kafka") \
    .outputMode("append") \
    .start()

# -------------------- DLQ SINK 2: Cassandra (for easy demo/verification) --------------------
dlq_cassandra_df = invalid_df.select(
    col("ride_id"),
    col("validation_errors").alias("reason"),
    col("raw_event"),
    current_timestamp().alias("rejected_at")
)

dlq_cassandra_query = dlq_cassandra_df.writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .options(table="dlq_rides", keyspace="taxi_streaming")
        .mode("append")
        .save()
    ) \
    .option("checkpointLocation", "checkpoints/dlq_cassandra") \
    .outputMode("append") \
    .start()
# -------------------- GOLD --------------------

gold_df = (
    silver_df
    .withColumn("pickup_time", col("pickup_datetime"))
    .withWatermark("pickup_time", "10 minutes")
    .groupBy(window("pickup_time", "1 minute"))
    .agg(
        avg("fare_amount").alias("avg_fare"),
        avg("trip_distance").alias("avg_trip_distance"),
        count("*").alias("ride_count")
    )
    .withColumn(
        "surge_flag",
        when(col("ride_count") > 5, "HIGH_DEMAND").otherwise("NORMAL")
    )
    .withColumn(
        "fare_anomaly",
        when(col("avg_fare") > 100, "ABNORMAL").otherwise("NORMAL")
    )
    .withColumn("agg_time", current_timestamp())
    .withColumn("agg_date", to_date("agg_time"))
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end", col("window.end"))
    .drop("window")
)

hotspot_df = (
    silver_df
    .withColumn("pickup_time", col("pickup_datetime"))
    .withWatermark("pickup_time", "10 minutes")
    .groupBy(
        window("pickup_time", "5 minutes"),
        col("pulocationid")
    )
    .agg(
        count("*").alias("ride_count")
    )
    .withColumn(
        "hotspot_flag",
        when(col("ride_count") > 10, "HOTSPOT").otherwise("NORMAL")
    )
    .withColumn("agg_time", current_timestamp())
    .withColumn("agg_date", to_date("agg_time"))
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end", col("window.end"))
    .drop("window")
)

gold_query = gold_df.writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .options(table="gold_rides", keyspace="taxi_streaming")
        .mode("append")
        .save()
    ) \
    .option("checkpointLocation", "checkpoints/gold") \
    .outputMode("update") \
    .start()

hotspot_query = hotspot_df.writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .options(table="hotspot_rides", keyspace="taxi_streaming")
        .mode("append")
        .save()
    ) \
    .option("checkpointLocation", "checkpoints/hotspot") \
    .outputMode("update") \
    .start()

# -------------------- Await Termination --------------------
spark.streams.awaitAnyTermination()
