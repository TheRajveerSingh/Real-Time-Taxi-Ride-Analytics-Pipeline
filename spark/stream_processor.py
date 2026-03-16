from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col, from_json, current_timestamp, expr, avg, count, to_timestamp
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
    .option("startingOffsets", "earliest") \
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

# -------------------- SILVER --------------------
silver_df = json_df.select(
    from_json(col("raw_event"), schema).alias("data"),
    col("ride_id")
).select("ride_id", "data.*")

silver_df = silver_df.select(
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
# -------------------- GOLD --------------------
gold_df = silver_df \
    .withColumn("pickup_time", to_timestamp("tpep_pickup_datetime")) \
    .withWatermark("pickup_time", "10 minutes") \
    .groupBy(window("pickup_time", "1 minute")) \
    .agg(
        avg("fare_amount").alias("avg_fare"),
        avg("trip_distance").alias("avg_trip_distance"),
        count("*").alias("ride_count")
    ) \
    .withColumn("agg_time", current_timestamp()) \
    .withColumn("agg_date", to_date("agg_time")) \
    .drop("window")

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

# -------------------- Await Termination --------------------
spark.streams.awaitAnyTermination()