from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count, min, to_timestamp, sum, when
)
from pyspark.sql.types import (
    StructType, StringType, DoubleType, BooleanType
)

from influxdb_client import InfluxDBClient, Point, WriteOptions
import os

# -------------------------
# InfluxDB config
# -------------------------
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
ORG = os.getenv("INFLUX_ORG")
BUCKET = os.getenv("INFLUX_BUCKET")

client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=ORG
)

write_api = client.write_api(
    write_options=WriteOptions(batch_size=500, flush_interval=1000)
)

# -------------------------
# Spark
# -------------------------
spark = SparkSession.builder \
    .appName("KafkaToInflux") \
    .getOrCreate()

schema = StructType() \
    .add("id", StringType()) \
    .add("name", StringType()) \
    .add("absolute_magnitude", DoubleType()) \
    .add("is_hazardous", BooleanType()) \
    .add("velocity_kph", DoubleType()) \
    .add("miss_distance_km", DoubleType()) \
    .add("approach_date", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.0.0.5:9092") \
    .option("subscribe", "asteroid_raw") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("approach_ts", to_timestamp(col("approach_date")))

agg = parsed \
    .groupBy(window(col("approach_ts"), "1 minute")) \
    .agg(
        count("*").alias("asteroid_count"),
        avg("velocity_kph").alias("avg_velocity"),
        sum(when(col("is_hazardous") == True, 1).otherwise(0)).alias("hazardous_count"),
        min("miss_distance_km").alias("min_distance")
    )

def write_to_influx(batch_df, batch_id):
    for row in batch_df.collect():  # OK for small batches
        point = (
            Point("asteroid_metrics")
            .tag("window", str(row["window"]))
            .field("asteroid_count", int(row["asteroid_count"]))
            .field("avg_velocity_kph", float(row["avg_velocity"]))
            .field("hazardous_count", int(row["hazardous_count"]))
            .field("min_miss_distance_km", float(row["min_distance"]))
            .time(row["window"].start)
        )
        write_api.write(bucket=BUCKET, record=point)

query = agg.writeStream \
    .foreachBatch(write_to_influx) \
    .outputMode("update") \
    .start()

query.awaitTermination()
