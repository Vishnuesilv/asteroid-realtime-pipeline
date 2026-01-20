from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType
import psycopg2

# -----------------------------
# TIMESCALEDB WRITE FUNCTION
# -----------------------------
def write_to_timescale(df, batch_id):
    rows = df.collect()
    
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="asteroid",
        user="postgres",
        password="Visvms871$"
    )

    cursor = conn.cursor()

    insert_query = """
        INSERT INTO asteroid_events 
        (id, name, absolute_magnitude, is_hazardous, miss_distance_km, velocity_kph, approach_date, event_time)
        VALUES (%s,%s,%s,%s,%s,%s,%s,NOW());
    """

    for row in rows:
        cursor.execute(insert_query, (
            row["id"],
            row["name"],
            row["absolute_magnitude"],
            row["is_hazardous"],
            row["miss_distance_km"],
            row["velocity_kph"],
            row["approach_date"]
        ))

    conn.commit()
    cursor.close()
    conn.close()


# -----------------------------
# SPARK SESSION
# -----------------------------
spark = SparkSession.builder \
    .appName("KafkaToHDFSAndTimescale") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# SCHEMA
# -----------------------------
schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("absolute_magnitude", FloatType()),
    StructField("is_hazardous", BooleanType()),
    StructField("miss_distance_km", StringType()),
    StructField("velocity_kph", StringType()),
    StructField("approach_date", StringType())
])

# -----------------------------
# KAFKA STREAM
# -----------------------------
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.0.0.5:9092") \
    .option("subscribe", "asteroid_raw") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json")
parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# -----------------------------
# WRITE TO HDFS
# -----------------------------
hdfs_writer = parsed_df \
    .writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "/asteroid_raw") \
    .option("checkpointLocation", "/checkpoint_asteroid") \
    .trigger(processingTime="10 seconds") \
    .start()

# -----------------------------
# WRITE TO TIMESCALE (foreachBatch)
# -----------------------------
ts_writer = parsed_df.writeStream \
    .foreachBatch(write_to_timescale) \
    .outputMode("update") \
    .start()

spark.streams.awaitAnyTermination()

