from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType

# 1. Start Spark
spark = SparkSession.builder \
    .appName("AsteroidKafkaToHDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Starting Spark Streaming Consumer...")

# 2. Schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("absolute_magnitude", FloatType(), True),
    StructField("is_hazardous", BooleanType(), True),
    StructField("miss_distance_km", StringType(), True),
    StructField("velocity_kph", StringType(), True),
    StructField("approach_date", StringType(), True)
])

# 3. Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.0.0.5:9092") \
    .option("subscribe", "asteroid_raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("Connected to Kafka topic: asteroid_raw")

# 4. Convert binary → JSON string → parsed JSON
json_df = df.selectExpr("CAST(value AS STRING) AS json")
parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# 5. Convert row → single JSON line string
output_df = parsed_df.select(to_json(struct("*")).alias("value"))

# 6. WRITE AS TEXT (fixes Spark bug)
query = output_df.writeStream \
    .format("text") \
    .outputMode("append") \
    .option("path", "/asteroid_raw") \
    .option("checkpointLocation", "/checkpoint_asteroid") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming started. Writing to HDFS at: /asteroid_raw")

query.awaitTermination()

