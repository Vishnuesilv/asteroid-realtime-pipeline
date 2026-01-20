from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder.appName("KafkaToHDFS").getOrCreate()

schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("absolute_magnitude", DoubleType()),
    StructField("is_hazardous", BooleanType()),
    StructField("velocity_kph", DoubleType()),
    StructField("miss_distance_km", DoubleType()),
    StructField("approach_date", StringType())
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "asteroid_raw")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
)

def write_to_hdfs(batch_df, batch_id):
    (
        batch_df
        .coalesce(1)
        .write
        .mode("append")
        .json("hdfs://10.0.0.5:9000/data/asteroid/raw")
    )

query = (
    parsed.writeStream
    .foreachBatch(write_to_hdfs)
    .option("checkpointLocation", "hdfs://10.0.0.5:9000/data/asteroid/checkpoints")
    .start()
)

query.awaitTermination()
