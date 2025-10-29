from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, BooleanType, IntegerType

# Define schema for incoming JSON
schema = StructType() \
    .add("vid", StringType()) \
    .add("tmstmp", StringType()) \
    .add("lat", StringType()) \
    .add("lon", StringType()) \
    .add("hdg", StringType()) \
    .add("pid", IntegerType()) \
    .add("rt", StringType()) \
    .add("des", StringType()) \
    .add("pdist", IntegerType()) \
    .add("dly", BooleanType()) \
    .add("tatripid", StringType()) \
    .add("origtatripno", StringType()) \
    .add("tablockid", StringType()) \
    .add("zone", StringType()) \
    .add("fetched_at", StringType())

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CTA Bus Kafka Consumer") \
    .getOrCreate()

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_bus_data") \
    .option("startingOffsets", "latest") \
    .load()

# Decode and parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select("data.*") \
    .filter(col("vid").isNotNull())  # Filter out any failed parses

# Write processed data to disk as JSON
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "output/processed_bus_data") \
    .option("checkpointLocation", "checkpoint/bus_data_checkpoint") \
    .start()

query.awaitTermination()