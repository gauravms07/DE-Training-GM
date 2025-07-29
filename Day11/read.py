from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, current_timestamp
import os

spark = SparkSession.builder \
    .appName("DeltaLakeWindowedAggregation") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

delta_path = "/tmp/delta/windowed_aggregates"
checkpoint_path = "/tmp/checkpoints/delta_demo"
os.system(f"rm -rf {delta_path} {checkpoint_path}")

stream_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) \
    .load() \
    .withColumn("event_time", current_timestamp())

windowed_counts = stream_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "10 seconds")) \
    .count()

query = windowed_counts.writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_path) \
    .option("path", delta_path) \
    .start()

query.awaitTermination()