# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *

# spark = SparkSession.builder \
#         .appName("Practice_v2") \
#         .master("local[*]") \
#         .getOrCreate()

# parking_df = spark.readStream \
#     .format("rate") \
#     .option("rowsPerSecond", 100) \
#     .load() \
#     .withColumn("terminal", expr("CASE WHEN value % 3 = 0 THEN 'A' WHEN value % 3 = 1 THEN 'B' ELSE 'C' END")) \
#     .withColumn("zone", expr("concat('P', (value % 5) + 1)")) \
#     .withColumn("slot_id", (col("value") % 500) + 1) \
#     .withColumn("status", expr("CASE WHEN value % 4 = 0 THEN 'Occupied' ELSE 'Available' END")) \
#     .withColumn("event_time", from_unixtime(col("timestamp").cast("long")).cast("timestamp")) \
#     .select("event_time", "terminal", "zone", "slot_id", "status")

# zoneCongestionQuery = parking_df \
#     .groupBy("terminal", "zone", "status", window("event_time", "1 minute")) \
#     .agg(count("*").alias("status_count")) \
#     .filter("status == 'Occupied' AND status_count > 40.0") \
#     .select("terminal", "zone", "status_count", "window")


# query1 = zoneCongestionQuery.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", False) \
#     .queryName("ZoneCongestion") \
#     .start()

# print("Running in MicroBatch mode for 10 seconds") 
# query1.awaitTermination(10)

# trackAverageOccupancyQuery = parking_df \
#     .withColumn("occupied", expr("CASE WHEN status = 'Occupied' THEN 1 ELSE 0 END")) \
#     .groupBy(window("event_time", "15 seconds", "5 seconds"), "terminal") \
#     .agg(avg("occupied").alias("avg_occupancy")) \
#     .select("avg_occupancy > 0.85")

# query2 = trackAverageOccupancyQuery.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", False) \
#     .queryName("AverageOccupancy") \
#     .start()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_unixtime, count, window, avg
import time

# Start Spark session
spark = SparkSession.builder \
    .appName("DummyStream") \
    .master("local[*]") \
    .getOrCreate()

# Create a dummy streaming source
stream_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

# Simulate schema with parking fields
parking_df = stream_df.withColumn("terminal", expr("CASE WHEN value % 3 = 0 THEN 'A' WHEN value % 3 = 1 THEN 'B' ELSE 'C' END")) \
    .withColumn("zone", expr("concat('P', (value % 5) + 1)")) \
    .withColumn("slot_id", (col("value") % 500) + 1) \
    .withColumn("status", expr("CASE WHEN value % 4 = 0 THEN 'Occupied' ELSE 'Available' END")) \
    .withColumn("event_time", from_unixtime(col("timestamp").cast("long")).cast("timestamp"))

# Zone Congestion Detection with Watermark
zoneCongestionQuery = parking_df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy("terminal", "zone", "status", window("event_time", "1 minute")) \
    .agg(count("*").alias("status_count")) \
    .filter("status = 'Occupied' AND status_count > 40") \
    .select("terminal", "zone", "status_count", "window")

# Average Occupancy per terminal with Watermark
avaregeQuery = parking_df \
    .withWatermark("event_time", "2 minutes") \
    .withColumn("occupied", expr("CASE WHEN status='Occupied' THEN 1 ELSE 0 END")) \
    .groupBy(window("event_time", "15 seconds", "5 seconds"), "terminal") \
    .agg(avg("occupied").alias("avg_ocup")) \
    .filter("avg_ocup > 0.85")


# Start congestion query
query1 = zoneCongestionQuery.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

# Start occupancy query
query2 = avaregeQuery.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

print("Running in micro-batch mode for 20 seconds...\n")
time.sleep(20)

print("Running for 60 seconds...")
time.sleep(60)

query1.stop()
query2.stop()
print("Both queries stopped.")