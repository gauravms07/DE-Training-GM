from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("AirportParking") \
    .master("local[*]") \
    .getOrCreate()

parking_schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("terminal", StringType()),
    StructField("zone", StringType()),
    StructField("slot_id", IntegerType()),
    StructField("status", StringType())
])

stream_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 100) \
    .load() \
    .withColumn("terminal", expr("CASE WHEN value % 3 = 0 THEN 'A' WHEN value % 3 = 1 THEN 'B' ELSE 'C' END")) \
    .withColumn("zone", expr("concat('P', (value % 5) + 1)")) \
    .withColumn("slot_id", (col("value") % 500) + 1) \
    .withColumn("status", expr("CASE WHEN value % 4 = 0 THEN 'Occupied' ELSE 'Available' END")) \
    .withColumn("event_time", from_unixtime(col("timestamp").cast("long")).cast("timestamp")) \
    .select("event_time", "terminal", "zone", "slot_id", "status")



zone_stats = stream_df.groupBy("terminal", "zone", window("event_time", "5 seconds")) \
    .agg(
        (sum(when(col("status") == "Occupied", 1).otherwise(0)) / count("*") * 100).alias("occupancy_percent")
    ) \
    .withColumn("is_congested", col("occupancy_percent") > 80)



avg_occupancy = stream_df.groupBy("terminal", window("event_time", "1 minute")) \
    .agg(
        (sum(when(col("status") == "Occupied", 1).otherwise(0)) / count("*") * 100).alias("avg_occupancy_percent")
    )



terminal_alerts = stream_df.groupBy("terminal", window("event_time", "15 seconds")) \
    .agg(
        (sum(when(col("status") == "Occupied", 1).otherwise(0)) / count("*") * 100).alias("terminal_occupancy")
    ) \
    .filter(col("terminal_occupancy") > 85) \
    .withColumn("alert", lit("High Terminal Utilization"))



query1 = zone_stats.writeStream.outputMode("update").format("console").start()
query2 = avg_occupancy.writeStream.outputMode("update").format("console").start()
query3 = terminal_alerts.writeStream.outputMode("update").format("console").start()



spark.streams.awaitAnyTermination()
