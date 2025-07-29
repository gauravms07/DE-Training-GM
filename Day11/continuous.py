from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("ContinuousMode") \
        .master("local[*]") \
        .getOrCreate()

df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

jvm = spark._jvm

continuous_t = jvm.org.apache.spark.sql.streaming.Trigger.Continuous("1 second")

writer = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False)

writer.__jwrite = writer._jwrite.trigger(continuous_t)

query = writer.start()

print("Running in continuous mode for 10 seconds")

time.sleep(10)
query.stop()