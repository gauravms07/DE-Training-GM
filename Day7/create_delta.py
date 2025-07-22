from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import random

spark = SparkSession.builder.appName("OptimizeANDZOrderLab") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

# Create a DataFrame with 10000 rows
regions = ["US", "EU", "APAC", "LATAM", "AFRICA"]

data = [(i, f"Name_{i}", random.choice(regions)) for i in range(10000)]
df = spark.createDataFrame(data, ["id", "name", "region"])

# Write to Delta table
df.write.format("delta").mode("overwrite").save("delta-tables/employees")

import time

start = time.time()
spark.read.format("delta").load("delta-tables/employees").filter(col("region") == "US").count()
end = time.time()

print("Query time before ZORDER:", end - start, "seconds")

# took 23.79 ms
# Query time before ZORDER: 1.5445144176483154 seconds

df2 = spark.read.format("delta").load("delta-tables/employees")
df2.repartition(1).sortWithinPartitions("region").write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("delta-tables/employees")

start = time.time()
spark.read.format("delta").load("delta-tables/employees").filter(col("region") == "US").count()
end = time.time()

print("Query time after ZORDER:", end - start, "seconds")

#  took 10.6303 ms
# Query time after ZORDER: 0.550830602645874 seconds