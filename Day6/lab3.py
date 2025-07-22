from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as SUMM, avg
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\gaurav.shastry\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\gaurav.shastry\AppData\Local\Programs\Python\Python310\python.exe"

spark = SparkSession.builder \
    .appName("Lab 3: Cache vs Checkpoint") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir("checkpoints")

df = spark.read.option("header", True).option("inferSchema", True).csv("user_activity.csv")

region_df = spark.createDataFrame([
    ("North", "Zone A"),
    ("South", "Zone B"),
    ("East", "Zone C"),
    ("West", "Zone D"),
    ("Central", "Zone E")
], ["region", "zone"])

long_pipeline = df.join(region_df, "region") \
    .filter(col("Total Duration") > 100) \
    .groupBy("zone").count() \
    .withColumnRenamed("count", "user_count")

cached_df = long_pipeline.cache()

print("\n=== Cached Pipeline Output ===")
cached_df.orderBy("user_count", ascending=False).show()

checkpoint_pipeline = df.join(region_df, "region") \
    .filter(col("Total Duration") > 100) \
    .groupBy("zone").count() \
    .withColumnRenamed("count", "user_count")

checkpointed_df = checkpoint_pipeline.checkpoint(eager=True)

print("\n=== Checkpointed Pipeline Output ===")
checkpointed_df.orderBy("user_count", ascending=False).show()

input("Check Spark UI to compare DAGs, then press Enter to exit...")

spark.stop()
