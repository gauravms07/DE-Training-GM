from pyspark.sql import SparkSession
import os
from delta.tables import DeltaTable

os.environ["PYSPARK_PYTHON"] = r"C:\Users\gaurav.shastry\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\gaurav.shastry\AppData\Local\Programs\Python\Python310\python.exe"

spark = SparkSession.builder.appName("TrialApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

df1 = spark.read.csv("sample.csv", header=True, inferSchema=True)

delta_path = "outputs2/path/delta_table"
df1.write.format("delta").mode("overwrite").save(delta_path)

print("Delta Table Content:")
spark.read.format("delta").load(delta_path).show()

DeltaTable.forPath(spark, delta_path).vacuum(0.0)

log_path = os.path.join(delta_path, "_delta_log")
print("Files inside _delta_log:")
print(os.listdir(log_path))