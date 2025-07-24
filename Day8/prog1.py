from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import time
import random

spark = SparkSession.builder \
    .appName("PartitioningComparison") \
    .getOrCreate()

regions = ['US', 'EU', 'IN', 'UK', 'CA']
data = [(i, f'user_{i}', random.choice(regions), random.randint(18, 65)) for i in range(1, 501)]
columns = ['id', 'name', 'region', 'age']

df = spark.createDataFrame(data, columns)

base_path = "/tmp/partitioning_comparison"
non_partitioned_path = f"{base_path}/non_partitioned"
partitioned_path = f"{base_path}/partitioned"

import shutil, os
shutil.rmtree(base_path, ignore_errors=True)

df.write.format("parquet").mode("overwrite").save(non_partitioned_path)

start_np = time.time()
non_part_df = spark.read.parquet(non_partitioned_path)
non_part_df.filter(col("region") == "US").count()
end_np = time.time()
non_partitioned_time = end_np - start_np

df.write.format("parquet").mode("overwrite").partitionBy("region").save(partitioned_path)

start_p = time.time()
part_df = spark.read.parquet(partitioned_path)
part_df.filter(col("region") == "US").count()
end_p = time.time()
partitioned_time = end_p - start_p

print(f"\nQuery Time (non-partitioned): {non_partitioned_time:.4f} seconds")
print(f"Query Time (partitioned):     {partitioned_time:.4f} seconds")

