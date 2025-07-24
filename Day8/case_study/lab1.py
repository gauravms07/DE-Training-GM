from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab1APP") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

data = [ 

    (1, 'Alice', 'UK'), 

    (2, 'Bob', 'Germany'), 

    (3, 'Carlos', 'Spain') 

] 

# df = spark.createDataFrame(data, ['user_id', 'name', 'country']) 

# df.write.parquet('/tmp/customer_parquet') 

# df.show()

# Load existing Parquet data 
# parquet_df = spark.read.parquet('/tmp/customer_parquet') 
 
# # Write it as a Delta table 
# parquet_df.write.format("delta").save("/tmp/customer_delta") 

# Register it as a managed table (optional) 
spark.sql("CREATE TABLE customer_delta USING DELTA LOCATION '/tmp/customer_delta'") 

result1_df = spark.sql("SELECT * FROM delta.`/tmp/customer_delta`")
result1_df.show()

result2_df = spark.sql("DELETE FROM delta.`/tmp/customer_delta` WHERE user_id = 2")
result2_df.show()

# result3_df = spark.sql("SELECT * FROM delta.`/tmp/customer_delta`")
# result3_df.show()

# result4_df = spark.sql("DESCRIBE HISTORY delta.`/tmp/customer_delta`")
# result4_df.show()

# result5_df = spark.sql("SELECT * FROM delta.`/tmp/customer_delta` VERSION AS OF 0")
# result5_df.show()

# result6_df = spark.sql("RESTORE TABLE customer_delta TO VERSION AS OF 0")
# result6_df.show()