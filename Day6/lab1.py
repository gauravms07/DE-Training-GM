from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as SUMM

spark = SparkSession.builder.appName("Lab1").master("local[*]").getOrCreate()

df1 = spark.read.csv("products.csv", header=True, inferSchema=True)
df2 = spark.read.csv("orders.csv", header=True, inferSchema=True)

joined_df = df2.join(df1, df2.product_id == df1.id)

revenue_df = joined_df.withColumn("revenue", col("price") * col("quantity"))

category_revenue = revenue_df.groupBy("category").agg(SUMM("revenue").alias("total_revenue"))

result_df = category_revenue.orderBy(col("total_revenue").desc())

buffer = input("Buffer to load Spark UI")

result_df.show()