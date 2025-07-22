from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as SUMM, avg as AVGG

spark = SparkSession.builder.appName("Lab 2 App").master("local[*]").getOrCreate()

users_df = spark.read.csv("users.csv", header=True, inferSchema=True)
activity_df = spark.read.csv("activity.csv", header=True, inferSchema=True)

filtered_df = users_df.filter("age > 30").join(activity_df, users_df.id == activity_df.user_id)

filtered_df.show()

df1 = activity_df.groupBy("user_id").agg(
    SUMM("duration_min").alias("Total Duration"), AVGG("duration_min").alias("Average Duration")
    )

df2 = df1.join(users_df, users_df.id == df1.user_id)

df2.show()

df2.write.csv("user_activity.csv")

input()

spark.stop()
