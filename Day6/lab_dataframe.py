from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test DataFrame").master("local[*]").getOrCreate()

countLines = spark.read.text("SPARKspark.txt").count()
print(countLines)

df = spark.read.csv("data.csv", header=True, inferSchema=True)
finalData = df.filter("salary > 80000")
print(finalData.show())