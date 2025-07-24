import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType


spark = SparkSession.builder.appName("SCD2_DimCustomer").getOrCreate()

names = ["Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Helen", "Ian", "Jane"]
cities = ["New York", "San Francisco", "Los Angeles", "Chicago", "Houston", "Phoenix", "Seattle", "Boston"]
tiers = ["Silver", "Gold", "Platinum"]

source_data = [
    (i, random.choice(names), random.choice(cities), random.choice(tiers))
    for i in range(1, 201)
]

source_df = spark.createDataFrame(source_data, ["CustomerID", "Name", "Address", "LoyaltyTier"])
source_df.show()



existing_data = [
    (i*10, i, random.choice(names), random.choice(cities), random.choice(tiers),
     datetime.today().date() - timedelta(days=100), None, True)
    for i in range(1, 101)  # only 100 customers exist in the warehouse
]

existing_schema = StructType([
    StructField("CustomerSK", IntegerType(), False),
    StructField("CustomerID", IntegerType(), False),
    StructField("Name", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("LoyaltyTier", StringType(), True),
    StructField("StartDate", DateType(), True),
    StructField("EndDate", DateType(), True),
    StructField("IsCurrent", BooleanType(), True)
])

# Create DataFrame using schema
existing_df = spark.createDataFrame(existing_data, schema=existing_schema)


joined_df = source_df.alias("src").join(
    existing_df.filter("IsCurrent = true").alias("tgt"),
    on="CustomerID",
    how="left"
)

# Only changed rows
changed_df = joined_df.filter(
    (col("src.Address") != col("tgt.Address")) |
    (col("src.LoyaltyTier") != col("tgt.LoyaltyTier"))
).select("src.*")




from pyspark.sql.functions import current_date, lit

expired_df = joined_df.filter(
    (col("src.Address") != col("tgt.Address")) |
    (col("src.LoyaltyTier") != col("tgt.LoyaltyTier"))
).select(
    col("tgt.CustomerSK"),
    col("tgt.CustomerID"),
    col("tgt.Name"),
    col("tgt.Address"),
    col("tgt.LoyaltyTier"),
    col("tgt.StartDate"),
    current_date().alias("EndDate"),
    lit(False).alias("IsCurrent")
)



from pyspark.sql.functions import monotonically_increasing_id

new_version_df = changed_df \
    .withColumn("CustomerSK", monotonically_increasing_id()) \
    .withColumn("StartDate", current_date()) \
    .withColumn("EndDate", lit(None).cast("date")) \
    .withColumn("IsCurrent", lit(True))



final_df = existing_df.filter("IsCurrent = true") \
    .join(expired_df, "CustomerSK", "left_anti") \
    .unionByName(expired_df) \
    .unionByName(new_version_df)

# final_df.write.format("delta").mode("overwrite").save("path/to/dim_customer")
print("Final DataFrame:")
final_df.show()



