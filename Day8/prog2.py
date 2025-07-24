from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import time

spark = SparkSession.builder.appName("BroadcastHintExample").getOrCreate()

orders = [(1, "Laptop"), (2, "Phone"), (3, "Tablet"), (4, "Monitor")]
orders_df = spark.createDataFrame(orders, ["order_id", "product"])

customers = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David")]
customers_df = spark.createDataFrame(customers, ["order_id", "customer_name"])

start_time_b = time.time()
joined_df = orders_df.join(broadcast(customers_df), on="order_id")
end_time_b = time.time()
time_b = end_time_b - start_time_b

start_time = time.time()
dataf = orders_df.join(customers_df, on="order_id")
end_time = time.time()
time_nb = end_time - start_time

print(f"Using Broadcast: {time_b: .4f}")
print(f"Using Normal: {time_nb: .4}")

joined_df.show()




