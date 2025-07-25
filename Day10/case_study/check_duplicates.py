from pyspark.sql.functions import col, count
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Check_Duplicates").getOrCreate()

def check_duplicates_spark(df, hash_col, entity_name):
    dup_df = df.groupBy(hash_col).count().filter(col("count") > 1)
    count_dups = dup_df.count()
    if count_dups > 0:
        print(f"OOPS - {count_dups} duplicate hash keys found in {entity_name} column {hash_col}")
        dup_df.show(truncate=False)
    else:
        print(f"YAY - No duplicates found in {entity_name} hash key column {hash_col}.")

df_hub_customer = spark.read.csv("hubs/Hub_Customer.csv", header=True, inferSchema=True)
df_hub_product = spark.read.csv("hubs/Hub_Product.csv", header=True, inferSchema=True)
df_link_purchase = spark.read.csv("links/Link_Purchase.csv", header=True, inferSchema=True)

check_duplicates_spark(df_hub_customer, "HK_Customer_ID", "Hub_Customer")
check_duplicates_spark(df_hub_product, "HK_Product_SKU", "Hub_Product")
check_duplicates_spark(df_link_purchase, "HK_Link_Purchase", "Link_Purchase")
