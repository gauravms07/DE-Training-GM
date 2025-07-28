import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, concat, lit, current_timestamp, to_timestamp, to_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from delta.tables import DeltaTable
from datetime import datetime # Import the datetime module

# 1. Configure Spark Session for Delta Lake
spark = SparkSession.builder.appName("EcommerceDataVaultWithDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Define Local Paths and Prepare Staging Data
# This base_path points to your local filesystem.
base_path = "/delta-vault"
hub_customer_path = f"{base_path}/hub_customer"
hub_product_path = f"{base_path}/hub_product"
hub_order_path = f"{base_path}/hub_order"
link_customer_order_path = f"{base_path}/link_customer_order"
link_order_product_path = f"{base_path}/link_order_product"
sat_customer_path = f"{base_path}/sat_customer_details"
sat_order_path = f"{base_path}/sat_order_details"
sat_link_order_product_path = f"{base_path}/sat_link_order_product_details"

# Define schema for the source data
schema = StructType([
    StructField("Order_ID", StringType(), True),
    StructField("Customer_ID", StringType(), True),
    StructField("Customer_FirstName", StringType(), True),
    StructField("Customer_LastName", StringType(), True),
    StructField("Customer_Email", StringType(), True),
    StructField("Order_Date", TimestampType(), True),
    StructField("Order_Status", StringType(), True),
    StructField("Product_ID", StringType(), True),
    StructField("Product_Name", StringType(), True),
    StructField("Product_Category", StringType(), True),
    StructField("Quantity_Ordered", IntegerType(), True),
    StructField("Price_Per_Unit", DecimalType(10, 2), True),
    StructField("Inventory_ID", StringType(), True)
])

# FIX: Convert string dates to datetime objects during data definition


df = (
    spark.read.csv("source_data/input_data.csv", header=True)
    .withColumn("Order_Date", to_timestamp("Order_Date", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("Quantity_Ordered", col("Quantity_Ordered").cast(IntegerType())) \
    .withColumn("Price_Per_Unit", col("Price_Per_Unit").cast(DecimalType(10,2)))
)

# Create the staging DataFrame
staging_df = spark.createDataFrame(df.rdd, schema)
# staging_df = staging_df \
#              .withColumn("Quantity_Ordered", col("Quantity_Ordered").cast(IntegerType())) \
#              .withColumn("Price_Per_Unit", col("Price_Per_Unit").cast(DecimalType(10,2)))

staging_df.printSchema()
print("Staging DataFrame created successfully with correct timestamps.")

# Define standard DV columns
load_date = current_timestamp()
record_source = lit("ECommercePlatform")

# --- Reusable Loading Functions ---
def merge_insert_only(target_path, source_df, merge_key):
    if not DeltaTable.isDeltaTable(spark, target_path):
        source_df.write.format("delta").mode("overwrite").save(target_path)
        return
    delta_target = DeltaTable.forPath(spark, target_path)
    delta_target.alias("target").merge(
        source_df.alias("source"), f"target.{merge_key} = source.{merge_key}"
    ).whenNotMatchedInsertAll().execute()

def append_new_satellite_data(target_path, source_df, pk_column, hash_diff_col):
    if DeltaTable.isDeltaTable(spark, target_path):
        target_table = spark.read.format("delta").load(target_path)
        new_records_df = source_df.join(
            target_table,
            (source_df[pk_column] == target_table[pk_column]) & (source_df[hash_diff_col] == target_table[hash_diff_col]),
            "left_anti"
        )
    else:
        new_records_df = source_df
    if not new_records_df.rdd.isEmpty():
        new_records_df.write.format("delta").mode("append").save(target_path)

# 3. Load Hubs and Links (no changes needed here)
# Hubs
hub_customer_source = staging_df.select("Customer_ID").distinct().withColumn("Customer_HK", md5(col("Customer_ID"))).withColumn("Load_Date", load_date).withColumn("Record_Source", record_source)
hub_product_source = staging_df.select("Product_ID").distinct().withColumn("Product_HK", md5(col("Product_ID"))).withColumn("Load_Date", load_date).withColumn("Record_Source", record_source)
hub_order_source = staging_df.select("Order_ID").distinct().withColumn("Order_HK", md5(col("Order_ID"))).withColumn("Load_Date", load_date).withColumn("Record_Source", record_source)
merge_insert_only(hub_customer_path, hub_customer_source, "Customer_HK")
merge_insert_only(hub_product_path, hub_product_source, "Product_HK")
merge_insert_only(hub_order_path, hub_order_source, "Order_HK")

# Links
hub_customer_df = spark.read.format("delta").load(hub_customer_path)
hub_order_df = spark.read.format("delta").load(hub_order_path)
hub_product_df = spark.read.format("delta").load(hub_product_path)
link_customer_order_source = staging_df.select("Customer_ID", "Order_ID").distinct().join(hub_customer_df, "Customer_ID").join(hub_order_df, "Order_ID").withColumn("Link_Customer_Order_HK", md5(concat(col("Customer_HK"), col("Order_HK")))).select("Link_Customer_Order_HK", "Customer_HK", "Order_HK", lit(load_date).alias("Load_Date"), record_source.alias("Record_Source"))
link_order_product_source = staging_df.select("Order_ID", "Product_ID").distinct().join(hub_order_df, "Order_ID").join(hub_product_df, "Product_ID").withColumn("Link_Order_Product_HK", md5(concat(col("Order_HK"), col("Product_HK")))).select("Link_Order_Product_HK", "Order_HK", "Product_HK", lit(load_date).alias("Load_Date"), record_source.alias("Record_Source"))
merge_insert_only(link_customer_order_path, link_customer_order_source, "Link_Customer_Order_HK")
merge_insert_only(link_order_product_path, link_order_product_source, "Link_Order_Product_HK")

# 4. Prepare and Load All Satellites
print("\nPreparing and loading satellite data...")

# Sat_Customer_Details
sat_customer_source = staging_df.select("Customer_ID", "Customer_FirstName", "Customer_LastName", "Customer_Email").distinct() \
    .withColumn("Hash_Diff", md5(concat(col("Customer_FirstName"), col("Customer_LastName"), col("Customer_Email")))) \
    .join(hub_customer_df, "Customer_ID") \
    .select("Customer_HK", "Hash_Diff", "Customer_FirstName", "Customer_LastName", "Customer_Email", lit(load_date).alias("Load_Date"), record_source.alias("Record_Source"))
append_new_satellite_data(sat_customer_path, sat_customer_source, "Customer_HK", "Hash_Diff")

# Sat_Order_Details
sat_order_source = staging_df.select("Order_ID", "Order_Date", "Order_Status").distinct() \
    .withColumn("Hash_Diff", md5(concat(col("Order_Date").cast("string"), col("Order_Status")))) \
    .join(hub_order_df, "Order_ID") \
    .select("Order_HK", "Hash_Diff", "Order_Date", "Order_Status", lit(load_date).alias("Load_Date"), record_source.alias("Record_Source"))
append_new_satellite_data(sat_order_path, sat_order_source, "Order_HK", "Hash_Diff")

# Sat_Link_Order_Product_Details
link_order_product_df = spark.read.format("delta").load(link_order_product_path)
sat_link_order_product_source = staging_df \
    .join(hub_order_df, "Order_ID").join(hub_product_df, "Product_ID") \
    .withColumn("Link_Order_Product_HK", md5(concat(col("Order_HK"), col("Product_HK")))) \
    .withColumn("Hash_Diff", md5(concat(col("Quantity_Ordered").cast("string"), col("Price_Per_Unit").cast("string")))) \
    .join(link_order_product_df, "Link_Order_Product_HK") \
    .select( "Link_Order_Product_HK", "Hash_Diff", "Quantity_Ordered", "Price_Per_Unit", lit(load_date).alias("Load_Date"), record_source.alias("Record_Source"))
append_new_satellite_data(sat_link_order_product_path, sat_link_order_product_source, "Link_Order_Product_HK", "Hash_Diff")

print("All data loaded into local Delta tables.")

# 5. Verification: Print All Satellite DataFrames
print("\n--- Verification of Satellite Data ---")

print("\n Sat_Customer_Details:")
spark.read.format("delta").load(sat_customer_path).show(truncate=False)

print("\n Sat_Order_Details:")
spark.read.format("delta").load(sat_order_path).show(truncate=False)

print("\n Sat_Link_Order_Product_Details:")
spark.read.format("delta").load(sat_link_order_product_path).show(truncate=False)