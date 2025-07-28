import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, concat, lit, current_timestamp, max as spark_max, when, lag, row_number, coalesce, to_timestamp, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, BooleanType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

spark = SparkSession.builder.appName("BusinessVault") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

base_path = "/delta-vault"
business_vault_path = f"{base_path}/business_vault"

hub_customer_path = f"{base_path}/hub_customer"
hub_product_path = f"{base_path}/hub_product"
hub_order_path = f"{base_path}/hub_order"
link_customer_order_path = f"{base_path}/link_customer_order"
link_order_product_path = f"{base_path}/link_order_product"
sat_customer_path = f"{base_path}/sat_customer_details"
sat_order_path = f"{base_path}/sat_order_details"
sat_link_order_product_path = f"{base_path}/sat_link_order_product_details"

bv_dim_customer_path = f"{business_vault_path}/dim_customer_scd2"
bv_dim_product_path = f"{business_vault_path}/dim_product_scd2"
bv_dim_order_path = f"{business_vault_path}/dim_order_scd2"
bv_fact_order_line_path = f"{business_vault_path}/fact_order_line"

print("Creating Business Vault with SCD Type 2 handling...")


def merge_scd2_dimension(target_path, source_df, business_key_col, hash_diff_col, dimension_name):
    print(f"Processing {dimension_name} with SCD Type 2...")
    
    if not DeltaTable.isDeltaTable(spark, target_path):
        initial_data = source_df.withColumn("effective_date", col("Load_Date")) \
                               .withColumn("expiry_date", lit(None).cast(TimestampType())) \
                               .withColumn("is_current", lit(True)) \
                               .withColumn("version", lit(1))
        
        initial_data.write.format("delta").mode("overwrite").save(target_path)
        print(f"Initial load completed for {dimension_name}")
        return
    
    existing_df = spark.read.format("delta").load(target_path)
    
    current_records = existing_df.filter(col("is_current") == True)
    
    new_or_changed = source_df.join(
        current_records,
        business_key_col,
        "left"
    ).filter(
        (current_records[hash_diff_col].isNull()) |  
        (source_df[hash_diff_col] != current_records[hash_diff_col])  
    )
    
    if new_or_changed.rdd.isEmpty():
        print(f"No changes detected for {dimension_name}")
        return
    
    keys_to_update = new_or_changed.select(business_key_col).distinct()
    
    delta_table = DeltaTable.forPath(spark, target_path)
    delta_table.alias("target").merge(
        keys_to_update.alias("updates"),
        f"target.{business_key_col} = updates.{business_key_col} AND target.is_current = true"
    ).whenMatchedUpdate(set={
        "expiry_date": current_timestamp(),
        "is_current": lit(False)
    }).execute()
    
    max_versions = existing_df.groupBy(business_key_col).agg(
        spark_max("version").alias("max_version")
    )
    
    new_records = new_or_changed.join(max_versions, business_key_col, "left") \
                               .withColumn("effective_date", col("Load_Date")) \
                               .withColumn("expiry_date", lit(None).cast(TimestampType())) \
                               .withColumn("is_current", lit(True)) \
                               .withColumn("version", coalesce(col("max_version") + 1, lit(1))) \
                               .drop("max_version")
    
    new_records.write.format("delta").mode("append").save(target_path)
    print(f"SCD Type 2 processing completed for {dimension_name}")



def create_customer_dimension():
    print("Creating Customer Dimension with SCD Type 2...")
    print("Using: Customer_ID, Customer_FirstName, Customer_LastName, Customer_Email")
    
    hub_customer = spark.read.format("delta").load(hub_customer_path)
    sat_customer = spark.read.format("delta").load(sat_customer_path)
    
    customer_complete = hub_customer.join(sat_customer.alias("x"), "Customer_HK") \
                                  .select("Customer_HK", "Customer_ID", "Hash_Diff", 
                                        "Customer_FirstName", "Customer_LastName", 
                                        "Customer_Email", "x.Load_Date", "x.Record_Source")
    
    customer_business = customer_complete.withColumn("full_name", 
                                                   concat(col("Customer_FirstName"), lit(" "), col("Customer_LastName")))
    
    merge_scd2_dimension(bv_dim_customer_path, customer_business, "Customer_ID", "Hash_Diff", "Customer")

def create_product_dimension():
    print("Creating Product Dimension...")
    print("Using: Product_ID, Product_Name, Product_Category")
    
    source_df = (
        spark.read.csv("source_data/input_data.csv", header=True)
        .withColumn("Order_Date", to_timestamp("Order_Date", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("Quantity_Ordered", col("Quantity_Ordered").cast(IntegerType()))
        .withColumn("Price_Per_Unit", col("Price_Per_Unit").cast(DecimalType(10,2)))
    )
    
    hub_product = spark.read.format("delta").load(hub_product_path)
    
    product_details = source_df.select("Product_ID", "Product_Name", "Product_Category").distinct() \
                              .join(hub_product, "Product_ID") \
                              .withColumn("Hash_Diff", md5(concat(col("Product_Name"), col("Product_Category")))) \
                              .withColumn("Load_Date", current_timestamp()) \
                              .withColumn("Record_Source", lit("ECommercePlatform"))
    
    product_scd2 = product_details.withColumn("effective_date", col("Load_Date")) \
                                  .withColumn("expiry_date", lit(None).cast(TimestampType())) \
                                  .withColumn("is_current", lit(True)) \
                                  .withColumn("version", lit(1))
    
    product_scd2.write.format("delta").mode("overwrite").save(bv_dim_product_path)
    print("Product Dimension created")

def create_order_dimension():
    print("Creating Order Dimension with SCD Type 2...")
    print("Using: Order_ID, Order_Date, Order_Status")
    
    hub_order = spark.read.format("delta").load(hub_order_path)
    sat_order = spark.read.format("delta").load(sat_order_path)
    
    order_complete = hub_order.join(sat_order.alias("x"), "Order_HK") \
                             .select("Order_HK", "Order_ID", "Hash_Diff", 
                                   "Order_Date", "Order_Status", "x.Load_Date", "x.Record_Source")
    
    order_business = order_complete.withColumn("order_year", year(col("Order_Date"))) \
                                  .withColumn("order_month", month(col("Order_Date"))) \
                                  .withColumn("order_day", dayofmonth(col("Order_Date"))) \
                                  .withColumn("order_hour", hour(col("Order_Date")))
    
    merge_scd2_dimension(bv_dim_order_path, order_business, "Order_ID", "Hash_Diff", "Order")

try:
    create_customer_dimension()
    create_product_dimension() 
    create_order_dimension()
    print("All Business Vault tables created successfully.")
except Exception as e:
    print(f"Error creating Business Vault: {str(e)}")
    raise


