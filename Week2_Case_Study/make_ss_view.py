from pyspark.sql import SparkSession


print("\nCreating Zero-Copy Star Schema Views...")

spark = SparkSession.builder.appName("StarSchema") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

base_path = "/delta-vault"
business_vault_path = f"{base_path}/business_vault"
bv_dim_customer_path = f"{business_vault_path}/dim_customer_scd2"
bv_dim_product_path = f"{business_vault_path}/dim_product_scd2"
bv_dim_order_path = f"{business_vault_path}/dim_order_scd2"
# bv_fact_order_line_path = f"{business_vault_path}/fact_order_line"

spark.read.format("delta").load(bv_dim_customer_path).createOrReplaceTempView("dim_customer")
spark.read.format("delta").load(bv_dim_product_path).createOrReplaceTempView("dim_product") 
spark.read.format("delta").load(bv_dim_order_path).createOrReplaceTempView("dim_order")
# spark.read.format("delta").load(bv_fact_order_line_path).createOrReplaceTempView("fact_order_line")

view_definitions = {}

customer_analysis_sql = """
CREATE OR REPLACE TEMPORARY VIEW v_customer_analysis AS
SELECT 
    c.Customer_ID,
    c.Customer_FirstName,
    c.Customer_LastName,
    c.full_name,
    c.Customer_Email,
    c.effective_date as customer_since,
    c.version as customer_version,
    c.is_current
FROM dim_customer c
WHERE c.is_current = true
"""

spark.sql(customer_analysis_sql)


print("\nVerification - Business Vault Tables:")

print("\nCustomer Dimension Sample (YOUR ACTUAL FIELDS):")
spark.sql("SELECT Customer_ID, Customer_FirstName, Customer_LastName, Customer_Email, is_current, version FROM dim_customer WHERE is_current = true LIMIT 3").show(truncate=False)

print("\nProduct Dimension Sample (YOUR ACTUAL FIELDS):")  
spark.sql("SELECT Product_ID, Product_Name, Product_Category, is_current, version FROM dim_product WHERE is_current = true LIMIT 3").show(truncate=False)

print("\nOrder Dimension Sample (YOUR ACTUAL FIELDS + basic date parts):")
spark.sql("SELECT Order_ID, Order_Date, Order_Status, order_year, order_month, is_current FROM dim_order WHERE is_current = true LIMIT 3").show(truncate=False)

spark.sql(
    """
    SELECT * FROM v_customer_analysis
    """
)