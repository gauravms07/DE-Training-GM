from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DataVault_StarSchema").getOrCreate()

# Load or create DataFrames (replace with your data source)
# Example: Loading from CSVs (or from a database table)
df_hub_customer = spark.read.csv("hubs/Hub_Customer.csv", header=True, inferSchema=True)
df_pit_customer = spark.read.csv("pit_tables/PIT_Customer.csv", header=True, inferSchema=True)
df_hub_product = spark.read.csv("hubs/Hub_Product.csv", header=True, inferSchema=True)
df_pit_product = spark.read.csv("pit_tables/PIT_Product.csv", header=True, inferSchema=True)
df_link_purchase = spark.read.csv("links/Link_Purchase.csv", header=True, inferSchema=True)
df_sat_purchase = spark.read.csv("satellites/Sat_Purchase_Details.csv", header=True, inferSchema=True)

# === Dim_Customer ===
dim_customer = df_hub_customer.alias("hc").join(
    df_pit_customer.alias("pc"),
    col("hc.HK_Customer_ID") == col("pc.HK_Customer_ID"),
    "inner"
).select(
    col("hc.HK_Customer_ID"),
    col("hc.Customer_ID"),
    col("pc.Customer_Name"),
    col("pc.Address"),
    col("pc.Contact_Number"),
    col("pc.Load_Date_x").alias("Effective_Date")
)

# === Dim_Product ===
dim_product = df_hub_product.alias("hp").join(
    df_pit_product.alias("pp"),
    col("hp.HK_Product_SKU") == col("pp.HK_Product_SKU"),
    "inner"
).select(
    col("hp.HK_Product_SKU"),
    col("hp.Product_SKU"),
    col("pp.Product_Name"),
    col("pp.Category"),
    col("pp.Price"),
    col("pp.Load_Date_x").alias("Effective_Date")
)

# === Fact_Purchase ===
fact_purchase = df_link_purchase.alias("lp") \
    .join(df_sat_purchase.alias("sp"), col("lp.HK_Link_Purchase") == col("sp.HK_Link_Purchase"), "inner") \
    .join(dim_customer.alias("dc"), col("lp.HK_Customer_ID") == col("dc.HK_Customer_ID"), "inner") \
    .join(dim_product.alias("dp"), col("lp.HK_Product_SKU") == col("dp.HK_Product_SKU"), "inner") \
    .select(
        col("lp.HK_Link_Purchase"),
        col("lp.HK_Customer_ID"),
        col("lp.HK_Product_SKU"),
        col("dc.Customer_Name"),
        col("dp.Product_Name"),
        col("sp.Purchase_Date"),
        col("sp.Quantity"),
        col("sp.Sales_Amount"),
        col("lp.Load_Date").alias("Transaction_Load_Date")
    )

# === Register as temp views or save as tables (optional) ===
dim_customer.createOrReplaceTempView("Dim_Customer")
dim_product.createOrReplaceTempView("Dim_Product")
fact_purchase.createOrReplaceTempView("Fact_Purchase")

print("Star schema views (DataFrames) are ready in Spark session.")


# Save Dim_Customer as CSV
dim_customer.coalesce(1).write.mode("overwrite").option("header", True).csv("output/Dim_Customer")

# Save Dim_Product as CSV
dim_product.coalesce(1).write.mode("overwrite").option("header", True).csv("output/Dim_Product")

# Save Fact_Purchase as CSV
fact_purchase.coalesce(1).write.mode("overwrite").option("header", True).csv("output/Fact_Purchase")

print("Star schema CSV files written to 'output/' directory.")
