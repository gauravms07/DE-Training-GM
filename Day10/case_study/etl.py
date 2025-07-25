import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import pandas as pd
import hashlib
from datetime import date

# === Helper Functions ===
def hash_key(*args):
    concat = ''.join(str(arg).lower().strip() for arg in args)
    return hashlib.sha256(concat.encode('utf-8')).hexdigest()

def hash_diff(*args):
    concat = '|'.join(str(arg).lower().strip() for arg in args)
    return hashlib.sha256(concat.encode('utf-8')).hexdigest()

# === Metadata ===
load_date = str(date.today())
record_source = "Retail_Source"
batch_id = "BATCH001"

# === Load Source Data ===
df_customers = pd.read_csv("source_data/customers.csv")
df_products = pd.read_csv("source_data/products.csv")

# === Hub_Customer ===
df_hub_customer = df_customers.copy()
df_hub_customer["HK_Customer_ID"] = df_hub_customer["Customer_ID"].apply(lambda x: hash_key(x))
df_hub_customer["Load_Date"] = load_date
df_hub_customer["Record_Source"] = record_source
df_hub_customer["Batch_ID"] = batch_id
df_hub_customer = df_hub_customer[["HK_Customer_ID", "Customer_ID", "Load_Date", "Record_Source", "Batch_ID"]]

# === Sat_Customer_Details ===
df_sat_customer = df_customers.copy()
df_sat_customer["HK_Customer_ID"] = df_sat_customer["Customer_ID"].apply(lambda x: hash_key(x))
df_sat_customer["HashDiff"] = df_sat_customer.apply(
    lambda row: hash_diff(row["Customer_Name"], row["Address"], str(row["Contact_Number"])),
    axis=1
)
df_sat_customer["Load_Date"] = load_date
df_sat_customer["Record_Source"] = record_source
df_sat_customer["Batch_ID"] = batch_id
df_sat_customer = df_sat_customer[[
    "HK_Customer_ID", "Customer_Name", "Address", "Contact_Number",
    "HashDiff", "Load_Date", "Record_Source", "Batch_ID"
]]

# === Hub_Product ===
df_hub_product = df_products.copy()
df_hub_product["HK_Product_SKU"] = df_hub_product["Product_SKU"].apply(lambda x: hash_key(x))
df_hub_product["Load_Date"] = load_date
df_hub_product["Record_Source"] = record_source
df_hub_product["Batch_ID"] = batch_id
df_hub_product = df_hub_product[["HK_Product_SKU", "Product_SKU", "Load_Date", "Record_Source", "Batch_ID"]]

# === Sat_Product_Details ===
df_sat_product = df_products.copy()
df_sat_product["HK_Product_SKU"] = df_sat_product["Product_SKU"].apply(lambda x: hash_key(x))
df_sat_product["HashDiff"] = df_sat_product.apply(
    lambda row: hash_diff(row["Product_Name"], row["Category"], str(row["Price"])),
    axis=1
)
df_sat_product["Load_Date"] = load_date
df_sat_product["Record_Source"] = record_source
df_sat_product["Batch_ID"] = batch_id
df_sat_product = df_sat_product[[
    "HK_Product_SKU", "Product_Name", "Category", "Price",
    "HashDiff", "Load_Date", "Record_Source", "Batch_ID"
]]

# === Load Purchases ===
df_purchases = pd.read_csv("source_data/purchases.csv")

# Join to get hashed foreign keys
df_link = df_purchases.merge(
    df_hub_customer[["Customer_ID", "HK_Customer_ID"]],
    on="Customer_ID",
    how="left"
).merge(
    df_hub_product[["Product_SKU", "HK_Product_SKU"]],
    on="Product_SKU",
    how="left"
)

# Generate Link Hash Key (customer + product + date for uniqueness)
df_link["HK_Link_Purchase"] = df_link.apply(
    lambda row: hash_key(row["HK_Customer_ID"], row["HK_Product_SKU"], row["Purchase_Date"]),
    axis=1
)

# Add audit columns
df_link["Load_Date"] = load_date
df_link["Record_Source"] = record_source
df_link["Batch_ID"] = batch_id

# Select Link table columns
df_link_purchase = df_link[[
    "HK_Link_Purchase", "HK_Customer_ID", "HK_Product_SKU",
    "Load_Date", "Record_Source", "Batch_ID"
]]

# === Satellite for Purchase Details ===
df_sat_purchase = df_link.copy()
df_sat_purchase["HashDiff"] = df_sat_purchase.apply(
    lambda row: hash_diff(str(row["Purchase_Date"]), str(row["Quantity"]), str(row["Sales_Amount"])),
    axis=1
)
df_sat_purchase = df_sat_purchase[[
    "HK_Link_Purchase", "Purchase_Date", "Quantity", "Sales_Amount",
    "HashDiff", "Load_Date", "Record_Source", "Batch_ID"
]]

# === Save to CSVs ===
df_link_purchase.to_csv("links/Link_Purchase.csv", index=False)
df_sat_purchase.to_csv("satellites/Sat_Purchase_Details.csv", index=False)

print("Links and Purchase satellite prepared successfully.")


# === Optional: Save to CSV for inspection ===
df_hub_customer.to_csv("hubs/Hub_Customer.csv", index=False)
df_sat_customer.to_csv("satellites/Sat_Customer_Details.csv", index=False)
df_hub_product.to_csv("hubs/Hub_Product.csv", index=False)
df_sat_product.to_csv("satellites/Sat_Product_Details.csv", index=False)

print("Hubs and Satellites prepared successfully.")
