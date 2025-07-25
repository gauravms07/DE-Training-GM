import pandas as pd

# === Load satellite and hub/link data ===
df_hub_customer = pd.read_csv("hubs/Hub_Customer.csv")
df_sat_customer = pd.read_csv("satellites/Sat_Customer_Details.csv")
df_hub_product = pd.read_csv("hubs/Hub_Product.csv")
df_sat_product = pd.read_csv("satellites/Sat_Product_Details.csv")
df_link_purchase = pd.read_csv("links/Link_Purchase.csv")
df_sat_purchase = pd.read_csv("satellites/Sat_Purchase_Details.csv")

# === PIT_DATE (can parameterize this) ===
PIT_DATE = "2025-07-10"

# Convert dates for filtering
df_sat_customer["Load_Date"] = pd.to_datetime(df_sat_customer["Load_Date"])
df_sat_product["Load_Date"] = pd.to_datetime(df_sat_product["Load_Date"])

# === PIT_Customer: Latest record for each customer as of PIT_DATE ===
df_pit_customer = df_sat_customer[df_sat_customer["Load_Date"] <= PIT_DATE]
df_pit_customer = df_pit_customer.sort_values(["HK_Customer_ID", "Load_Date"]).drop_duplicates("HK_Customer_ID", keep="last")

df_pit_customer = df_hub_customer.merge(
    df_pit_customer[["HK_Customer_ID", "Customer_Name", "Address", "Contact_Number", "Load_Date"]],
    on="HK_Customer_ID",
    how="left"
)
df_pit_customer.rename(columns={"Load_Date": "PIT_Load_Date"}, inplace=True)

# === PIT_Product: Latest record for each product as of PIT_DATE ===
df_pit_product = df_sat_product[df_sat_product["Load_Date"] <= PIT_DATE]
df_pit_product = df_pit_product.sort_values(["HK_Product_SKU", "Load_Date"]).drop_duplicates("HK_Product_SKU", keep="last")

df_pit_product = df_hub_product.merge(
    df_pit_product[["HK_Product_SKU", "Product_Name", "Category", "Price", "Load_Date"]],
    on="HK_Product_SKU",
    how="left"
)
df_pit_product.rename(columns={"Load_Date": "PIT_Load_Date"}, inplace=True)

# === PIT_Purchase: Join Link_Purchase with PIT_Customer and PIT_Product
df_pit_purchase = df_link_purchase.merge(
    df_pit_customer[["HK_Customer_ID", "Customer_Name", "Address", "Contact_Number"]],
    on="HK_Customer_ID",
    how="left"
).merge(
    df_pit_product[["HK_Product_SKU", "Product_Name", "Category", "Price"]],
    on="HK_Product_SKU",
    how="left"
)

# Optional: join Sat_Purchase_Details for completeness
df_pit_purchase = df_pit_purchase.merge(
    df_sat_purchase[["HK_Link_Purchase", "Purchase_Date", "Quantity", "Sales_Amount"]],
    on="HK_Link_Purchase",
    how="left"
)

# === Save PIT tables ===
df_pit_customer.to_csv("pit_tables/PIT_Customer.csv", index=False)
df_pit_product.to_csv("pit_tables/PIT_Product.csv", index=False)
df_pit_purchase.to_csv("pit_tables/PIT_Purchase.csv", index=False)

print("PIT tables generated successfully.")
