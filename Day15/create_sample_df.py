from pyspark.sql import SparkSession
from great_expectations.dataset import SparkDFDataset
from pyspark.sql.types import *
import datetime
from great_expectations.core.batch import RuntimeBatchRequest


# Initialize Spark session
spark = SparkSession.builder.appName("GE Testing").getOrCreate()

# Sample data
data = [
    (1, 100.0, datetime.datetime.now(), "Amazon", "New York", "purchase", True),
    (2, 49.99, datetime.datetime.now(), "Walmart", "Chicago", "purchase", False),
    (3, 999.00, datetime.datetime.now(), "BestBuy", "Los Angeles", "refund", True),
    (4, 79.50, datetime.datetime.now(), "Target", "Miami", "purchase", False),
    (5, 250.00, datetime.datetime.now(), "Amazon", "Seattle", "transfer", True)
]

# Schema
schema = StructType([
    StructField("transactionId", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("merchant", StringType(), True),
    StructField("location", StringType(), True),
    StructField("transactionType", StringType(), True),
    StructField("isOnline", BooleanType(), True)
])

# Create DataFrame
sample_df = spark.createDataFrame(data, schema)

# Convert to GE dataset
ge_df = SparkDFDataset(sample_df)


# Create test data directly in GE
batch_request = RuntimeBatchRequest(
    datasource_name="my_spark_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="test_data",
    runtime_parameters={
        "batch_data": spark.createDataFrame([{"test_col": 1}, {"test_col": 2}])
    },
    batch_identifiers={"default_identifier_name": "default_identifier"},
)

ge_df.expect_column_values_to_be_between("amount", 1, 1000)
ge_df.expect_column_values_to_be_in_set("merchant", ["Amazon", "Walmart", "BestBuy", "Target"])

validation_result = ge_df.validate()
print(validation_result)

# Save expectations if needed
ge_df.save_expectation_suite("great_expectations/expectations/sample_expectations.json")