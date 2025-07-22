import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Initialize SparkSession with Delta support
spark = SparkSession.builder \
    .appName("Join CVs with Employee Data") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Path to JSON file and CVs folder
json_path = "sample.json"
cv_dir = "files/cvs/"

# Read JSON file
emp_df = spark.read.option("multiline", "true").json(json_path)
emp_df.show()

# Define a UDF to fetch CV text based on empName and id
def get_cv_text(emp_id, emp_name):
    # Find matching CV file
    for filename in os.listdir(cv_dir):
        if emp_name in filename and str(emp_id) in filename:
            with open(os.path.join(cv_dir, filename), "r", encoding="utf-8") as f:
                return f.read().strip()
    return None

# Register UDF
get_cv_text_udf = udf(get_cv_text, StringType())

# Add new column using UDF
final_df = emp_df.withColumn("cv_text", get_cv_text_udf(col("emp_id"), col("emp_name")))

# Save to Delta format
final_df.write.format("delta").mode("overwrite").save("outputs4/emp_with_cv.delta")

# Optional: Show result
final_df.show(truncate=False)
