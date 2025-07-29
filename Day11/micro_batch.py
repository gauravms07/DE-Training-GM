from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp 
import time
from datetime import datetime

def print_with_timestamp(message):
    """Helper function to print messages with current timestamp"""
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')}] {message}")

def main():
    
    print_with_timestamp("Starting Spark session")
    spark = SparkSession.builder \
        .appName("MicroBatchStream") \
        .master("local[*]") \
        .getOrCreate()

    
    print_with_timestamp("Creating rate source stream")
    df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 10) \
        .load()
    
    
    df = df.withColumn("processingTime", current_timestamp())

    
    print_with_timestamp("=== Starting Micro-Batch Mode ===")
    print_with_timestamp("Expected behavior: Batches every ~1 second (default trigger interval)")
    
    query = df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("append") \
        .start()
    
    
    for i in range(1, 11):
        time.sleep(1)
        print_with_timestamp(f"Waiting... {i} seconds elapsed")
    
    print_with_timestamp("Stopping the query")
    query.stop()
    
    print_with_timestamp("Spark session stopped")
    spark.stop()

if __name__ == "__main__":
    main()