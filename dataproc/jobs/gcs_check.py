"""
Test Access GCS and read Parquet files
"""

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Test access raw bucket") \
    .getOrCreate()

input_path = "gs://uber-datalake"

df = spark.read.option("header", True) \
    .parquet(input_path)

df.show(truncate=False)

spark.stop()
