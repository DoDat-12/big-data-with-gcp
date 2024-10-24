from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Test") \
    .getOrCreate()

input_path = "gs://uber-parquet/yellow_tripdata_2024-01.parquet"

df = spark.read.option("header", True) \
    .parquet(input_path)

df.show(truncate=False)

spark.stop()
