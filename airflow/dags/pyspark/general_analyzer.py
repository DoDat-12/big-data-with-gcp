# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("General Analyzer") \
    .getOrCreate()

# Path lists
fact_trip = "hdfs://10.128.0.59:8020/data_warehouse/fact_trip"
output = "uber-analysis-439804.query_result.agg_total"

# Read data into dataframe
df_fact = spark.read \
    .format("parquet") \
    .option("path", fact_trip) \
    .load()

# Query
df_result = df_fact \
    .agg(
        count("trip_id").alias("total_trips"),
        sum("passenger_count").alias("total_passengers"),
        sum("trip_distance").alias("total_distance"),
        sum("total_amount").alias("total_amount"),
        avg("trip_distance").alias("average_distance_per_trip"),
        avg("total_amount").alias("average_amount_per_trip")) \
    .select(
        "total_trips",
        "total_passengers",
        "total_distance",
        "total_amount",
        "average_distance_per_trip",
        "average_amount_per_trip"
    )

# Save to BigQuery
df_result.write \
    .format("bigquery") \
    .option("table", output) \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

df_result.unpersist()
df_fact.unpersist()

# Stop Spark Session
spark.stop()
