# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("Aggregation Full Data") \
    .getOrCreate()

# Path lists
zone_lookup = "hdfs://10.128.0.59:8020/raw_data/updated_zone_lookup.csv"
fact_trip = "hdfs://10.128.0.59:8020/data_warehouse/fact_trip"
dim_vendor = "hdfs://10.128.0.59:8020/data_warehouse/dim_vendor"
dim_datetime = "hdfs://10.128.0.59:8020/data_warehouse/dim_datetime"
dim_rate_code = "hdfs://10.128.0.59:8020/data_warehouse/dim_rate_code"
dim_pickup_location = "hdfs://10.128.0.59:8020/data_warehouse/dim_pickup_location"
dim_dropoff_location = "hdfs://10.128.0.59:8020/data_warehouse/dim_dropoff_location"
dim_payment = "hdfs://10.128.0.59:8020/data_warehouse/dim_payment"

# uber-analysis-439804.query_result. + the table's name
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

spark.stop()
