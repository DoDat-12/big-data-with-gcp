# Import
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("Pickup Location Analyzer") \
    .getOrCreate()

# Path lists
fact_trip = "hdfs://10.128.0.59:8020/data_warehouse/fact_trip"
dim_datetime = "hdfs://10.128.0.59:8020/data_warehouse/dim_datetime"
dim_pickup_location = "hdfs://10.128.0.59:8020/data_warehouse/dim_pickup_location"

# uber-analysis-439804.query_result. + the table's name
output_pickup = "uber-analysis-439804.query_result.trips_per_pu_location"

# Read data into dataframe
df_fact = spark.read \
    .format("parquet") \
    .option("path", fact_trip) \
    .load() \
    .select("trip_id", "datetimestamp_id", "pu_location_id")

df_datetime = spark.read \
    .format("parquet") \
    .option("path", dim_datetime) \
    .load() \
    .select("pick_year", "datetime_id")

df_pickup_location = spark.read \
    .format("parquet") \
    .option("path", dim_pickup_location) \
    .load()

df_fact.printSchema()

# --------------------------------- Pick Up Location ---------------------------------
# Joining
df_pu_joined = df_fact.alias("fact_data") \
    .join(df_datetime.alias("dim_datetime"),
          col("fact_data.datetimestamp_id") == col("dim_datetime.datetime_id"), "inner") \
    .join(df_pickup_location.alias("dim_pickup_location"),
          col("fact_data.pu_location_id") == col("dim_pickup_location.PULocationID"), "inner") \
    .select(
        col("fact_data.trip_id").alias("trip_id"),
        col("dim_datetime.pick_year").alias("year"),
        col("dim_pickup_location.PULocationID").alias("LocationID"),
        col("dim_pickup_location.X").alias("pickup_x"),
        col("dim_pickup_location.Y").alias("pickup_y"),
        col("dim_pickup_location.zone").alias("zone"),
        col("dim_pickup_location.borough").alias("borough"),
        col("dim_pickup_location.service_zone").alias("service_zone")
)

# Aggregation
df_pu_result = df_pu_joined \
    .groupBy("year", "LocationID", "pickup_x", "pickup_y", "zone", "borough", "service_zone") \
    .agg(count("trip_id").alias("total_trips"))

# Save to BigQuery
df_pu_result.write \
    .format("bigquery") \
    .option("table", output_pickup) \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

df_pu_result.unpersist()
df_pu_joined.unpersist()

spark.stop()
