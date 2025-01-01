# Import
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("Dropoff Location Analyzer") \
    .getOrCreate()

# Path lists
fact_trip = "hdfs://10.128.0.59:8020/data_warehouse/fact_trip"
dim_datetime = "hdfs://10.128.0.59:8020/data_warehouse/dim_datetime"
dim_dropoff_location = "hdfs://10.128.0.59:8020/data_warehouse/dim_dropoff_location"

# uber-analysis-439804.query_result. + the table's name
output_dropoff = "uber-analysis-439804.query_result.trips_per_do_location"

# Read data into dataframe
df_fact = spark.read \
    .format("parquet") \
    .option("path", fact_trip) \
    .load() \
    .select("trip_id", "datetimestamp_id", "do_location_id")

df_datetime = spark.read \
    .format("parquet") \
    .option("path", dim_datetime) \
    .load() \
    .select("pick_year", "datetime_id")

df_dropoff_location = spark.read \
    .format("parquet") \
    .option("path", dim_dropoff_location) \
    .load()

df_fact.printSchema()

# --------------------------------- Drop Off Location ---------------------------------
# Joining
df_do_joined = df_fact.alias("fact_data") \
    .join(df_datetime.alias("dim_datetime"),
          col("fact_data.datetimestamp_id") == col("dim_datetime.datetime_id"), "inner") \
    .join(df_dropoff_location.alias("dim_dropoff_location"),
          col("fact_data.do_location_id") == col("dim_dropoff_location.DOLocationID"), "inner") \
    .select(
        col("fact_data.trip_id").alias("trip_id"),
        col("dim_datetime.pick_year").alias("year"),
        col("dim_dropoff_location.DOLocationID").alias("LocationID"),
        col("dim_dropoff_location.X").alias("dropoff_x"),
        col("dim_dropoff_location.Y").alias("dropoff_y"),
        col("dim_dropoff_location.zone").alias("zone"),
        col("dim_dropoff_location.borough").alias("borough"),
        col("dim_dropoff_location.service_zone").alias("service_zone")
)

# Aggregation
df_do_result = df_do_joined \
    .groupBy("year", "LocationID", "dropoff_x", "dropoff_y", "zone", "borough", "service_zone") \
    .agg(count("trip_id").alias("total_trips"))

# Save to BigQuery
df_do_result.write \
    .format("bigquery") \
    .option("table", output_dropoff) \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

df_do_result.unpersist()
df_do_joined.unpersist()

spark.stop()
