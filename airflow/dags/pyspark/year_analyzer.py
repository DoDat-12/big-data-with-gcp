# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("Year Analyzer") \
    .getOrCreate()

# Path lists
fact_trip = "hdfs://10.128.0.59:8020/data_warehouse/fact_trip"
dim_datetime = "hdfs://10.128.0.59:8020/data_warehouse/dim_datetime"
dim_payment = "hdfs://10.128.0.59:8020/data_warehouse/dim_payment"
dim_pickup_location = "hdfs://10.128.0.59:8020/data_warehouse/dim_pickup_location"
dim_dropoff_location = "hdfs://10.128.0.59:8020/data_warehouse/dim_dropoff_location"

# BigQuery output table
output_agg_total = "uber-analysis-439804.query_result.agg_total"
output_year = "uber-analysis-439804.query_result.agg_per_year"
output_payment_year = "uber-analysis-439804.query_result.payment_per_year"
output_payment = "uber-analysis-439804.query_result.agg_per_payment"
output_pickup_location = "uber-analysis-439804.query_result.trips_per_pu_location"
output_dropoff_location = "uber-analysis-439804.query_result.trips_per_do_location"

# Read data into dataframe
# Fact Table
df_fact = spark.read \
    .format("parquet") \
    .option("path", fact_trip) \
    .load()
# Dim Datetime
df_datetime = spark.read \
    .format("parquet") \
    .option("path", dim_datetime) \
    .load() \
    .select("datetime_id", "pick_year")
# Dim Payment
df_payment = spark.read \
    .format("parquet") \
    .option("path", dim_payment) \
    .load() \
    .filter(~col("payment_type").isin([4, 6]))
# Dim Location
df_pickup_location = spark.read \
    .format("parquet") \
    .option("path", dim_pickup_location) \
    .load()
df_dropoff_location = spark.read \
    .format("parquet") \
    .option("path", dim_dropoff_location) \
    .load()

# ----------------- Aggregation All Year -----------------
df_agg_total = df_fact \
    .agg(
        count("trip_id").alias("total_trips"),
        sum("passenger_count").alias("total_passengers"),
        sum("trip_distance").alias("total_distance"),
        sum("total_amount").alias("total_amount"),
        avg("trip_distance").alias("average_distance_per_trip"),
        avg("total_amount").alias("average_amount_per_trip")) \
    .select(
        col("total_trips"),
        col("total_passengers"),
        col("total_distance"),
        col("total_amount"),
        col("average_distance_per_trip"),
        col("average_amount_per_trip")
    )

# Save to BigQuery
df_agg_total.write \
    .format("bigquery") \
    .option("table", output_agg_total) \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

# Free Space
df_agg_total.unpersist()

# ----------------- Aggregation Per Year -----------------
df_joined = df_fact \
    .join(df_datetime, df_fact.datetimestamp_id == df_datetime.datetime_id, "inner") \
    .join(broadcast(df_payment), df_fact.payment_id == df_payment.payment_type, "inner")

df_year_result = df_joined.groupBy("pick_year") \
    .agg(
        count("trip_id").alias("total_trips"),
        sum("passenger_count").alias("total_passengers"),
        sum("trip_distance").alias("total_distance"),
        sum("total_amount").alias("total_amount"),
        avg("passenger_count").alias("average_passengers_per_trip"),
        avg("trip_distance").alias("average_distance_per_trip"),
        avg("total_amount").alias("average_amount_per_trip")) \
    .select(
        col("pick_year").alias("year"),
        col("total_trips"),
        col("total_passengers"),
        col("total_distance"),
        col("total_amount"),
        col("average_passengers_per_trip"),
        col("average_distance_per_trip"),
        col("average_amount_per_trip")
)

df_payment_each_year_result = df_joined.groupBy("pick_year", "payment_id", "payment_type_name") \
    .agg(count("trip_id").alias("total_trips")) \
    .select(
        col("pick_year").alias("year"),
        col("payment_id"),
        col("payment_type_name").alias("payment_name"),
        col("total_trips")
)

df_year_result.write \
    .format("bigquery") \
    .option("table", output_year) \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()
df_year_result.unpersist()

df_payment_each_year_result.write \
    .format("bigquery") \
    .option("table", output_payment_year) \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()
df_payment_each_year_result.unpersist()

# ----------------- Aggregation Per Payment -----------------
df_payment_result = df_joined.groupBy("payment_id", "payment_type_name") \
    .agg(count("trip_id").alias("total_trips")) \
    .select(col("payment_id"),
            col("payment_type_name").alias("payment_name"),
            col("total_trips"))

df_payment_result.write \
    .format("bigquery") \
    .option("table", output_payment) \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

df_payment_result.unpersist()
df_joined.unpersist()
df_payment.unpersist()

# ----------------- Aggregation Per Location -----------------
# ----------------- Pickup Location -----------------
df_joined = df_fact.alias("fact_data") \
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

df_pickup_result = df_joined \
    .groupBy("year", "LocationID", "pickup_x", "pickup_y", "zone", "borough", "service_zone") \
    .agg(count("trip_id").alias("total_trips"))

df_pickup_result.write \
    .format("bigquery") \
    .option("table", output_pickup_location) \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()
df_pickup_result.unpersist()
df_joined.unpersist()
df_pickup_location.unpersist()

# ----------------- Dropoff Location -----------------
df_joined = df_fact.alias("fact_data") \
    .join(df_datetime.alias("dim_datetime"),
          col("fact_data.datetimestamp_id") == col("dim_datetime.datetime_id"), "inner") \
    .join(broadcast(df_dropoff_location.alias("dim_dropoff_location")),
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

df_dropoff_result = df_joined \
    .groupBy("year", "LocationID", "dropoff_x", "dropoff_y", "zone", "borough", "service_zone") \
    .agg(count("trip_id").alias("total_trips"))

df_dropoff_result.write \
    .format("bigquery") \
    .option("table", output_dropoff_location) \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

# Stop Spark Session
spark.stop()
