# PySpark Job to load new data to Warehouse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
batch_year = sys.argv[1]

spark = SparkSession \
    .builder \
    .appName("Monthly Load") \
    .getOrCreate()

# Path lists
zone_lookup = "hdfs://10.128.0.59:8020/raw_data/taxi_zone_lookup.csv"
input_path = "gs://new-data-154055"

output_fact_trip = "hdfs://10.128.0.59:8020/data_warehouse/fact_trip"
output_dim_datetime = "hdfs://10.128.0.59:8020/data_warehouse/dim_datetime"
output_dim_pickup_location = "hdfs://10.128.0.59:8020/data_warehouse/dim_pickup_location"
output_dim_dropoff_location = "hdfs://10.128.0.59:8020/data_warehouse/dim_dropoff_location"

# Schema
input_schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

lookup_schema = StructType([
    StructField("LocationID", LongType(), True),
    StructField("X", DoubleType(), True),
    StructField("Y", DoubleType(), True),
    StructField("zone", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("service_zone", StringType(), True)
])

# Get last ID
df_prev_fact = spark.read.format("parquet") \
    .option("path", output_fact_trip) \
    .load() \
    .select("trip_id")
max_fact_id = df_prev_fact.agg({"trip_id": "max"}).collect()[0][0]

df_prev_datetime = spark.read.format("parquet") \
    .option("path", output_dim_datetime) \
    .load() \
    .select("datetime_id")
max_datetime_id = df_prev_datetime.agg({"datetime_id": "max"}).collect()[0][0]

# Load new data
df_input = spark.read.format("parquet") \
    .schema(input_schema) \
    .load(input_path) \
    .dropna() \
    .filter((year(col("tpep_pickup_datetime")) == batch_year) &
            (col("trip_distance") > 0.0) &
            (col("passenger_count") > 0)) \
    .withColumn("trip_id", monotonically_increasing_id() + max_fact_id + 1)
df_input.printSchema()

df_lookup = spark.read.format("csv") \
    .schema(lookup_schema) \
    .option("header", True) \
    .load(zone_lookup) \
    .dropna()
df_lookup.printSchema()

# Transform Output
# Datetime dimension
dim_datetime = df_input \
    .select("tpep_pickup_datetime", "tpep_dropoff_datetime") \
    .distinct() \
    .withColumn("datetime_id", monotonically_increasing_id() + max_datetime_id + 1) \
    .withColumn("pick_hour", hour(col("tpep_pickup_datetime")) + minute(col("tpep_pickup_datetime")) / 60.0) \
    .withColumn("pick_day", dayofmonth(col("tpep_pickup_datetime"))) \
    .withColumn("pick_month", month(col("tpep_pickup_datetime"))) \
    .withColumn("pick_year", year(col("tpep_pickup_datetime"))) \
    .withColumn("pick_weekday", F.date_format(col("tpep_pickup_datetime"), "EEEE")) \
    .withColumn("pick_weekday_id", dayofweek(col("tpep_pickup_datetime"))) \
    .withColumn("drop_hour", hour(col("tpep_dropoff_datetime")) + minute(col("tpep_dropoff_datetime")) / 60.0) \
    .withColumn("drop_day", dayofmonth(col("tpep_dropoff_datetime"))) \
    .withColumn("drop_month", month(col("tpep_dropoff_datetime"))) \
    .withColumn("drop_year", year(col("tpep_dropoff_datetime"))) \
    .withColumn("drop_weekday", F.date_format(col("tpep_pickup_datetime"), "EEEE")) \
    .withColumn("drop_weekday_id", dayofweek(col("tpep_pickup_datetime")))

# Pickup location dimension
# PULocationID + Borough + Zone + service_zone
dim_pickup_location = df_input \
    .select("PULocationID") \
    .distinct() \
    .join(df_lookup, df_input.PULocationID == df_lookup.LocationID, "inner") \
    .select("PULocationID", "X", "Y", "zone", "borough", "service_zone")

# Dropoff location dimension
# DOLocationID + Borough + Zone + service_zone
dim_dropoff_location = df_input \
    .select("DOLocationID") \
    .distinct() \
    .join(df_lookup, df_input.DOLocationID == df_lookup.LocationID, "inner") \
    .select("DOLocationID", "X", "Y", "zone", "borough", "service_zone")

# Fact table
fact_trip = df_input.alias("fact_data") \
    .join(dim_datetime.alias("dim_datetime"), (col("fact_data.tpep_pickup_datetime") == col("dim_datetime.tpep_pickup_datetime")) & (col("fact_data.tpep_dropoff_datetime") == col("dim_datetime.tpep_dropoff_datetime")), "inner") \
    .select(
        col("fact_data.trip_id"),
        col("fact_data.VendorID").alias("vendor_id"),
        col("dim_datetime.datetime_id").alias("datetimestamp_id"),
        col("fact_data.PULocationID").alias("pu_location_id"),
        col("fact_data.DOLocationID").alias("do_location_id"),
        col("fact_data.RatecodeID").alias("ratecode_id").cast("long"),
        col("fact_data.payment_type").alias("payment_id").cast("long"),
        col("fact_data.passenger_count"),
        col("fact_data.trip_distance"),
        col("fact_data.fare_amount"),
        col("fact_data.extra"),
        col("fact_data.mta_tax"),
        col("fact_data.tip_amount"),
        col("fact_data.tolls_amount"),
        col("fact_data.total_amount")
)

# Append to HDFS
# Dim datetime
dim_datetime.write \
    .partitionBy("pick_year", "pick_month") \
    .format("parquet") \
    .option("path", output_dim_datetime) \
    .mode("append") \
    .save()

# Dim Location
dim_prev_pu = spark.read.format("parquet") \
    .load(output_dim_pickup_location)
dim_append_pu = dim_pickup_location.subtract(dim_prev_pu)
dim_append_pu.write \
    .partitionBy("service_zone") \
    .format("parquet") \
    .option("path", output_dim_pickup_location) \
    .mode("append") \
    .save()
dim_prev_pu.unpersist()
dim_append_pu.unpersist()

dim_prev_do = spark.read.format("parquet") \
    .load(output_dim_dropoff_location)
dim_append_do = dim_dropoff_location.subtract(dim_prev_do)
dim_append_do.write \
    .partitionBy("service_zone") \
    .format("parquet") \
    .option("path", output_dim_dropoff_location) \
    .mode("append") \
    .save()
dim_prev_do.unpersist()
dim_append_do.unpersist()

# Fact trip
fact_trip.write \
    .format("parquet") \
    .option("path", output_fact_trip) \
    .mode("append") \
    .save()

spark.stop()
