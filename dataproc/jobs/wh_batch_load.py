from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
batch_year = sys.argv[1]

spark = SparkSession \
    .builder \
    .appName(f"Batch Process {batch_year}") \
    .getOrCreate()

input_path = f"gs://uber-{batch_year}-154055"
zone_lookup = "gs://uber-pyspark-jobs/taxi_zone_lookup.csv"

df_prev_fact = spark.read.format("bigquery") \
    .option('table', 'uber-analysis-439804.uber_warehouse.fact_trip') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .load() \
    .select("trip_id")
max_fact_id = df_prev_fact.agg({"trip_id": "max"}).collect()[0][0]

df_prev_datetime = spark.read.format("bigquery") \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_datetime') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .load() \
    .select("datetime_id")
max_datetime_id = df_prev_datetime.agg({"datetime_id": "max"}).collect()[0][0]

# raw schema
schema = StructType([
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

# raw dataframe
df = spark \
    .read \
    .format("parquet") \
    .load(input_path) \
    .dropna() \
    .filter(year(col("tpep_pickup_datetime")) == int(batch_year)) \
    .withColumn("trip_id", monotonically_increasing_id() + max_fact_id + 1)
df.printSchema()

lookup_schema = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])

df_lookup = spark \
    .read \
    .format("csv") \
    .schema(lookup_schema) \
    .option("header", True) \
    .load(zone_lookup) \
    .dropna()
df_lookup.printSchema()

# Datetime dimension
dim_datetime = df \
    .select("tpep_pickup_datetime", "tpep_dropoff_datetime") \
    .distinct() \
    .withColumn("datetime_id", monotonically_increasing_id() + max_datetime_id + 1) \
    .withColumn("pick_hour", hour(col("tpep_pickup_datetime"))) \
    .withColumn("pick_day", dayofmonth(col("tpep_pickup_datetime"))) \
    .withColumn("pick_month", month(col("tpep_pickup_datetime"))) \
    .withColumn("pick_year", year(col("tpep_pickup_datetime"))) \
    .withColumn("pick_weekday", dayofweek(col("tpep_pickup_datetime"))) \
    .withColumn("drop_hour", hour(col("tpep_dropoff_datetime"))) \
    .withColumn("drop_day", dayofmonth(col("tpep_dropoff_datetime"))) \
    .withColumn("drop_month", month(col("tpep_dropoff_datetime"))) \
    .withColumn("drop_year", year(col("tpep_dropoff_datetime"))) \
    .withColumn("drop_weekday", dayofweek(col("tpep_dropoff_datetime")))

# Rate code dimension
rate_code_type = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
}
# RatecodeID + rate_code_name
dim_rate_code = df \
    .select("RatecodeID") \
    .distinct() \
    .withColumn("rate_code_name",
                F.when(F.col("RatecodeID") == 1, rate_code_type[1])
                 .when(F.col("RatecodeID") == 2, rate_code_type[2])
                 .when(F.col("RatecodeID") == 3, rate_code_type[3])
                 .when(F.col("RatecodeID") == 4, rate_code_type[4])
                 .when(F.col("RatecodeID") == 5, rate_code_type[5])
                 .when(F.col("RatecodeID") == 6, rate_code_type[6])
                 .otherwise(None)
                ) \
    .dropna()

# Pickup location dimension
# PULocationID + Borough + Zone + service_zone
dim_pickup_location = df \
    .select("PULocationID") \
    .distinct() \
    .join(df_lookup, df.PULocationID == df_lookup.LocationID, "inner") \
    .select("PULocationID", "Borough", "Zone", "service_zone")

# Dropoff location dimension
# DOLocationID + Borough + Zone + service_zone
dim_dropoff_location = df \
    .select("DOLocationID") \
    .distinct() \
    .join(df_lookup, df.DOLocationID == df_lookup.LocationID, "inner") \
    .select("DOLocationID", "Borough", "Zone", "service_zone")

# dim_dropoff_location.show(5)

# Payment type dim
payment_type_name = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}
# payment_type + payment_type_name
dim_payment = df \
    .select("payment_type") \
    .distinct() \
    .withColumn("payment_type_name",
                F.when(F.col("payment_type") == 1, payment_type_name[1])
                 .when(F.col("payment_type") == 2, payment_type_name[2])
                 .when(F.col("payment_type") == 3, payment_type_name[3])
                 .when(F.col("payment_type") == 4, payment_type_name[4])
                 .when(F.col("payment_type") == 5, payment_type_name[5])
                 .when(F.col("payment_type") == 6, payment_type_name[6])
                 .otherwise(None)
                ) \
    .dropna()

# Vendor dimension
vendor_name = {
    1: "Create Mobile Technologies, LLC",
    2: "VeriFone Inc",
}
# VendorID + vendor_name
dim_vendor = df \
    .select("VendorID") \
    .distinct() \
    .withColumn("vendor_name",
                F.when(F.col("VendorID") == 1, vendor_name[1])
                 .when(F.col("VendorID") == 2, vendor_name[2])
                 .otherwise(None)
                ) \
    .dropna()

# Fact table
fact_trip = df.alias("fact_data") \
    .join(dim_vendor.alias("dim_vendor"), col("fact_data.VendorID") == col("dim_vendor.VendorID"), "inner") \
    .join(dim_datetime.alias("dim_datetime"), (col("fact_data.tpep_pickup_datetime") == col("dim_datetime.tpep_pickup_datetime")) & (
        col("fact_data.tpep_dropoff_datetime") == col("dim_datetime.tpep_dropoff_datetime")), "inner") \
    .join(dim_pickup_location.alias("dim_pickup_location"), col("fact_data.PULocationID") == col("dim_pickup_location.PULocationID"), "inner") \
    .join(dim_dropoff_location.alias("dim_dropoff_location"), col("fact_data.DOLocationID") == col("dim_dropoff_location.DOLocationID"), "inner") \
    .join(dim_rate_code.alias("dim_ratecode"), col("fact_data.RatecodeID") == col("dim_ratecode.RatecodeID"), "inner") \
    .join(dim_payment.alias("dim_payment"), col("fact_data.payment_type") == col("dim_payment.payment_type"), "inner") \
    .select(
        col("fact_data.trip_id"),
        col("dim_vendor.VendorID").alias("vendor_id"),
        col("dim_datetime.datetime_id").alias("datetimestamp_id"),
        col("dim_pickup_location.PULocationID").alias("pu_location_id"),
        col("dim_dropoff_location.DOLocationID").alias("do_location_id"),
        col("dim_ratecode.RatecodeID").alias("ratecode_id"),
        col("dim_payment.payment_type").alias("payment_id"),
        col("fact_data.passenger_count"),
        col("fact_data.trip_distance"),
        col("fact_data.fare_amount"),
        col("fact_data.extra"),
        col("fact_data.mta_tax"),
        col("fact_data.tip_amount"),
        col("fact_data.tolls_amount"),
        col("fact_data.total_amount")
        # delete cause of version conflict
        # df.congestion_surcharge,
        # df.Airport_fee,
        # df.improvement_surcharge,
)

# append diff rows
dim_prev_vendor = spark.read.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_vendor') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .load()
df_append_vendor = dim_vendor.subtract(dim_prev_vendor)
df_append_vendor.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_vendor') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("append") \
    .save()

# APPEND ALL
dim_datetime.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_datetime') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("append") \
    .save()

# append diff rows
dim_prev_ratecode = spark.read.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_rate_code') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .load()
dim_append_ratecode = dim_rate_code.subtract(dim_prev_ratecode)
dim_append_ratecode.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_rate_code') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("append") \
    .save()

# append diff rows
dim_prev_pu = spark.read.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_pickup_location') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .load()
dim_append_pu = dim_pickup_location.subtract(dim_prev_pu)
dim_append_pu.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_pickup_location') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("append") \
    .save()

# append diff rows
dim_prev_do = spark.read.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_dropoff_location') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .load()
dim_append_do = dim_dropoff_location.subtract(dim_prev_do)
dim_append_do.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_dropoff_location') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("append") \
    .save()

# append diff rows
dim_prev_payment = spark.read.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_payment') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .load()
dim_append_payment = dim_payment.subtract(dim_prev_payment)
dim_append_payment.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_payment') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("append") \
    .save()

# APPEND ALL
fact_trip.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.fact_trip') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("append") \
    .save()

spark.stop()
