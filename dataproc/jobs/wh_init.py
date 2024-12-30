# Import
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("Warehouse Init Job") \
    .getOrCreate()

# Path lists
zone_lookup = "hdfs://10.128.0.59:8020/raw_data/updated_zone_lookup.csv"
input_path = "hdfs://10.128.0.59:8020/raw_data/2014"

output_fact_trip = "hdfs://10.128.0.59:8020/data_warehouse/fact_trip"
output_dim_vendor = "hdfs://10.128.0.59:8020/data_warehouse/dim_vendor"
output_dim_datetime = "hdfs://10.128.0.59:8020/data_warehouse/dim_datetime"
output_dim_rate_code = "hdfs://10.128.0.59:8020/data_warehouse/dim_rate_code"
output_dim_pickup_location = "hdfs://10.128.0.59:8020/data_warehouse/dim_pickup_location"
output_dim_dropoff_location = "hdfs://10.128.0.59:8020/data_warehouse/dim_dropoff_location"
output_dim_payment = "hdfs://10.128.0.59:8020/data_warehouse/dim_payment"

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
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])

df_input = spark.read.format("parquet") \
    .schema(input_schema) \
    .load(input_path) \
    .dropna() \
    .filter((year(col("tpep_pickup_datetime")) == 2014) &
            (col("trip_distance") > 0.0) &
            (col("passenger_count") > 0)) \
    .withColumn("trip_id", monotonically_increasing_id())

df_lookup = spark.read.format("csv") \
    .schema(lookup_schema) \
    .option("header", True) \
    .load(zone_lookup) \
    .dropna()

df_input.printSchema()
df_lookup.printSchema()
# df_input.cache() - not enough RAM

# Processing data
# Datetime dimension
dim_datetime = df_input \
    .select("tpep_pickup_datetime", "tpep_dropoff_datetime") \
    .distinct() \
    .withColumn("datetime_id", monotonically_increasing_id()) \
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
    .withColumn("drop_weekday", F.date_format(col("tpep_dropoff_datetime"), "EEEE")) \
    .withColumn("drop_weekday_id", dayofweek(col("tpep_dropoff_datetime")))


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
dim_rate_code = df_input \
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
dim_payment = df_input \
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

# dim_payment.printSchema()
# dim_payment.distinct().show()

# Vendor dimension
vendor_name = {
    1: "Create Mobile Technologies, LLC",
    2: "VeriFone Inc",
}
# VendorID + vendor_name
dim_vendor = df_input \
    .select("VendorID") \
    .distinct() \
    .withColumn("vendor_name",
                F.when(F.col("VendorID") == 1, vendor_name[1])
                 .when(F.col("VendorID") == 2, vendor_name[2])
                 .otherwise(None)
                ) \
    .dropna()

# Fact table
fact_trip = df_input.alias("fact_data") \
    .join(dim_datetime.alias("dim_datetime"), (col("fact_data.tpep_pickup_datetime") == col("dim_datetime.tpep_pickup_datetime")) & (col("fact_data.tpep_dropoff_datetime") == col("dim_datetime.tpep_dropoff_datetime")), "inner") \
    .join(dim_pickup_location.alias("dim_pickup_location"), col("fact_data.PULocationID") == col("dim_pickup_location.PULocationID"), "inner") \
    .join(dim_dropoff_location.alias("dim_dropoff_location"), col("fact_data.DOLocationID") == col("dim_dropoff_location.DOLocationID"), "inner") \
    .select(
        col("fact_data.trip_id"),
        col("fact_data.VendorID").alias("vendor_id"),
        col("dim_datetime.datetime_id").alias("datetimestamp_id"),
        col("dim_pickup_location.PULocationID").alias("pu_location_id"),
        col("dim_dropoff_location.DOLocationID").alias("do_location_id"),
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

# Write to HDFS
fact_trip.write \
    .format("parquet") \
    .option("path", output_fact_trip) \
    .mode("overwrite") \
    .save()

# SCD Type II
# dim_datetime_partitioned = dim_datetime.coalesce(24)
dim_datetime.write \
    .partitionBy("pick_year", "pick_month") \
    .format("parquet") \
    .option("path", output_dim_datetime) \
    .mode("overwrite") \
    .save()

# SCD Type I
dim_vendor.write \
    .partitionBy("VendorID") \
    .format("parquet") \
    .option("path", output_dim_vendor) \
    .mode("overwrite") \
    .save()

# SCD Type I
dim_rate_code.write \
    .partitionBy("RatecodeID") \
    .format("parquet") \
    .option("path", output_dim_rate_code) \
    .mode("overwrite") \
    .save()

# SCD Type II
# pickup = dim_pickup_location.coalesce(12)
dim_pickup_location.write \
    .partitionBy("service_zone") \
    .format("parquet") \
    .option("path", output_dim_pickup_location) \
    .mode("overwrite") \
    .save()

# SCD Type II
# dropoff = dim_dropoff_location.coalesce(12)

dim_dropoff_location.write \
    .partitionBy("service_zone") \
    .format("parquet") \
    .option("path", output_dim_dropoff_location) \
    .mode("overwrite") \
    .save()

# SCD Type I
dim_payment.write \
    .partitionBy("payment_type") \
    .format("parquet") \
    .option("path", output_dim_payment) \
    .mode("overwrite") \
    .save()

# Stop Spark Session
spark.stop()
