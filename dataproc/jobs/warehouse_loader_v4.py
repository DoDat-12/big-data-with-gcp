from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("Test access raw bucket") \
    .getOrCreate()

input_path = "gs://uber-datalake"
zone_lookup = "gs://uber-pyspark-jobs/taxi_zone_lookup.csv"

# raw dataframe
df = spark \
    .read \
    .format("parquet") \
    .load(input_path) \
    .distinct() \
    .dropna() \
    .withColumn("trip_id", monotonically_increasing_id())

lookup_schema = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])

df.printSchema()

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
    .withColumn("datetime_id", monotonically_increasing_id()) \
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

dim_datetime.show()

# Rate code dimension
rate_code_type = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
}

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
    .dropna() \
    .withColumn("rate_code_id", monotonically_increasing_id())

dim_rate_code.show()

# Pickup location dimension
dim_pickup_location = df \
    .select("PULocationID") \
    .distinct() \
    .join(df_lookup, df.PULocationID == df_lookup.LocationID, "inner") \
    .select("PULocationID", "Borough", "Zone", "service_zone") \
    .withColumn("pickup_location_id", monotonically_increasing_id())

dim_pickup_location.show()

# Dropoff location dimension
dim_dropoff_location = df \
    .select("DOLocationID") \
    .distinct() \
    .join(df_lookup, df.DOLocationID == df_lookup.LocationID, "inner") \
    .select("DOLocationID", "Borough", "Zone", "service_zone") \
    .withColumn("dropoff_location_id", monotonically_increasing_id())

dim_dropoff_location.show()

# Payment type dim
payment_type_name = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}

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
    .dropna() \
    .withColumn("payment_type_id", monotonically_increasing_id())

dim_payment.show()

# Vendor dimension
vendor_name = {
    1: "Create Mobile Technologies, LLC",
    2: "VeriFone Inc",
}

dim_vendor = df \
    .select("VendorID") \
    .distinct() \
    .withColumn("vendor_name",
                F.when(F.col("VendorID") == 1, vendor_name[1])
                 .when(F.col("VendorID") == 2, vendor_name[2])
                 .otherwise(None)
                ) \
    .dropna() \
    .withColumn("vendor_id", monotonically_increasing_id())

dim_vendor.show()

# Fact table
fact_trip = df \
    .join(dim_vendor, df.VendorID == dim_vendor.VendorID, "inner") \
    .join(dim_datetime, (df.tpep_pickup_datetime == dim_datetime.tpep_pickup_datetime) & (df.tpep_dropoff_datetime == dim_datetime.tpep_dropoff_datetime), "inner") \
    .join(dim_pickup_location, df.PULocationID == dim_pickup_location.PULocationID, "inner") \
    .join(dim_dropoff_location, df.DOLocationID == dim_dropoff_location.DOLocationID, "inner") \
    .join(dim_rate_code, df.RatecodeID == dim_rate_code.RatecodeID, "inner") \
    .join(dim_payment, df.payment_type == dim_payment.payment_type, "inner") \
    .select(
        df.trip_id,
        dim_vendor.vendor_id,
        dim_datetime.datetime_id,
        dim_pickup_location.pickup_location_id,
        dim_dropoff_location.dropoff_location_id,
        dim_rate_code.rate_code_id,
        dim_payment.payment_type_id,
        df.passenger_count,
        df.trip_distance,
        df.fare_amount,
        df.extra,
        df.mta_tax,
        df.tip_amount,
        df.tolls_amount,
        df.improvement_surcharge,
        df.total_amount,
        df.congestion_surcharge,
        df.Airport_fee,
    )

fact_trip.show()

dim_vendor.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_vendor') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

dim_datetime.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_datetime') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

dim_rate_code.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_rate_code') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

dim_pickup_location.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_pickup_location') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

dim_dropoff_location.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_dropoff_location') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

dim_payment.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.dim_payment') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

fact_trip.write.format('bigquery') \
    .option('table', 'uber-analysis-439804.uber_warehouse.fact_trip') \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

spark.stop()
