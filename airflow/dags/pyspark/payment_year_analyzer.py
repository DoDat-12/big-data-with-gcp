# Import
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("Year and Payment Analyzer") \
    .getOrCreate()

# Path lists
fact_trip = "hdfs://10.128.0.59:8020/data_warehouse/fact_trip"
dim_datetime = "hdfs://10.128.0.59:8020/data_warehouse/dim_datetime"
dim_payment = "hdfs://10.128.0.59:8020/data_warehouse/dim_payment"

# uber-analysis-439804.query_result. + the table's name
output_year = "uber-analysis-439804.query_result.agg_per_year"
output_payment_year = "uber-analysis-439804.query_result.payment_per_year"
output_payment = "uber-analysis-439804.query_result.agg_per_payment"

# Read data into dataframe
df_fact = spark.read \
    .format("parquet") \
    .option("path", fact_trip) \
    .load()

df_datetime = spark.read \
    .format("parquet") \
    .option("path", dim_datetime) \
    .load() \
    .select("pick_year", "datetime_id")

df_payment = spark.read \
    .format("parquet") \
    .option("path", dim_payment) \
    .load() \
    .filter(~col("payment_type").isin([4, 6]))

# Join
df_joined = df_fact \
    .join(df_datetime,
          df_fact.datetimestamp_id == df_datetime.datetime_id, "inner") \
    .join(broadcast(df_payment),
          df_fact.payment_id == df_payment.payment_type, "inner")

# Aggregation each year
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

df_payment_result = df_joined.groupBy("payment_id", "payment_type_name") \
    .agg(count("trip_id").alias("total_trips")) \
    .select(
        col("payment_id"),
        col("payment_type_name").alias("payment_name"),
        col("total_trips")
)

# Save to BigQuery
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

df_payment_result.write \
    .format("bigquery") \
    .option("table", output_payment) \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()
df_payment_result.unpersist()

df_joined.unpersist()
df_fact.unpersist()
df_datetime.unpersist()
df_payment.unpersist()

spark.stop()
