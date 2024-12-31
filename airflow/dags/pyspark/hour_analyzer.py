# Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start SparkSession
spark = SparkSession \
    .builder \
    .appName("Hourly Analyzer") \
    .getOrCreate()

# Path lists
fact_trip = "hdfs://10.128.0.59:8020/data_warehouse/fact_trip"
dim_datetime = "hdfs://10.128.0.59:8020/data_warehouse/dim_datetime"

# uber-analysis-439804.query_result. + the table's name
output = "uber-analysis-439804.query_result.trips_per_hour"

# Read data into dataframe
df_fact = spark.read \
    .format("parquet") \
    .option("path", fact_trip) \
    .load() \
    .select("trip_id", "datetimestamp_id")

df_datetime = spark.read \
    .format("parquet") \
    .option("path", dim_datetime) \
    .load() \
    .select("pick_year", "pick_hour", "datetime_id") \
    .withColumn("pick_hour", col("pick_hour").cast("int"))

# Join
df_joined = df_fact.join(df_datetime,
                         df_fact.datetimestamp_id == df_datetime.datetime_id,
                         "inner")

# Query
df_result = df_joined.groupBy("pick_year", "pick_hour") \
    .agg(count("trip_id").alias("total_trips")) \
    .select(
        col("pick_year").alias("year"),
        col("pick_hour").alias("hour"),
        col("total_trips")
)

# Save to BigQuery
df_result.write \
    .format("bigquery") \
    .option("table", output) \
    .option("temporaryGcsBucket", "uber-pyspark-jobs/temp") \
    .mode("overwrite") \
    .save()

df_result.unpersist()
df_joined.unpersist()
df_fact.unpersist()
df_datetime.unpersist()

spark.stop()
