from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("Check Schema") \
    .getOrCreate()

zone_lookup = "gs://uber-pyspark-jobs/taxi_zone_lookup.csv"

spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2011-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2012-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2013-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2014-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2015-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2016-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2017-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2018-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2019-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2020-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2021-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2022-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2023-01.parquet") \
    .printSchema()
spark.read.format("parquet") \
    .load("gs://uber-lake-yellow/yellow_tripdata_2024-01.parquet") \
    .printSchema()

spark.stop()
