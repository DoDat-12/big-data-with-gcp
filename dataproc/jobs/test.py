from pyspark.sql import SparkSession

print("What's up motherf*cker")

spark = SparkSession.builder.appName("Test").getOrCreate()

data = [(i,) for i in range(1, 11)]

df = spark.createDataFrame(data, ["number"])
df.show()

print(df)

spark.stop()
