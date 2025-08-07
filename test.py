from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestApp").getOrCreate()
print("SparkSession created successfully!")
spark.stop()
