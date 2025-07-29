from operations.sql import SparkSession
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Test Spark App") \
    .getOrCreate()
data = [("Kishore", 21), ("Ravi", 22), ("Anu", 20)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, schema=columns)
df.show()

#operations
df.select("Name").show()

#filter
df.filter(df["Age"] > 20).show()

#add new column
df.withColumn("Age_Next_Year", df["Age"] + 1).show()

#Aggregation
from operations.sql.functions import avg, max
df.groupBy().agg(avg("Age"), max("Age")).show()

