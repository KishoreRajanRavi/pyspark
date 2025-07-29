from pyspark.sql import SparkSession
# Spark session
spark = SparkSession.builder.appName("cache_example").getOrCreate()
# Create sample DataFrame
data = [("Kishore", 70000), ("Ravi", 50000)]
df = spark.createDataFrame(data, ["Name", "Salary"])
# Cache the DataFrame
df.cache()
# Perform multiple actions
df.show()
df.select("Name").show()
#To remove cached data and free memory:




