from pyspark.sql import SparkSession
from pyspark import StorageLevel
# Step 1: Start Spark session
spark = SparkSession.builder.appName("persist_example").getOrCreate()
# Step 2: Create sample data
data = [("Kishore", 70000), ("Ravi", 50000), ("Anand", 45000)]
columns = ["Name", "Salary"]
# Step 3: Create DataFrame
df = spark.createDataFrame(data, schema=columns)
# Step 4: Persist DataFrame using MEMORY_AND_DISK
df.persist(StorageLevel.MEMORY_AND_DISK)
# Step 5: Perform multiple actions
print("=== Full DataFrame ===")
df.show()
print("=== Names only ===")
df.select("Name").show()
print("=== Average Salary ===")
df.groupBy().avg("Salary").show()
# Step 6: To clear memory:
df.unpersist()
