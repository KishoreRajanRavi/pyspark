from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder \
    .appName("SCD Type 1 Example") \
    .getOrCreate()

#  Existing customer data (Target)
data_existing = [
    (1, "Alice", "New York"),
    (2, "Bob", "Los Angeles"),
    (3, "Charlie", "Chicago")
]

columns = ["customer_id", "name", "city"]

df_existing = spark.createDataFrame(data_existing, columns)
print(" Existing Customer Data:")
df_existing.show()

#  New incoming data (Source) – e.g., Bob moved to San Francisco
data_new = [
    (2, "Bob", "San Francisco"),
    (4, "David", "Houston")
]

df_new = spark.createDataFrame(data_new, columns)
print("Incoming New Data:")
df_new.show()

# Type 1 Logic: Overwrite based on customer_id
# Step 1: Remove records from existing that are in incoming (same customer_id)
df_remaining = df_existing.join(df_new, on="customer_id", how="left_anti")

# Step 2: Union with the new data to get the final SCD Type 1 result
df_type1 = df_remaining.unionByName(df_new)

print(" Final Table after SCD Type 1 (Overwrite):")
df_type1.orderBy("customer_id").show()