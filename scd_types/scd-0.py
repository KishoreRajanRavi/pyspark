from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder \
    .appName("SCD Type 1 Example") \
    .getOrCreate()

# Spark session
spark = SparkSession.builder.appName("SCD Type 0").getOrCreate()

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

df_existing = spark.createDataFrame(data_existing, columns)
print("Existing Customer Data:")
df_existing.show()

# New incoming data – Bob moved to San Francisco, and a new customer David appears
data_incoming = [
    (2, "Bob", "San Francisco"),  # Should be ignored due to SCD Type 0
    (4, "David", "Houston")       # Should be added
]

df_incoming = spark.createDataFrame(data_incoming, columns)
print(" Incoming Data:")
df_incoming.show()

# SCD Type 0: Keep only new records — ignore updates to existing ones
df_new_records = df_incoming.join(df_existing, on="customer_id", how="left_anti")

# Final dimension table = existing + only new records
df_final = df_existing.unionByName(df_new_records)

print(" Final Table after SCD Type 0 (No Overwrites):")
df_final.orderBy("customer_id").show()
