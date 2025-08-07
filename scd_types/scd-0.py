import os
from pyspark.sql import SparkSession

# Set Hadoop path (for Windows)
os.environ['HADOOP_HOME'] = r"C:\hadoop\hadoop-3.2.2"

# Spark session
spark = SparkSession.builder.appName("SCD Type 0").getOrCreate()

# Original Customer Dimension Data
data_existing = [
    (101, "Kishore", "Basic"),
    (102, "Anu", "Premium")
]
columns = ["CustomerID", "Name", "SignupPlan"]

df_existing = spark.createDataFrame(data_existing, columns)
print("Existing Dimension Table (Original):")
df_existing.show(truncate=False)

#  Incoming Updates (which we will ignore as per SCD 0)
data_updates = [
    (101, "Kishore", "Gold"),     # Attempted change
    (102, "Anu", "Platinum")      # Attempted change
]
df_updates = spark.createDataFrame(data_updates, columns)
print(" Incoming Update Requests:")
df_updates.show(truncate=False)

#  SCD Type 0: Do NOT update. Keep only original data.
df_final = df_existing
print("Final Dimension Table after applying SCD Type 0:")
df_final.show(truncate=False)
