from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.appName("Simple Upsert Simulation").getOrCreate()

# Existing base data
data_base = [(1, "Apple"), (2, "Banana")]
df_base = spark.createDataFrame(data_base, ["id", "fruit"])
append_path = "append_output"

# Save existing data
df_base.write.mode("overwrite").parquet(append_path)

# Load base
df_existing = spark.read.parquet(append_path)

# New data to append
data_new = [(3, "Cherry"), (4, "Date")]
df_new = spark.createDataFrame(data_new, ["id", "fruit"])

# Append new data to the same location
df_new.write.mode("append").parquet(append_path)

# Read and show the final data
print("Final Table after Append:")
spark.read.parquet(append_path).orderBy("id").show()