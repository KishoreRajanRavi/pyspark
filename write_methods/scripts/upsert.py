from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.appName("Simple Upsert Simulation").getOrCreate()

# Existing base data
data_base = [(1, "Apple"), (2, "Banana")]
df_base = spark.createDataFrame(data_base, ["id", "fruit"])
upsert_path = "simple_upsert_output"

# Save base data
df_base.write.mode("overwrite").parquet(upsert_path)

# Load base
df_existing = spark.read.parquet(upsert_path)

# Incoming new data: id=2 should be updated, id=3 is new
data_new = [(2, "Blueberry"), (3, "Cherry")]
df_new = spark.createDataFrame(data_new, ["id", "fruit"])

# Remove matching ids from base
df_filtered = df_existing.join(df_new, on="id", how="left_anti")

# Union: keep updated/new rows
df_result = df_filtered.union(df_new)

# Overwrite final result
df_result.write.mode("overwrite").parquet(upsert_path)

# Show result
print("Final Table after Simulated Upsert:")
spark.read.parquet(upsert_path).orderBy("id").show()
