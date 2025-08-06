from pyspark.sql import SparkSession
# Step 1: Create Spark session
spark = SparkSession.builder.appName("Read Parquet").getOrCreate()

# Step 2: Read the Parquet file
df_parquet = spark.read.parquet(
    r"C:\Users\KishoreRajanRavi\Desktop\pyspark\file_reading\data\house_prices.parquet"
)

# Step 3: Print schema and show data
print("Parquet Schema (Auto-detected):")
df_parquet.printSchema()

print("Parquet Data Preview:")
df_parquet.show(5, truncate=False)



