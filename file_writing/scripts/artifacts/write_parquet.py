from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Read and Write Parquet").getOrCreate()
# Path
input_path =r"C:\Users\KishoreRajanRavi\Desktop\pyspark\file_writing\data\house-price.parquet"
output_path = r"C:\Users\KishoreRajanRavi\Desktop\pyspark\file_writing\output\house_prices_output.parquet"
# Read Parquet
df = spark.read.parquet(input_path)
# Show schema and preview
print("Parquet Schema:")
df.printSchema()
print("Parquet Data Preview:")
df.show(5)
#  Write to new Parquet location
df.write.mode("overwrite").parquet(output_path)
print("Parquet file written successfully to:", output_path)


