from pyspark.sql import SparkSession
import os
# Step 1: Create Spark session
spark = SparkSession.builder.appName("Read and Write CSV").getOrCreate()
# Step 2: Set input and output paths
input_path = r"C:\Users\KishoreRajanRavi\Desktop\pyspark\file_writing\data\employee.csv"
output_path = r"C:\Users\KishoreRajanRavi\Desktop\pyspark\file_writing\output\employee_csv"
# Step 3: Read the CSV with header and schema inference
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
# Show the schema and data
print("Schema:")
df.printSchema()
print(" Data:")
df.show(5)
# Step 4: Write the DataFrame to CSV
df.write.mode("overwrite").option("header", True).csv(output_path)
print(" CSV written successfully to:", output_path)


