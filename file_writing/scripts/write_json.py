from pyspark.sql import SparkSession
# Create Spark session
spark = SparkSession.builder.appName("Read and Write JSON").getOrCreate()
# Paths
input_path = r"C:\Users\KishoreRajanRavi\Desktop\pyspark\file_writing\data\employees.json"
output_path = r"C:\Users\KishoreRajanRavi\Desktop\pyspark\file_writing\output\employees_json"
# Step 1: Read JSON with multiline=True
df = spark.read.option("multiline", True).json(input_path)
# Step 2: Show schema and data
print("JSON Schema:")
df.printSchema()
print("JSON Data:")
df.show(truncate=False)
# Step 3: Write to JSON
df.write.mode("overwrite").json(output_path)
print("JSON written successfully to:", output_path)
