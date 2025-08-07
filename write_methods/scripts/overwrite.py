from pyspark.sql import SparkSession
# Initialize Spark session
spark = SparkSession.builder.appName("Write Mode: Overwrite").getOrCreate()
# Sample data
data = [
    (101, "Kishore", "Developer", 60000),
    (102, "Anu", "Analyst", 50000),
    (103, "Ravi", "Manager", 75000)
]
columns = ["EmpID", "Name", "Role", "Salary"]
# Create DataFrame
df = spark.createDataFrame(data, columns)
# Show the data
print("Sample DataFrame:")
df.show()
# Output path (change as needed)
output_path = r"C:\Users\KishoreRajanRavi\Desktop\pyspark\write_methods\output\overwrite_example"
# Write using overwrite mode
df.write.mode("overwrite").option("header", True).csv(output_path)
print(f"Data successfully written using OVERWRITE mode to:\n{output_path}")


