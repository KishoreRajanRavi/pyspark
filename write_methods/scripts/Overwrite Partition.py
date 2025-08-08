from pyspark.sql import SparkSession
# Initialize Spark session
spark = SparkSession.builder.appName("Overwrite Partition").getOrCreate()
# Required config for dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# Sample data with a partition column (e.g., Department)
data = [
    (101, "Kishore", "Sales", 60000),
    (102, "Anu", "HR", 50000),
    (103, "Ravi", "Sales", 65000),
    (104, "Ram", "IT", 70000),
]
columns = ["EmpID", "Name", "Department", "Salary"]
df = spark.createDataFrame(data, columns)
# Show the initial data
print(" Initial Partitioned Data:")
df.show()
# Define output path (partitioned by Department)
output_path = r"C:\Users\KishoreRajanRavi\Desktop\pyspark\file_writing\output\overwrite_partition"
#  First write (base data)
df.write.mode("overwrite").partitionBy("Department").option("header", True).csv(output_path)
print(" Base partitioned data written.")
#  Now simulate update: overwrite only 'Sales' partition
update_data = [
    (101, "Kishore", "Sales", 62000),   # Updated salary
    (103, "Ravi", "Sales", 68000)       # Updated salary
]
df_update = spark.createDataFrame(update_data, columns)
#  Overwrite only the 'Sales' partition
df_update.write.mode("overwrite").partitionBy("Department").option("header", True).csv(output_path)
print("'Sales' partition overwritten successfully at:\n", output_path)


