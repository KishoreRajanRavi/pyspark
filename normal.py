from operations.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Test Spark App") \
    .getOrCreate()

# # Create DataFrame from list
# data = [("Kishore", "Sales", 70000),
#         ("Ravi", "HR", 60000),
#         ("Anu", "IT", 75000)]
#
# columns = ["Name", "Department", "Salary"]
#
# df = spark.createDataFrame(data, columns)
#
# # Operations
# df.show()
# df.printSchema()
# df.filter(df["Salary"] > 65000).show()

# Sample DataFrame
# data = [("Kishore", 70000), ("Ravi", 50000), ("Anu", 65000)]
# columns = ["Name", "Salary"]
#
# df = spark.createDataFrame(data, columns)
#
#
# # Transformation
# filtered_df = df.filter(df["Salary"] > 60000)
#
# # Action
# filtered_df.show()

#
# # Create RDD from a list
# rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
#
# # Transformation
# mapped_rdd = rdd.map(lambda x: x * 2)
#
# # Action
# print(mapped_rdd.collect())

#create rdd
# rdd = spark.sparkContext.parallelize([1, 2, 3])
# #map()
# mapped_rdd = rdd.map(lambda x: x * 2)
# print("Mapped RDD Output:", mapped_rdd.collect())
# #filer()
# filtered_rdd = rdd.filter(lambda x: x > 1)
# print("Filtered RDD Output:", filtered_rdd.collect())
# #reduce()
# reduced_value = rdd.reduce(lambda x, y: x + y)
# print("Reduced RDD Output:", reduced_value)

data = [("Kishore", 70000), ("Ravi", 50000), ("Anu", 65000)]
columns = ["Name", "Salary"]
#
df = spark.createDataFrame(data, columns)
# df.show()

from operations.sql import Row
row = Row(name="Kishore", age=21)
print(row.name)

df.select(df["Salary"] + 5).show()


