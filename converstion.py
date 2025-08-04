from pyspark.sql import SparkSession
#required to import
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("CAST Example").getOrCreate()

data = [
    ("101", "25", "50000.5"),
    ("102", "30", "60000.75"),
    ("103", "28", "45000.0")
]

columns = ["EmpID", "Age", "Salary"]
df = spark.createDataFrame(data, columns)
"""
df.show()
df.printSchema()
"""
#CAST Age to Integer
df_casted = df.withColumn("Age_Int", col("Age").cast("int"))
df_casted.show()
df_casted.printSchema()

#CAST Salary to Float
df_casted = df_casted.withColumn("Salary_Float", col("Salary").cast("float"))
df_casted.show()
df_casted.printSchema()
