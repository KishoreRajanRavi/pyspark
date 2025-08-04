from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#all aggregate functions

spark=SparkSession.builder.appName("aggeregate functions").getOrCreate()

#import the datas and columns from numeric_function file
from numeric_function import data,columns
df=spark.createDataFrame(data,columns)

#mean and avg()
"""
df.select(mean("Salary").alias("Mean Salary")).show()
df.select(avg("Salary").alias("Average Salary")).show()
"""
#sum()
"""
df.select(sum("Salary").alias("Total Salary")).show()
"""
#Min and Max
"""
df.select(min("Salary").alias("Min_Salary")).show()
df.select(max("Salary").alias("Max_Salary")).show()
"""
#count()
"""
df.select(count("Salary").alias("Total Entries")).show()
"""

#countdistinct()
"""
df.select(count_distinct("Salary").alias("Unique_Salaries")).show()
"""
#collect_list()
"""
df.groupBy("Department").agg(collect_list("Name").alias("All_Names")).show(truncate=False)
"""
#collect_list()
"""
df.groupBy("Department").agg(collect_set("Name").alias("Unique_Names")).show(truncate=False)
"""
#first and last
df.select(first("Name").alias("First_Name")).show()
df.select(last("Name").alias("Last_Name")).show()
