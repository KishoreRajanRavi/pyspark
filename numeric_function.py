from pyspark.sql import SparkSession
from pyspark.sql.functions import*
# * import all sum, avg, min, max, round, abs functions
spark=SparkSession.builder.appName("Numeric Functions").getOrCreate()
"""
data=[
    ('Kishore',22,50000,"development"),
    ('Manoj',21,45000,"HR"),
    ('Mithun',24,80000,"Sales"),
    ('Ram',30,87000,"Marketing"),
    ('Anu',27,53000,"IT")
]
columns=["Name","Age","Salary","Department"]
df=spark.createDataFrame(data,columns)
"""

#sum()
"""
df.select(sum("salary")).show()
"""
#avg()
"""
df.select(avg("Salary").alias("Average_Salary")).show()
"""
#min()
"""
df.select(min("Salary").alias("Min_Salary")).show()
"""
#max()
"""
df.select(max("Salary").alias("Max_Salary")).show()
"""
#round()
"""
df.select("Name",round(df["Salary"]/1000,2).alias("Salary_in_thousands")).show()
"""
sample_data=[("Kishore",4000),("murali",-3000),("kevin",-5000)]
sample_columns=("Name","Balance")
df=spark.createDataFrame(sample_data,sample_columns)

df.select("Name",abs("Balance").alias("Abs_Balance")).show()


