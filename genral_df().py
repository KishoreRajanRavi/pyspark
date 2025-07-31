#genral dataframe functions:
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("genral df function").getOrCreate()
#sample data
data=[
    ('kishore',22,50000,'development'),
    ('kiran',24,75000,'HR'),
    ('mithun',35,50000,'Sales'),
    ('Anu',23,120000,'IT'),
    ('Ram',26,75000,'Marketing')
]
columns=['Name','Age','Salary','Department']
#create dataframe
df=spark.createDataFrame(data,columns)

#1.show()
#df.show()

#2.collect()
"""
rows=df.collect()
print(rows[0])
"""
#3.take(n)
"""print(df.take(2))"""
#4.printschema()
"""
df.printSchema()
df.show()
"""
#count()
"""
print("Row count:", df.count())
"""
#select()
"""
df.select("Name","Salary").show()
"""
#filter or where()
"""
df.filter(df.Salary>70000).show()
df.where(df.Department=='HR').show()"""

#like()
"""
df.filter(df.Name.like("k%")).show()
"""
#sort()
"""
df.sort("Salary").show()
df.sort(df.Salary.desc()).show()
"""
#describe()
"""
df.describe("Age","Salary").show()
"""
#columns
print("Column names:",df.columns)