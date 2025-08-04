from pyspark.sql import SparkSession
#reuired imports are
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Window Functions").getOrCreate()

data = [
    ("Kishore", "Sales", 50000),
    ("Kiran", "Sales", 60000),
    ("Ravi", "Sales", 50000),
    ("Anu", "HR", 75000),
    ("Priya", "HR", 82000),
    ("Mithun", "HR", 75000),
    ("Ram", "IT", 90000),
    ("Raj", "IT", 90000)
]

columns = ["Name", "Department", "Salary"]
df = spark.createDataFrame(data, columns)
#df.show()

#Define window specification
window_spec = Window.partitionBy("Department").orderBy(df["Salary"].desc())
#row_number()
"""
df.withColumn("row_num", row_number().over(window_spec)).show()
"""
#rank()
"""
df.withColumn("rank", rank().over(window_spec)).show()
"""
#denserank()
"""
df.withColumn("dense_rank", dense_rank().over(window_spec)).show()
"""
#lead()
"""
df.withColumn("lead_salary", lead("Salary", 1).over(window_spec)).show()
"""
#lag()
df.withColumn("lag_salary", lag("Salary", 1).over(window_spec)).show()
