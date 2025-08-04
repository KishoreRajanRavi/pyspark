from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Math Functions").getOrCreate()
data = [
    (1, -10.5),
    (2, 0.0),
    (3, 3.14),
    (4, 2.718),
    (5, 9.0)
]

columns = ["ID", "Value"]
df = spark.createDataFrame(data, columns)
#df.show()

#abs()
"""
df.select("ID", "Value", abs("Value").alias("ABS_Value")).show()
"""
#ceil()
"""
df.select("ID", "Value", ceil("Value").alias("Ceil_Value")).show()
"""
#floor()
"""
df.select("ID", "Value", floor("Value").alias("Floor_Value")).show()
"""
#exp()
"""
df.select("ID", "Value", exp("Value").alias("Exp_Value")).show()
"""
#log
"""
df.select("ID", "Value", log("Value").alias("Log_Value")).show()
"""
#pow
df.select("ID", "Value", pow("Value", 2).alias("Power2")).show()
#SQRT
df.select("ID", "Value", sqrt("Value").alias("Sqrt_Value")).show()


