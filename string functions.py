from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("String Functions").getOrCreate()
data = [
    ("  kishore  ", "dev,backend"),
    ("KIRAN", "HR,Admin"),
    ("   mithun", "Sales"),
    ("Anu", "IT"),
    ("Ram  ", "Marketing,Sales")
]

columns = ["Name", "Department"]
df = spark.createDataFrame(data, columns)

#upper()
"""
df.select("Name",upper("Name").alias ("upper_name")).show()
"""
#LOWER()
"""
df.select("Name", lower("Name").alias("LowerName")).show()
"""
#trim()
"""
df.select("Name", trim("Name").alias("Trimmed")).show()
"""
#ltrim()
"""
df.select("Name", ltrim("Name").alias("LeftTrimmed")).show()
"""
#rtrim()
"""
df.select("Name", rtrim("Name").alias("RightTrimmed")).show()
"""
#substring(col, start, length)
"""
df.select("Name", substring("Name", 2, 4).alias("Substr")).show()
"""
#substring_index(col, delim, count)
"""
df.select("Department", substring_index("Department", ",", 1).alias("FirstDept")).show()
"""
#split(col, pattern)
"""
df.select("Department", split("Department", ",").alias("DeptArray")).show(truncate=False)
"""
#repeat(col, n)
"""
df.select("Name", repeat(trim("Name"), 2).alias("Repeated")).show()
"""
#rpad(col, len, pad)
"""
df.select("Name", rpad(trim("Name"), 10, "#").alias("RPad")).show()
"""
# lpad(col, len, pad)
"""
df.select("Name", lpad(trim("Name"), 10, "*").alias("LPad")).show()
"""
#reg_exp replace
"""
df.select("Name", regexp_replace("Name", "  ", "").alias("NoSpaces")).show()
"""
#regexp_extract(col, pattern, group)
"""
df.select("Department", regexp_extract("Department", r'(\w+)', 1).alias("FirstWord")).show()
"""
#length()
"""
df.select("Name", length("Name").alias("Length")).show()
"""
#instr(col, substring)
"""
df.select("Department", instr("Department", "Sales").alias("Sales_Pos")).show()
"""
#initcap()
df.select("Name", initcap("Name").alias("InitCap")).show()







