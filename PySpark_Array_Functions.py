from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Array Functions").getOrCreate()
data = [
    ("Kishore", ["Python", "Java", "SQL"]),
    ("Kiran", ["Java", "Scala"]),
    ("Ravi", ["Python", "C++", "Java"]),
    ("Anu", ["JavaScript", "HTML", "CSS"]),
    ("Ram", ["Python", "Python", "SQL"])
]

columns = ["Name", "Skills"]
df = spark.createDataFrame(data, columns)
#df.show(truncate=False)

#array()
"""
df_array = df.withColumn("BasicArray", array(lit("A"), lit("B"), lit("C")))
df_array.select("Name", "BasicArray").show()
"""
#array_contains
"""
df.withColumn("Knows_Python", array_contains("Skills", "Python")).show()
"""
#array_length
"""
df.withColumn("Skill_Count", size("Skills")).show()
"""
#array_position
"""
df.withColumn("Python_Position", array_position("Skills", "Python")).show()
"""
#array_remove()
df.withColumn("Without_Python", array_remove("Skills", "Python")).show(truncate=False)


