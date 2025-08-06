from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType
spark = SparkSession.builder.appName("UDF Example").getOrCreate()
data = [
    ("Kishore", 70000),
    ("Kiran", 90000),
    ("Ravi", 40000),
    ("Anu", 120000),
    ("Ram", 65000)
]
columns = ["Name", "Salary"]
df = spark.createDataFrame(data, columns)
#df.show()

#step-1
def skill_level(salary):
    if salary >= 100000:
        return "Expert"
    elif salary >= 70000:
        return "Intermediate"
    else:
        return "Beginner"

#step-2
skill_level_udf = udf(skill_level, StringType())


#step-3
df = df.withColumn("SkillLevel", skill_level_udf(df["Salary"]))
df.show()





