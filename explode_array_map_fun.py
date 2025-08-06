from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode_outer
spark = SparkSession.builder.appName("Explode Functions").getOrCreate()
data = [
    ("Kishore", ["Python", "Java", "SQL"]),
    ("Kiran", ["Java", "Scala"]),
    ("Ravi", None),
    ("Anu", []),
    ("Ram", ["Python", "SQL"])
]
columns = ["Name", "Skills"]
df = spark.createDataFrame(data, columns)
#df.show(truncate=False)

#explode()
"""
df_explode = df.select("Name", explode("Skills").alias("Skill"))
df_explode.show()
"""

#explode_outer
"""
df_outer = df.select("Name", explode_outer("Skills").alias("Skill"))
df_outer.show()
"""
#psoeexplode_outer
df_pos = df.select("Name", posexplode_outer("Skills").alias("Pos", "Skill"))
df_pos.show()
