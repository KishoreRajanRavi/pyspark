import os

# os.environ['PYSPARK_PYTHON'] = 'C:Users/KishoreRajanRavi/Desktop/pyspark/venv/Scripts/python.exe'
# os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:Users/KishoreRajanRavi/Desktop/pyspark/.venv/Scripts/python.exe'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Upsert").getOrCreate()

data = [
    (1,"vegash","Trainee"),
    (2,"Jeeva","Full time")
]
schema = ['id',"name","Role"]

df = spark.createDataFrame(data,schema)
df.show()