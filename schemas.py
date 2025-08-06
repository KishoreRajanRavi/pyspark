
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("Department", StringType(), True)
])


StructField("Age", IntegerType(), True)



