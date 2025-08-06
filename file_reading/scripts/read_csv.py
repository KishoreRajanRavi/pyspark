from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
spark = SparkSession.builder.appName("Read with Dynamic Schema").getOrCreate()
# === 1. READ CSV with INFERRED schema ===
df_csv = spark.read.option("header", True).option("inferSchema", True).csv(r"C:\Users\KishoreRajanRavi\Desktop\pyspark\file_reading\data\employee.csv")
print(" CSV Schema (Inferred):")
df_csv.printSchema()
# Extract the inferred schema for reuse
csv_schema: StructType = df_csv.schema
# Read again using the extracted schema (Production style)
df_csv_prod = spark.read.option("header", True).schema(csv_schema).csv(r"C:\Users\KishoreRajanRavi\Desktop\pyspark\file_reading\data\employee.csv")
df_csv_prod.show(5)



