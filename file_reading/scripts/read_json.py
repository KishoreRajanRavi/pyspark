from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# Step 1: Create Spark session
spark = SparkSession.builder.appName("Read JSON with Schema").getOrCreate()

# Step 2: Read JSON with multiline enabled and infer schema
df_json = spark.read.option("multiline", True).json(
    r"C:\Users\KishoreRajanRavi\Desktop\pyspark\file_reading\data\employees.json"
)

# Step 3: Print schema and preview data
print(" JSON Schema (Inferred):")
df_json.printSchema()
print("JSON Data Preview:")
df_json.show(truncate=False)

# Step 4: Extract schema for reuse
json_schema: StructType = df_json.schema


