from pyspark.sql import SparkSession

# Create SparkSession directly in this file
spark = SparkSession.builder \
    .appName("spark_sql") \
    .getOrCreate()

# Sample data
data = [("Kishore", "Sales", 70000), ("Ravi", "HR", 50000)]
columns = ["Name", "Department", "Salary"]


# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

#register as a temp view table
df.createOrReplaceTempView('employees')

#sql
#result=spark.sql("select  name,salary from employees where salary>60000")
result = spark.sql("SELECT Name, Salary FROM employees WHERE Salary > 60000")
result.show()
