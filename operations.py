# Create DataFrame from list
data = [("Kishore", "Sales", 70000),
        ("Ravi", "HR", 60000),
        ("Anu", "IT", 75000)]

columns = ["Name", "Department", "Salary"]

df = spark.createDataFrame(data, columns)

# Operations
df.show()
df.printSchema()
df.filter(df["Salary"] > 65000).show()
