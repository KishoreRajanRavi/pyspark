from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Join Examples").getOrCreate()
# Employee data
emp_data = [
    (1, "Kishore", 101),
    (2, "Kiran", 102),
    (3, "Anu", 103),
    (4, "Ravi", 105),
    (5, "Mithun", None)
]
emp_cols = ["EmpID", "Name", "DeptID"]
df1 = spark.createDataFrame(emp_data, emp_cols)

# Department data
dept_data = [
    (101, "Sales"),
    (102, "HR"),
    (103, "IT"),
    (104, "Marketing")
]
dept_cols = ["DeptID", "DeptName"]
df2 = spark.createDataFrame(dept_data, dept_cols)
"""
df1.show()
df2.show()
"""
#innerjoin
"""
df1.join(df2,on="DeptID",how="inner").show()
"""
#crossjoin
"""
df1.crossJoin(df2).show()
"""
#outer
"""
df1.join(df2,on="DeptID",how="outer").show()
"""
#left outer join
"""
df1.join(df2, on="DeptID", how="left").show()
"""
#right outer join
"""
df1.join(df2, on="DeptID", how="right").show()
"""
#leftsemijoin
"""
df1.join(df2, on="DeptID", how="left_semi").show()
"""
#leftantijoin
df1.join(df2, on="DeptID", how="left_anti").show()
