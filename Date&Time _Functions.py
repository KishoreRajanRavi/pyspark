from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date,current_timestamp,date_add,date_diff,year,month,day,to_date,date_format

spark=SparkSession.builder.appName("data and time function").getOrCreate()

data=[
    ('kamal','2025-07-06'),
    ('kevin','2025-06-17'),
    ('Ram','2025-04-22'),
    ('Peter','2025-05-09'),
    ('Raj','2025-07-01')
]

columns = ["Name", "JoinDate"]
df=spark.createDataFrame(data,columns)
#convert string to data type
df=df.withColumn("JoinDate",to_date("JoinDate","yyyy-MM-dd"))
#df.show()

#current_date()
"""
df.withColumn("today",current_date()).show()
"""
#current_time_stamp
"""
df.withColumn("now",current_timestamp()).show()
"""
#date_add()
"""
df.withColumn("dateplus10",date_add("JoinDate",10)).show()
"""
#date_diff()
"""
df.withColumn("data_since_join",date_diff(current_date(),"JoinDate")).show()
"""
#year()
"""
df.withColumn("Joinyear",year("JoinDate")).show()
"""

#month()
"""
df.withColumn("JoinMonth",month("JoinDate")).show()
"""
#day()-dayofmonth()
"""
df.withColumn("Joinday",day("JoinDate")).show()
"""
#date_format()
df.withColumn("Formatted", date_format("JoinDate", "dd-MMM-yyyy")).show()










