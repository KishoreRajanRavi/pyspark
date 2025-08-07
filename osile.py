import os
os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-17"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["JAVA_HOME"], "bin")

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestApp").getOrCreate()
print("SparkSession created successfully!")
spark.stop()
#
# import os
# print(os.environ.get("JAVA_HOME"))
# print(os.system("java -version"))
