from operations import SparkConf, SparkContext

conf = SparkConf().setAppName("ManualSC").setMaster("local[*]")

sc = SparkContext(conf=conf)

print(sc.parallelize([1, 2, 3]).collect())

# Stop it when done to avoid resource lock
sc.stop()
