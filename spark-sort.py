from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as F

input_bucket = ""
input_path = "unsorted.dat"

app_name = "spark"

conf = SparkConf().setAppName(app_name)
sc = SparkContext()

input_file = sc.binaryFiles(input_bucket+input_path)
print("input count: " + str(input_file.count()))
# input_file.map(lambda content : content[i: i+100] for i in range(1000000))
# input_file = input_file.flatMap(lambda line: line.split("\n"))
# print("input count: " + str(input_file.count()))
# records = []
# with open("unsorted.dat", "rb") as f:
#   r = f.read(100)
#   while len(r) == 100:
#     records.append(r)
#     r = f.read(100)

sorted_op = input_file.flatMap(lambda line: line.split(10))\
    .map(lambda line: (line[:10], line[10:100]))\
    .sortByKey()\
    .map(lambda key, value: key+value+" ")

sorted_op.saveAsTextFile("sorted.data")
# sortBy(lambda r: r[0:10])