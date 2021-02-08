from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as F

input_bucket = ""
input_path = "unsorted.dat"

app_name = "spark"

conf = SparkConf().setAppName(app_name)
sc = SparkContext()

input_file = sc.binaryRecords(input_bucket+input_path, 100)
print("len first: {}".format(len(input_file.first())))
print("input count: " + str(input_file.count()))

sorted_op = input_file.map(lambda line: (line[:10], line[10:100]))\
    .sortByKey()\
    .map(lambda item: item[0]+item[1])

result = sorted_op.collect()
print("type(result): {}".format(type(result)))
print("len(result): {}".format(len(result)))
print("type(result[0]): {}".format(type(result[0])))
print("len(result[0]): {}".format(len(result[0])))

with open("sorted.dat", "ab") as result_file:
    for output in result:
        result_file.write(output)
# sorted_op.saveAsSequenceFile("sorted.dat")
# sortBy(lambda r: r[0:10])