import time
import os
import configparser
import logging
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as F
import boto3
from botocore.exceptions import ClientError

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

input_bucket = "s3://unsorted-bucket/"
input_path = "unsorted-mini.dat"
output_bucket_name = "unsorted-bucket"
output_path = "sorted.dat"
# input_bucket = ""
# input_path = "unsorted.dat"

app_name = "spark"

conf = SparkConf().setAppName(app_name)
sc = SparkContext()

# uncomment this block if run local
# aws_profile = 'default'
# config = configparser.ConfigParser()
# config.read(os.path.expanduser("~/.aws/credentials"))
# access_id = config.get(aws_profile, "aws_access_key_id") 
# access_key = config.get(aws_profile, "aws_secret_access_key")
# hadoop_conf = sc._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
# hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
# hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)

input_file = sc.binaryRecords(input_bucket+input_path, 100)
input_file = input_file.map(lambda line: (line[:10], line[10:100]))
input_file = input_file.partitionBy(10)

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(message)s')
logging.info("input file patition num: {}".format(len(input_file.glom().collect())))
logging.debug("len first: {}".format(len(input_file.first())))
logging.debug("type first: {}".format(type(input_file.first())))
logging.debug("len first[0]: {}".format(len(input_file.first()[0])))
logging.debug("type first[0]: {}".format(type(input_file.first()[0])))
logging.info("input count: " + str(input_file.count()))

start = time.time()
# input_file.map(lambda line: (line[:10], line[10:100]))\
sorted_op = input_file.sortByKey()\
    .map(lambda item: item[0]+item[1])
result = sorted_op.collect()
end = time.time()
logging.info("Time elapse in seconds: {}".format(end - start))

logging.debug("type(result): {}".format(type(result)))
logging.debug("len(result): {}".format(len(result)))
logging.debug("type(result[0]): {}".format(type(result[0])))
logging.debug("len(result[0]): {}".format(len(result[0])))


with open(output_path, "wb") as result_file:
    for output in result:
        result_file.write(output)

# write to s3
# with open(output_path, 'rb') as data:
#     s3 = boto3.resource('s3')
#     s3.Bucket(output_bucket_name).put_object(Key=output_path, Body=data)
upload_file(output_path, output_bucket_name)

# sorted_op.saveAsSequenceFile("sorted.dat")
# sortBy(lambda r: r[0:10])
