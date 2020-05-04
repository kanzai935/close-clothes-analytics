import json
import boto3
from boto3 import Session
from datetime import datetime
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import timedelta

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

s3client = Session().client('s3')
s3 = boto3.resource('s3')
bucket = s3.Bucket('zozo-image-analyze-user-favorite')

now = datetime.now()
target_date = now - timedelta(hours=1)
prefix = target_date.strftime("%Y/%m/%d/%H")

response = s3client.list_objects(
    Bucket='zozo-image-analyze-user-favorite',
    Prefix=prefix
)

if 'Contents' in response:
    keys = [content['Key'] for content in response['Contents']]

timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")

json_file_name = '/home/ec2-user/close-clothes/user-favorite/' + timestamp + '.json'
parquet_file_name = '/home/ec2-user/close-clothes/user-favorite/' + timestamp + '.parquet'

f = open(json_file_name, 'w')
for key in keys:
    obj = bucket.Object(key)
    obj = obj.get()
    json_stream = obj['Body'].read()
    decoder = json.JSONDecoder()

    while len(json_stream) > 0:
        record, index = decoder.raw_decode(json_stream)
        json_stream = json_stream[index:]
        out_put_parquet_list = {}

        dynamodb_record = record['dynamodb']['NewImage']
        new_item_flg = dynamodb_record['flg']['N']
        if new_item_flg == 0:
            continue

        out_put_parquet_list['user_id'] = dynamodb_record['user_id']['S']
        out_put_parquet_list['image_id'] = dynamodb_record['image_id']['S']
        out_put_parquet_list['flg'] = new_item_flg
        out_put_parquet_list['updatetime'] = dynamodb_record['updatetime']['S']
        f.write("\n")
        json.dump(out_put_parquet_list, f)

f.close()

json_data = spark.read.json(json_file_name)
json_data.write.parquet(parquet_file_name)
cmd = 'aws s3 cp ' + parquet_file_name + '/*.parquet s3://zozo-image-analyze-user-favorite-parquet/;'
cmd += 'rm -rf /home/ec2-user/close-clothes/user-favorite/' + timestamp + '.parquet;rm -rf ' + json_file_name
cmd += '; aws s3 rm s3://zozo-image-analyze-user-favorite/' + prefix + ' --recursive'
os.system(cmd)
