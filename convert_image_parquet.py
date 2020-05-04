# coding: utf-8
import json
import boto3
import sys
import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession.builder.getOrCreate()

sqs_queue = boto3.resource('sqs').get_queue_by_name(QueueName='zozo-image-analyze')
s3 = boto3.client('s3')


def convert_to_parquet(timestamp, convert_file_name, type):
    # convert to parquet

    json_data = spark.read.json('/home/ec2-user/close-clothes/' + convert_file_name)
    json_data.write.parquet('/home/ec2-user/close-clothes/' + timestamp + '.parquet')
    cmd = 'aws s3 cp /home/ec2-user/close-clothes/' + timestamp + '.parquet/*.parquet s3://zozo-image-analyze-parquet/'
    cmd += type + '/;'

    #対応　ここから 2018/10/17 ⇒  2018/10/29 test

    #プログラムを定期実行させる
    #parquet変換の終わったjsonファイルをS3「zozo-image-analyze-json」バケットから削除する

    #cmd += 'rm -rf /home/ec2-user/close-clothes/' + timestamp + '.parquet'
    #os.system(cmd)


    cmd += 'rm -rf /home/ec2-user/close-clothes/' + timestamp + '.parquet'
    cmd += '; aws s3 rm s3://zozo-image-analyze-json/' + timestamp + '.json'
    os.system(cmd)
    #対応　ここまで
    
message_flg = True
while message_flg:

    try:
        messages = sqs_queue.receive_messages(MaxNumberOfMessages=10)

        if messages:
            for message in messages:
                # receive message
                json_data = json.loads(message.body)
                json_file_name = json_data['Records'][0]['s3']['object']['key']
                convert_file_name = 'tmp_' + json_file_name
                timestamp = json_file_name.split('.')[0]

                # get json file from s3
                obj = s3.get_object(Bucket='zozo-image-analyze-json', Key=json_file_name)
                json_image_data = json.loads(obj['Body'].read())
                analyze_data = json_image_data['responses'][0]                
                
                # get color description
                f = open(convert_file_name, 'w')
                colors = analyze_data['imagePropertiesAnnotation']['dominantColors']['colors']
                for color in colors:
                    if "color" in color and "score" in color:
                        outputColorList = {}

                        tmp_color = color["color"]
                        if "red" in tmp_color:
                            outputColorList["red"] = tmp_color['red']
                        else:
                            outputColorList["red"] = 0

                        if "green" in tmp_color:
                            outputColorList["green"] = tmp_color['green']
                        else:
                            outputColorList["green"] = 0

                        if "blue" in tmp_color:
                            outputColorList["blue"] = tmp_color['blue']
                        else:
                            outputColorList["blue"] = 0

                        outputColorList["score"] = color["score"]
                        outputColorList["image_id"] = timestamp
                        json.dump(outputColorList, f)
                        f.write("\n")
                f.close()
                # convert to parquet
                convert_to_parquet(timestamp, convert_file_name, 'color')

                # get web description
                f = open(convert_file_name, 'w')
                webEntities = analyze_data['webDetection']['webEntities']
                for webEntitiy in webEntities:
                    if "description" in webEntitiy and "score" in webEntitiy:
                        outputEntitieList = {}
                        outputEntitieList["image_id"] = timestamp
                        outputEntitieList["description"] = webEntitiy["description"]
                        outputEntitieList["score"] = webEntitiy["score"]
                        json.dump(outputEntitieList, f)
                        f.write("\n")
                f.close()
                # convert to parquet
                convert_to_parquet(timestamp, convert_file_name, 'description')

                # delete message
                message.delete()
        else:
            message_flg = False

    except:
        print 'ERROR'
        message_flg = False
        print sys.exc_info()[1]


