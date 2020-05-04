# coding: utf-8
import boto3
import json
import os
from datetime import datetime

sqs_queue = boto3.resource('sqs').get_queue_by_name(QueueName='zozo-image-upload')
sumTable = boto3.resource('dynamodb').Table('zozo-image-summary')
urlTable = boto3.resource('dynamodb').Table('zozo-image-url')

message_flg = True

while message_flg:

    try:
        messages = sqs_queue.receive_messages(MaxNumberOfMessages=10)

        if messages:
            for message in messages:
                # receive message
                image_json_data = json.loads(message.body)
                image_url = image_json_data['image_url']
                image_urls = image_url.split('_')
                image_url = image_urls[0] + '_' + image_urls[1] + '_' + image_urls[2] + '_' + '500.jpg'
                brand = image_json_data['brand']
                sex = image_json_data['sex']
                category = image_json_data['category']

                image_id = datetime.now().strftime("%Y%m%d%H%M%S%f")
                image_file = '/home/ec2-user/close-clothes/images/' + image_id + '.jpg '

                cmd = 'wget -O ' + image_file + ' ' + image_url
                os.system(cmd)

                sumResponse = sumTable.update_item(
                    Key={
                        'id': image_id[0:8],
                        'image_id': image_id
                    },
                    AttributeUpdates={
                        'brand': {
                            'Action': 'PUT',
                            'Value': brand
                        },
                        'sex': {
                            'Action': 'PUT',
                            'Value': sex
                        },
                        'category': {
                            'Action': 'PUT',
                            'Value': category
                        }
                    }
                )

                urlResponse = urlTable.update_item(
                     Key={
                         'image_url': image_url
                     }
                ) 

                cmd = 'aws s3 cp ' + image_file + ' s3://zozo-image/;rm -f ' + image_file
                os.system(cmd)

                message.delete()
        else:
            message_flg = False

    except:
        print 'ERROR'
        message_flg = False



