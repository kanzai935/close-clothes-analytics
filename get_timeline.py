# coding: utf-8
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

user_table = boto3.resource('dynamodb').Table('zozo-image-user')
user_favorite_table = boto3.resource('dynamodb').Table('zozo-image-user-favorite')
image_table = boto3.resource('dynamodb').Table('zozo-image-summary')

now_str = datetime.now().strftime("%Y%m%d%H%M%S%f")
image_id = '20180512051728026580'

response = image_table.query(
    KeyConditionExpression=Key('id').eq(image_id[0:8]) & Key('image_id').gt(image_id),
    ScanIndexForward=False
)
if 'Items' in response:
    itmes = response['Items']
else:
    exit()

response = user_table.scan()
if 'Items' in response:
    users = response['Items']
else:
    exit()

for user in users:
    user_id = user['user_id']
    for item in itmes:
        response = user_favorite_table.update_item(
            Key={
                'user_id': user_id,
                'image_id': item['image_id']
            },
            AttributeUpdates={
                'flg': {
                    'Action': 'PUT',
                    'Value': 0
                },
                'updatetime': {
                    'Action': 'PUT',
                    'Value': now_str
                }
            }
        )

