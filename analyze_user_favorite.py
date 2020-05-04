# coding: utf-8
import csv
import os
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
from datetime import timedelta

import boto3

athena = boto3.client('athena', region_name='us-east-1')

s3 = boto3.resource('s3')
bucket_name = 'zozo-image-analyze-user-favorite-summary'
user_favorite_table = boto3.resource('dynamodb').Table('zozo-image-user-timeline')
user_table = boto3.resource('dynamodb').Table('zozo-image-user')
image_table = boto3.resource('dynamodb').Table('zozo-image-summary')

now = datetime.now()
now_str = now.strftime("%Y%m%d%H%M%S%f")
target_date = now - timedelta(hours=1)
target_date_str = target_date.strftime("%Y%m%d%H%M%S%f")

def start_query_execution(sql):
    global athena
    global bucket_nam

    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            'Database': 'zozoimage'
        },
        ResultConfiguration={
            'OutputLocation': 's3://zozo-image-analyze-user-favorite-summary'
        }
    )
    return response


def get_query_execution(query_execution_id):
    global athena

    status = True
    while status:
        response = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        if response['QueryExecution']['Status']['State'] == 'SUCCEEDED':
            status = False


def get_image_id(query_execution_id):
    global bucket_name
    global s3

    bucket = s3.Bucket(bucket_name)
    bucket.download_file(query_execution_id + '.csv', '/home/ec2-user/close-clothes/summary/' + query_execution_id + '.csv')
    f = open('/home/ec2-user/close-clothes/summary/' + query_execution_id + '.csv', 'r')
    reader = csv.reader(f)
    next(reader)

    image_ids = []
    for row in reader:
        image_ids.append(row[0])

    f.close()
    return image_ids


def update_timeline(user_id, records):
    global now_str
    global user_favorite_table

    for record in records:
        response = user_favorite_table.update_item(
            Key={
                'user_id': user_id,
                'image_id': record
            }
        )


response = user_table.scan()
if 'Items' in response:
    users = response['Items']
else:
    exit()

for user in users:
    user_id = user['user_id']
    user_id_sql = "AND user_favorite.user_id = '" + user_id + "'"
    # 衣服の模様の特徴を抽出
    sql = "SELECT description.image_id FROM description WHERE description.description IN (SELECT description.description FROM user_favorite INNER JOIN description ON user_favorite.image_id = description.image_id WHERE user_favorite.flg = '1' "
    sql += user_id_sql
    sql += ' GROUP BY  description.description, user_favorite.user_id) GROUP BY  description.image_id'
    response = start_query_execution(sql)
    query_execution_id = response['QueryExecutionId']
    get_query_execution(query_execution_id)
    response = get_image_id(query_execution_id)
    # タイムラインに載せるべき対象画像が空の時は、新しく取ってきた画像を全部タイムラインに載せる
    if not response:
        image_ids_timeline = image_table.query(
            KeyConditionExpression=Key('id').eq(now_str[0:8]) & Key('image_id').gt(target_date_str),
            ScanIndexForward=False
        )
        if 'Items' in image_ids_timeline:
            items = image_ids_timeline['Items']
            for item in items:
                response = user_favorite_table.update_item(
                    Key={
                        'user_id': user_id,
                        'image_id': item['image_id']
                    }
                )
    else:
        update_timeline(user_id, response)

    # 衣服の色の特徴を抽出
    sql = "SELECT image_id FROM color WHERE (red BETWEEN (SELECT (avg(color.red) - 30 ) FROM user_favorite INNER JOIN color ON user_favorite.image_id = color.image_id WHERE user_favorite.flg = '1' "
    sql += user_id_sql
    sql += ")AND (SELECT (avg(color.red) + 30 ) FROM user_favorite INNER JOIN color ON user_favorite.image_id = color.image_id WHERE user_favorite.flg = '1' "
    sql += user_id_sql
    sql += "))AND (blue BETWEEN (SELECT (avg(color.blue) - 30 ) FROM user_favorite INNER JOIN color ON user_favorite.image_id = color.image_id WHERE user_favorite.flg = '1' "
    sql += user_id_sql
    sql += ")AND (SELECT (avg(color.blue) + 30 ) FROM user_favorite INNER JOIN color ON user_favorite.image_id = color.image_id WHERE user_favorite.flg = '1' "
    sql += user_id_sql
    sql += "))AND (green BETWEEN (SELECT (avg(color.green) - 30 ) FROM user_favorite INNER JOIN color ON user_favorite.image_id = color.image_id WHERE user_favorite.flg = '1' "
    sql += user_id_sql
    sql += ")AND (SELECT (avg(color.green) + 30 ) FROM user_favorite INNER JOIN color ON user_favorite.image_id = color.image_id WHERE user_favorite.flg = '1' "
    sql += user_id_sql
    sql += ")) GROUP BY  color.image_id"

    response = start_query_execution(sql)
    query_execution_id = response['QueryExecutionId']
    get_query_execution(query_execution_id)
    response = get_image_id(query_execution_id)

    # タイムラインに載せるべき対象画像が空の時は、新しく取ってきた画像を全部タイムラインに載せる
    if not response:
        image_ids_timeline = image_table.query(
            KeyConditionExpression=Key('id').eq(now_str[0:8]) & Key('image_id').gt(target_date_str),
            ScanIndexForward=False
        )
        if 'Items' in image_ids_timeline:
            items = image_ids_timeline['Items']
            for item in items:
                response = user_favorite_table.update_item(
                    Key={
                        'user_id': user_id,
                        'image_id': item['image_id']
                    }
                )
    else:
        update_timeline(user_id, response)

cmd = 'rm -f /home/ec2-user/close-clothes/summary/*.csv'
os.system(cmd)
