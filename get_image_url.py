# coding: utf-8
import json

import boto3
from bs4 import BeautifulSoup

sqs_queue_html = boto3.resource('sqs').get_queue_by_name(QueueName='zozo-image-html')
sqs_queue_upload = boto3.resource('sqs').get_queue_by_name(QueueName='zozo-image-upload')
zozo_image_url_table = boto3.resource('dynamodb').Table('zozo-image-url')

client = boto3.client('s3')

message_flg = True
while message_flg:
    try:
        messages = sqs_queue_html.receive_messages(MaxNumberOfMessages=10)
        if messages:
            for message in messages:
                # receive message
                json_data = json.loads(message.body)
                html_file_key = json_data['Records'][0]['s3']['object']['key']

                # download html
                html_body = client.get_object(Bucket='zozo-image-html', Key=html_file_key)['Body'].read().decode(
                    'utf-8')

                # scraping
                soup = BeautifulSoup(html_body, "html.parser")
                li_list = soup.find(id="searchResultList").find_all("li")

                for li in li_list:
                    # get image url ,brand ,sex, category
                    image_url = li.select("div.listInner > div.thumb > a > img")[0].get("data-src")
                    if image_url is None:
                        image_url = li.select("div.listInner > div.thumb > a > img")[0].get("src")
                    brand = li.select("div.listInner > div > p.brand")[0].string
                    sex = html_file_key.split('/')[0]
                    category = html_file_key.split('/')[1]

                    response = zozo_image_url_table.get_item(
                        Key={
                            'image_url': image_url
                        }
                    )
                    if 'Item' in response:
                        continue

                    message_body = {
                        "image_url": image_url,
                        "brand": brand,
                        "sex": sex,
                        "category": category
                    }

                    # send sqs_queue_image
                    sqs_queue_upload.send_message(
                        MessageBody=json.dumps(message_body)
                    )

                message.delete()
        else:
            message_flg = False
    except:
        print 'ERROR'
        message_flg = False

