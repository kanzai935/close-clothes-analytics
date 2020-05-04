# coding: utf-8
import json
import boto3
import os

sqs_queue = boto3.resource('sqs').get_queue_by_name(QueueName='zozo-image')

message_flg = True
while message_flg:

    try:
        messages = sqs_queue.receive_messages(MaxNumberOfMessages=10)

        if messages:
            for message in messages:

                # receive message
                image_json_data = json.loads(message.body)
                image_file_name = image_json_data['Records'][0]['s3']['object']['key']

                # make request
                request_str_1 = '''
{
  "requests": [
    {
      "image": {
        "source": {
          "imageUri": "https://s3.amazonaws.com/zozo-image/'''

                request_str_2 = '''
        }
      },
      "features": [
        {
          "type": "IMAGE_PROPERTIES"
        },
        {
          "type": "WEB_DETECTION"
        }
      ]
    }
  ]
}
                '''
                request_str = request_str_1 + image_file_name + '"' + request_str_2

                # write request json

                f = open('/home/ec2-user/close-clothes/request.json', 'w')
                f.write(request_str)
                f.close()

                # send vision api request
                cmd = 'curl -H "Content-Type: application/json" ' \
                      'https://vision.googleapis.com/v1/images:annotate?key=AIzaSyAiUfwHVu1QSJRFfdFdwMjFIojHxtINBm8 ' \
                      '--data-binary @/home/ec2-user/close-clothes/request.json > /home/ec2-user/close-clothes/response.json; ' \
                      'aws s3 cp /home/ec2-user/close-clothes/response.json s3://zozo-image-analyze-json/'
                cmd += image_file_name.split('.')[0]
                cmd += '.json'
                os.system(cmd)

                # remove image
                s3 = boto3.resource('s3')
                s3.Object('zozo-image-analyze', image_file_name).copy_from(CopySource='zozo-image/' + image_file_name)
                s3.Object('zozo-image', image_file_name).delete()

                # delete tmp file
                cmd = 'rm -f /home/ec2-user/close-clothes/request.json; rm -f /home/ec2-user/close-clothes/response.json'
                os.system(cmd)

                #delete message
                message.delete()
        else:
            message_flg = False

    except:
        print 'ERROR'
        message_flg = False

