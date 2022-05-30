import json
import boto3
import csv
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    csv_file = event['Records'][0]['s3']['object']['key']
    csv_object = s3_client.get_object(Bucket = bucket, Key = csv_file)
    print(csv_file)
    file_reader = csv_object['Body'].read().decode("utf-8")
    oscars = file_reader.split("\n")
    oscars = list(filter(None, oscars))
    table = dynamodb.Table('Oscar')
    n = 0
    id = 0
    with open("/tmp/log.txt", "w") as f:
        json.dump(csv_file, f)
    s3_client.upload_file("/tmp/log.txt", "hometaskepam", "example/logs.txt")
    for oscar in oscars:
        if n == 0:
            n = n+1
            continue 
        id = id + 1 
        oscar_data = oscar.split(",")
        print(oscar_data)
        nominee = oscar_data[2]
        full_name = nominee.split()
        award = oscar_data[0].lower()
        movie = '"'+ oscar_data[1]+ '"'
        first_name = full_name[0]
        last_name = full_name[1]
        table.put_item(Item = {
                    "Id": id,
                    "Award": award,
                    "Movie": movie,
                    "First name": first_name,
                    "Last name": last_name 
                }
        )
   
    return "success"