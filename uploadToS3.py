import boto3
from confluent_kafka import Consumer, KafkaError
import json
import os

def kafka_consumer(topic, broker, group_id):
    c = Consumer({
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })
    c.subscribe([topic])
    return c

def upload_to_s3(bucket_name, file_name, data):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(data))

if __name__ == "__main__":
    broker = 'localhost:9092'
    topic = 'stock_data'
    group_id = 'stock_group'
    bucket_name = 'your-s3-bucket-name'

    consumer = kafka_consumer(topic, broker, group_id)
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        data = json.loads(msg.value().decode('utf-8'))
        file_name = f'stock_data/{time.time()}.json'
        upload_to_s3(bucket_name, file_name, data)
    
    consumer.close()
