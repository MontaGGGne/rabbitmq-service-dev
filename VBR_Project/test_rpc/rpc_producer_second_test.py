import sys, os, pika, json
import pandas as pd
import time
import boto3
import dagshub
import json
import os
from dagshub import streaming

PROD_NUM = 1

HOST = 'localhost'
PORT = 5672
USER = 'rmuser'
PASSWORD = 'rmpassword'

REPO_URL = 'https://dagshub.com/Dimitriy200/diplom_autoencoder'
TOKEN = 'a1482d904ec14cd6e61aa6fcc9df96278dc7c911'
# https://dagshub.com/Dimitriy200/diplom_autoencoder/src/main/data/raw
URL_PATH_STORAGE = 'https://dagshub.com/api/v1/repos/Dimitriy200/diplom_autoencoder/raw/CURRENT_REVISION/data/raw/'
# URL_PATH_STORAGE = 'https://dagshub.com/api/v1/repos/Dimitriy200/diplom_autoencoder/raw/8c1d92c8f0db4192c94adcc113064ec6ec7280e2/data/raw/train_FD001.csv'

EXCHANGE='dataset-reader'
EXCHANGE_TYPE='topic'
QUEUE_REQUEST='dataset-reader-request'
QUEUE_RESPONSE='dataset-reader-response'
ROUTING_KEY_REQUEST=f'request.{PROD_NUM}'
ROUTING_KEY_RESPONSE=f'response.{PROD_NUM}'


def pika_connection():
    connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=HOST,
                    port=PORT,
                    credentials=pika.PlainCredentials(
                        USER,
                        PASSWORD)))
    return connection


def main():
    connection = pika_connection()
    
    channel = connection.channel()  
    
    channel.exchange_declare(exchange=EXCHANGE, exchange_type=EXCHANGE_TYPE, durable=True)

    channel.queue_declare(queue=QUEUE_REQUEST, durable=True)
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE_REQUEST, routing_key=ROUTING_KEY_REQUEST)

    channel.queue_declare(queue=QUEUE_RESPONSE, durable=True)
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE_RESPONSE, routing_key=ROUTING_KEY_RESPONSE)
    
    
    def callback(ch, method, properties, body):
        print(body)

    channel.basic_consume(queue=QUEUE_RESPONSE, on_message_callback=callback)

    fs = streaming.DagsHubFilesystem(".", repo_url=REPO_URL, token=TOKEN)
    current_revision = fs._current_revision
    url_path_storage = URL_PATH_STORAGE.replace('CURRENT_REVISION', current_revision)
    # https://dagshub.com/api/v1/repos/Dimitriy200/diplom_autoencoder/raw/ee325159c4cd9c796be0ea038c9272b8dc10626d/data/raw/test_FD001.csv
    csv_file_str = fs.http_get(os.path.join(url_path_storage, 'test_FD001.csv'))
    
    
    
    list_csv = csv_file_str.text.split('\n')
    columns_names = list_csv[0].split(',')
    data_list = []
    list_csv.pop(0)
    for data_id, data_line in enumerate(list_csv):
        if data_line is None or data_line == '':
            continue
        obj_with_dicts = {}
        data_list.append(data_line.split(','))
        # data_line_obj_with_dicts = [{col_name: float(col_val)} for col_name, col_val in zip(columns_names, data_line.split(','))]
        [obj_with_dicts.update({col_name: col_val}) for col_name, col_val in zip(columns_names, data_line.split(','))]
        obj_with_dicts.update({'prod num': PROD_NUM})
        obj_with_dicts.update({'time interval': data_id})
        # body=json.dumps({data_id: data_line_list_of_dicts})

        channel.basic_publish(exchange=EXCHANGE,
                                routing_key=ROUTING_KEY_REQUEST,
                                body=json.dumps(obj_with_dicts))

    connection.process_data_events(time_limit=None)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Request Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)