import sys, os, pika, json, logger
import pandas as pd
import time
import boto3
import dagshub
import json
import os
from dagshub import streaming

PROD_NUM = 2

HOST = 'localhost'
PORT = 7801
USER = 'rmuser'
PASSWORD = 'rmpassword'

REPO_URL = 'https://dagshub.com/Dimitriy200/diplom_autoencoder'
TOKEN = 'a1482d904ec14cd6e61aa6fcc9df96278dc7c911'
# https://dagshub.com/Dimitriy200/diplom_autoencoder/src/main/data/raw
URL_PATH_STORAGE = 'https://dagshub.com/api/v1/repos/Dimitriy200/diplom_autoencoder/raw/ee325159c4cd9c796be0ea038c9272b8dc10626d/data/raw/'

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
    # https://dagshub.com/api/v1/repos/Dimitriy200/diplom_autoencoder/raw/ee325159c4cd9c796be0ea038c9272b8dc10626d/data/raw/test_FD001.csv
    csv_file_str = fs.http_get(os.path.join(URL_PATH_STORAGE, 'test_FD001.csv'))
    
    csv_as_list_of_dicts = []
    columns_names = []  
    list_csv = []
    list_csv = csv_file_str.text.split('\n')
    columns_names = list_csv[0].split(',')
    list_csv.pop(0)
    for data_line in list_csv:
        if data_line is None or data_line == '':
            continue
        dict_from_line_in_csv = {}
        data_list = data_line.split(',')
        for col_id, col_name in enumerate(columns_names):
            dict_from_line_in_csv[col_name] = data_list[col_id]
            
            channel.basic_publish(exchange=EXCHANGE,
                                  routing_key=ROUTING_KEY_REQUEST,
                                  body=json.dumps(dict_from_line_in_csv))
            
        csv_as_list_of_dicts.append(dict_from_line_in_csv)
    
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