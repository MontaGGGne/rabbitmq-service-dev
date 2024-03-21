import sys, os, pika, json, logger
import pandas as pd
import time
import boto3
import dagshub
import json
from dagshub import streaming
# from dagshub.streaming import DagsHubFilesystem


def main():
    connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='localhost',
                    port=7801,
                    credentials=pika.PlainCredentials(
                        'rmuser',
                        'rmpassword')))
    
    # connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=7801))
    
    # logger.info('Trying to connect to RabbitMQ')
    # while True:
    #     try:
    #         conn_broker = pika.BlockingConnection(
    #             pika.ConnectionParameters(
    #                 host=conf.rabbit_server,
    #                 port=conf.rabbit_port,
    #                 virtual_host=conf.rabbit_vhost,
    #                 ssl=conf.rabbit_ssl, # do not set it to True if there is no ssl!
    #                 heartbeat_interval=conf.rabbit_heartbeat_interval,
    #                 credentials=pika.PlainCredentials(
    #                     conf.rabbit_user,
    #                     conf.rabbit_pass)))
    #         logger.info('Successfully connected to Rabbit at %s:%s' % (conf.rabbit_server, conf.rabbit_port)) 
    #         channel = conn_broker.channel()
    #         # Don't dispatch a new message to a worker until it has processed and acknowledged the previous one
    #         channel.basic_qos(prefetch_count=conf.rabbit_prefetch_count)
    #         status = channel.queue_declare(queue=conf.rabbit_queue_name,
    #                                         durable=conf.rabbit_queue_durable,
    #                                         exclusive=conf.rabbit_queue_exclusive,
    #                                         passive=conf.rabbit_queue_passive)
    #         if status.method.message_count == 0:
    #             logger.info("Queue empty")
    #         else:
    #             logger.info('Queue status: %s' % status)                  
    #         channel.queue_bind(
    #             queue=conf.rabbit_queue_name,
    #             exchange=conf.rabbit_exchange_name,
    #             routing_key=conf.rabbit_exchange_routing_key)  
    #         return channel
    #     except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError), error:
    #         time.sleep(3)
    #         logger.error('Exception while connecting to Rabbit %s' % error)
    #     else:
    #         break 
    
    channel = connection.channel()  
      
    
    # channel.exchange_declare(exchange=os.getenv("EXCHANGE"), exchange_type='direct', durable=True)

    # channel.queue_declare(queue=os.getenv("QUEUE_REQUEST"), durable=True)
    # channel.queue_bind(exchange=os.getenv("EXCHANGE"), queue=os.getenv("QUEUE_REQUEST"), routing_key=os.getenv("ROUTING_KEY_REQUEST"))

    # channel.queue_declare(queue=os.getenv("QUEUE_RESPONSE"), durable=True)
    # channel.queue_bind(exchange=os.getenv("EXCHANGE"), queue=os.getenv("QUEUE_RESPONSE"), routing_key=os.getenv("ROUTING_KEY_RESPONSE"))
    
    
    channel.exchange_declare(exchange='dataset-reader', exchange_type='direct', durable=True)

    channel.queue_declare(queue='dataset-reader-request', durable=True)
    channel.queue_bind(exchange='dataset-reader', queue='dataset-reader-request', routing_key='request')

    channel.queue_declare(queue='dataset-reader-response', durable=True)
    channel.queue_bind(exchange='dataset-reader', queue='dataset-reader-response', routing_key='response')
    
    
    def callback(ch, method, properties, body):
        print(body)

    channel.basic_consume(queue='dataset-reader-response', on_message_callback=callback)

    #### обработка csv ####
    
    # dagshub_client = dagshub.auth.add_app_token(token='a1482d904ec14cd6e61aa6fcc9df96278dc7c911', host='https://dagshub.com')
    # Get a boto3.client object
    # session = boto3.session.Session()
    # boto_client_object = session.client(
    #     service_name='s3',
    #     endpoint_url='https://dagshub.com/api/v1/repo-buckets/s3/Dimitriy200',
    #     aws_access_key_id = 'a1482d904ec14cd6e61aa6fcc9df96278dc7c911',
    #     aws_secret_access_key = 'a1482d904ec14cd6e61aa6fcc9df96278dc7c911')
    
    # s3 = dagshub.get_repo_bucket_client(boto_client_object)
    
    # fs = streaming.DagsHubFilesystem(
    #     repo_url='https://dagshub.com/Dimitriy200/Data',
    #     token='a1482d904ec14cd6e61aa6fcc9df96278dc7c911')
    
    # dagshub.common.download.Enable_s3_bucket_downloader()
        
    # print("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
    # with open('Data/train_FD001.csv') as f:
    #     print(f.read)
    
    # boto_client_object.download_file(
    #     "Data",  # name of the repo
    #     "train_FD001.csv",  #  remote path from where to download the file
    #     "train_FD001.csv",  # local path where to download the file
    # )
    
    # Download file
    # s3.download_file(
    #     Bucket="Data",  # name of the repo
    #     Key="train_FD001.csv",  #  remote path from where to download the file
    #     Filename="train_FD001.csv",  # local path where to download the file
    # )
    
    # Download file object
    # csv_fileobj = s3.download_fileobj(
    #     Bucket="Data",  # name of the repo
    #     Key="train_FD001.csv",  #  remote path from where to download the file
    #     Filename="train_FD001.csv",  # local path where to download the file
    # )
    
    fs = streaming.DagsHubFilesystem(".", repo_url='https://dagshub.com/Dimitriy200/Data', token='a1482d904ec14cd6e61aa6fcc9df96278dc7c911')
    csv_file_str = fs.http_get('https://dagshub.com/api/v1/repos/Dimitriy200/Data/raw/82fd8214a8769595e670f10ce0c135947bb6638e/test_FD001.csv')
    
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
            
            channel.basic_publish(exchange='dataset-reader',
                                  routing_key='request',
                                  body=json.dumps(dict_from_line_in_csv))
            
        csv_as_list_of_dicts.append(dict_from_line_in_csv)
    
    # full_body = "TestTestTestTestTestTestTestTestTestTest"
    
    #### обработка csv ####
    
    # channel.basic_publish(exchange='dataset-reader',
    #                       routing_key='request', 
    #                       body=full_body)
    
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