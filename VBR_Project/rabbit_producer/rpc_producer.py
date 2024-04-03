import sys
import os
from rmq_custom_pack import rpc_producer

PROD_NUM=os.getenv('PROD_NUM')

HOST=os.getenv('HOST')
PORT=os.getenv('PORT')
USER=os.getenv('USER')
PASSWORD=os.getenv('PASSWORD')

REPO_URL=os.getenv('REPO_URL')
TOKEN=os.getenv('TOKEN')
URL_PATH_STORAGE=os.getenv('URL_PATH_STORAGE')

EXCHANGE=os.getenv('EXCHANGE')
EXCHANGE_TYPE=os.getenv('EXCHANGE_TYPE')
QUEUE_REQUEST=os.getenv('QUEUE_REQUEST')
QUEUE_RESPONSE=os.getenv('QUEUE_RESPONSE')
ROUTING_KEY_REQUEST=f'{os.getenv('ROUTING_KEY_REQUEST')}{PROD_NUM}'
ROUTING_KEY_RESPONSE=f'{os.getenv('ROUTING_KEY_RESPONSE')}{PROD_NUM}'


def main():
    producer = rpc_producer.Producer(host=HOST,
                            port=PORT,
                            user=USER,
                            password=PASSWORD,
                            exchange=EXCHANGE,
                            exchange_type=EXCHANGE_TYPE,
                            queue_request=QUEUE_REQUEST,
                            queue_response=QUEUE_RESPONSE,
                            r_key_request=ROUTING_KEY_REQUEST,
                            r_key_response=ROUTING_KEY_RESPONSE)
    producer.producer_handler(repo_url=REPO_URL, token=TOKEN, url_path_storage=URL_PATH_STORAGE)


if __name__ == '__main__':
    main()