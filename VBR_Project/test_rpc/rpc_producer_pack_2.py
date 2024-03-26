import sys
import os
from rmq_custom_pack import rpc_producer

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
    try:
        main()
    except KeyboardInterrupt:
        print("Request Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)