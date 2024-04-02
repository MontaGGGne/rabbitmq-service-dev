import sys
import os
from rmq_custom_pack import rpc_consumer

HOST = 'localhost'
PORT = 7801
USER = 'rmuser'
PASSWORD = 'rmpassword'

EXCHANGE='dataset-reader'
EXCHANGE_TYPE='topic'
QUEUE_REQUEST='dataset-reader-request'
QUEUE_RESPONSE='dataset-reader-response'
ROUTING_KEY_REQUEST='request.*'
ROUTING_KEY_RESPONSE='response.*'


def main():
    consumer = rpc_consumer.Consumer(host=HOST,
                            port=PORT,
                            user=USER,
                            password=PASSWORD,
                            exchange=EXCHANGE,
                            exchange_type=EXCHANGE_TYPE,
                            queue_request=QUEUE_REQUEST,
                            queue_response=QUEUE_RESPONSE,
                            r_key_request=ROUTING_KEY_REQUEST,
                            r_key_response=ROUTING_KEY_RESPONSE)
    consumer.consumer_handler()


if __name__ == '__main__':
    main()
