import sys
import os
from rmq_custom_pack import rpc_consumer

HOST=os.getenv('HOST')
PORT=os.getenv('PORT')
USER=os.getenv('USER')
PASSWORD=os.getenv('PASSWORD')

EXCHANGE=os.getenv('EXCHANGE')
EXCHANGE_TYPE=os.getenv('EXCHANGE_TYPE')
QUEUE_REQUEST=os.getenv('QUEUE_REQUEST')
QUEUE_RESPONSE=os.getenv('QUEUE_RESPONSE')
ROUTING_KEY_REQUEST=os.getenv('ROUTING_KEY_REQUEST')
ROUTING_KEY_RESPONSE=os.getenv('ROUTING_KEY_RESPONSE')


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
