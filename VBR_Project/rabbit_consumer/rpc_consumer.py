import sys
import os
import logging
import traceback
from rmq_custom_pack import rpc_consumer
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(level=logging.INFO, filename=f"py_log_consumer_{os.environ.get('PROD_NUM')}.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
NEW_DIR_TIMEOUT = 5.0 # 6000.0

KEY_ID=os.environ.get('AWS_ACCESS_KEY_ID')
SECRET_KEY=os.environ.get('AWS_SECRET_ACCESS_KEY')
BUCKET_ID=os.environ.get('BUCKET_ID')

HOST=os.environ.get('HOST')
PORT=int(os.environ.get('PORT'))
USER=os.environ.get('USER')
PASSWORD=os.environ.get('PASSWORD')

# print(f"[HOST]: {HOST}, [PORT]: {PORT}, [USER]: {USER}, [PASSWORD]: {PASSWORD}")
logging.info(f"[HOST]: {HOST}, [PORT]: {PORT}, [USER]: {USER}, [PASSWORD]: {PASSWORD}")

EXCHANGE=os.environ.get('EXCHANGE')
EXCHANGE_TYPE=os.environ.get('EXCHANGE_TYPE')
QUEUE_REQUEST=os.environ.get('QUEUE_REQUEST')
QUEUE_RESPONSE=os.environ.get('QUEUE_RESPONSE')
ROUTING_KEY_REQUEST=os.environ.get('ROUTING_KEY_REQUEST')
ROUTING_KEY_RESPONSE=os.environ.get('ROUTING_KEY_RESPONSE')

# print(f"""[EXCHANGE]: {EXCHANGE}, 
#              [EXCHANGE_TYPE]: {EXCHANGE_TYPE}, 
#              [QUEUE_REQUEST]: {QUEUE_REQUEST}, 
#              [QUEUE_RESPONSE]: {QUEUE_RESPONSE}, 
#              [ROUTING_KEY_REQUEST]: {ROUTING_KEY_REQUEST}, 
#              [ROUTING_KEY_RESPONSE]: {ROUTING_KEY_RESPONSE}""")
logging.info(f"""[EXCHANGE]: {EXCHANGE}, 
             [EXCHANGE_TYPE]: {EXCHANGE_TYPE}, 
             [QUEUE_REQUEST]: {QUEUE_REQUEST}, 
             [QUEUE_RESPONSE]: {QUEUE_RESPONSE}, 
             [ROUTING_KEY_REQUEST]: {ROUTING_KEY_REQUEST}, 
             [ROUTING_KEY_RESPONSE]: {ROUTING_KEY_RESPONSE}""")


def main():
    try:
        # print('An instance of the class must be obtained')
        logging.info('An instance of the class must be obtained')
        conn_num = 10
        while conn_num != 0:
            try:
                consumer = rpc_consumer.Consumer(key_id=KEY_ID,
                                                 secret_key=SECRET_KEY,
                                                 host=HOST,
                                                 port=PORT,
                                                 user=USER,
                                                 password=PASSWORD,
                                                 exchange=EXCHANGE,
                                                 exchange_type=EXCHANGE_TYPE,
                                                 queue_request=QUEUE_REQUEST,
                                                 queue_response=QUEUE_RESPONSE,
                                                 r_key_request=ROUTING_KEY_REQUEST,
                                                 r_key_response=ROUTING_KEY_RESPONSE)
                conn_num = 0
                continue
            except:
                conn_num -= 1
        # print('Received an instance of the class')
        logging.info('Received an instance of the class')
    except Exception as e:
        # print(traceback.format_exc())
        logging.exception(e)
        logging.error(traceback.format_exc())

    try:
        consumer_handler_res = consumer.consumer_handler(BUCKET_ID, NEW_DIR_TIMEOUT)
        # print(f"consumer_handler_res: {consumer_handler_res['basic_consume_res']}")
        logging.info(f"consumer_handler_res: {consumer_handler_res['basic_consume_res']}")
    except Exception as e:
        # print(traceback.format_exc())
        logging.exception(e)
        logging.error(traceback.format_exc())


if __name__ == '__main__':
    main()
