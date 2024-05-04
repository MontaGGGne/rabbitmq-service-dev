import sys
import os
import logging
import traceback
from rmq_custom_pack import rpc_consumer
from dotenv import load_dotenv

print("""Текст песни «Хороший день»]

[Интро]
Какой хороший день, какой хороший день (Ты чувствуешь, сынок?)
Какой хороший день, какой хороший день
Ebanat on the beat

[Припев]
Эй, какой хороший день, чтоб пойти на СВО
Какой хороший день, чтоб разбомбить всех хохлов
Какой хороший день: нацепил Z на авто
Просто съезди на Бахмут, просто съезди на Бахмут
Эй, какой хороший день, чтоб пойти на СВО
Какой хороший день, чтоб разбомбить всех хохлов
Какой хороший день: нацепил Z на авто
Просто съезди на Бахмут, просто съезди на Бахмут

[Куплет]
Фактор, Бахмут, калаши, выстрелы по Киеву
У вас нету ядерки, а также просто выбора
Наше ПВО ебашит, наши воины сильные
Я знаю, что за стены нами будут летом разбомблены
Правда победа в кармане сейчас у нас
Зачем на пары? Пойду я воевать
Вам всем пизда, вагнера уже у вас
У-у-у (Ebanat on the beat)
[Припев]
Эй, какой хороший день, чтоб пойти на СВО
Какой хороший день, чтоб разбомбить всех хохлов
Какой хороший день: нацепил Z на авто
Просто съезди на Бахмут, просто съезди на Бахмут
Эй, какой хороший день, чтоб пойти на СВО
Какой хороший день, чтоб разбомбить всех хохлов
Какой хороший день: нацепил Z на авто
Просто съезди на Бахмут, просто съезди на Бахмут""")

logging.basicConfig(level=logging.INFO, filename="py_log_consumer.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

load_dotenv()

# HOST=os.getenv('HOST')
# PORT=int(os.getenv('PORT'))
# USER=os.getenv('USER')
# PASSWORD=os.getenv('PASSWORD')

HOST=os.environ.get('HOST')
PORT=int(os.environ.get('PORT'))
USER=os.environ.get('USER')
PASSWORD=os.environ.get('PASSWORD')

print(f"[HOST]: {HOST}, [PORT]: {PORT}, [USER]: {USER}, [PASSWORD]: {PASSWORD}")
logging.info(f"[HOST]: {HOST}, [PORT]: {PORT}, [USER]: {USER}, [PASSWORD]: {PASSWORD}")

# EXCHANGE=os.getenv('EXCHANGE')
# EXCHANGE_TYPE=os.getenv('EXCHANGE_TYPE')
# QUEUE_REQUEST=os.getenv('QUEUE_REQUEST')
# QUEUE_RESPONSE=os.getenv('QUEUE_RESPONSE')
# ROUTING_KEY_REQUEST=os.getenv('ROUTING_KEY_REQUEST')
# ROUTING_KEY_RESPONSE=os.getenv('ROUTING_KEY_RESPONSE')

EXCHANGE=os.environ.get('EXCHANGE')
EXCHANGE_TYPE=os.environ.get('EXCHANGE_TYPE')
QUEUE_REQUEST=os.environ.get('QUEUE_REQUEST')
QUEUE_RESPONSE=os.environ.get('QUEUE_RESPONSE')
ROUTING_KEY_REQUEST=os.environ.get('ROUTING_KEY_REQUEST')
ROUTING_KEY_RESPONSE=os.environ.get('ROUTING_KEY_RESPONSE')

print(f"""[EXCHANGE]: {EXCHANGE}, 
             [EXCHANGE_TYPE]: {EXCHANGE_TYPE}, 
             [QUEUE_REQUEST]: {QUEUE_REQUEST}, 
             [QUEUE_RESPONSE]: {QUEUE_RESPONSE}, 
             [ROUTING_KEY_REQUEST]: {ROUTING_KEY_REQUEST}, 
             [ROUTING_KEY_RESPONSE]: {ROUTING_KEY_RESPONSE}""")
logging.info(f"""[EXCHANGE]: {EXCHANGE}, 
             [EXCHANGE_TYPE]: {EXCHANGE_TYPE}, 
             [QUEUE_REQUEST]: {QUEUE_REQUEST}, 
             [QUEUE_RESPONSE]: {QUEUE_RESPONSE}, 
             [ROUTING_KEY_REQUEST]: {ROUTING_KEY_REQUEST}, 
             [ROUTING_KEY_RESPONSE]: {ROUTING_KEY_RESPONSE}""")


def main():
    try:
        print('An instance of the class must be obtained')
        logging.info('An instance of the class must be obtained')
        conn_num = 10
        while conn_num != 0:
            try:
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
                conn_num = 0
                continue
            except:
                conn_num -= 1
        print('Received an instance of the class')
        logging.info('Received an instance of the class')
    except Exception as e:
        print(traceback.format_exc())
        logging.exception(e)
        logging.error(traceback.format_exc())

    try:
        consumer_handler_res = consumer.consumer_handler()
        print(f"consumer_handler_res: {consumer_handler_res['basic_consume_res']}")
        logging.info(f"consumer_handler_res: {consumer_handler_res['basic_consume_res']}")
    except Exception as e:
        print(traceback.format_exc())
        logging.exception(e)
        logging.error(traceback.format_exc())


if __name__ == '__main__':
    main()
