import requests, json, struct, pika, sys, os
from pathlib import Path


PROD_NUM = 1

HOST = 'localhost'
PORT = 5672
USER = 'rmuser'
PASSWORD = 'rmpassword'

EXCHANGE='dataset-reader'
EXCHANGE_TYPE='topic'
QUEUE_REQUEST='dataset-reader-request'
QUEUE_RESPONSE='dataset-reader-response'
ROUTING_KEY_REQUEST='request.*'
ROUTING_KEY_RESPONSE='response.*'

# Получение директории, в которой находится файл скрипта
# SCRIPT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), )
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


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
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        print(body)
        body_dict = {}
        body_dict = json.loads(body)
        unit_number = body_dict["unit number"]
        time_in_cycles = body_dict["time in cycles"]

        current_dir = os.path.join(SCRIPT_DIR, f"unit_number_{unit_number}")
        if Path(current_dir).exists() is False:
            os.mkdir(current_dir)
            with open(os.path.join(current_dir, f"time_in_cycles_{time_in_cycles}.json"), 'w') as f:
                json.dump(body_dict, f)
        else:
            with open(os.path.join(current_dir, f"time_in_cycles_{time_in_cycles}.json"), 'w') as f:
                json.dump(body_dict, f)

        # try:
        #     with open(f"unit_number_{unit_number}.json", 'r') as j:
        #         fcc_data = json.load(j)
        #         with open(f"unit_number_{unit_number}.json", 'w') as f:
        #             dict_with_interval = {f"time_interval_{time_interval}": body_dict}
        #             fcc_data.update(dict_with_interval)
        #             json.dump(fcc_data, f)
        # except:
        #     with open(f"unit_number_{unit_number}.json", 'w') as f:
        #         dict_with_interval = {f"time_interval_{time_interval}": body_dict}
        #         json.dump(dict_with_interval, f)

        # ch.basic_publish(exchange=EXCHANGE, 
        #                 routing_key=ROUTING_KEY_RESPONSE, 
        #                 body=body)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_REQUEST, on_message_callback=callback)

    print('[Server_RabbitMQ] Waiting for messages')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("[Server_RabbitMQ] Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)