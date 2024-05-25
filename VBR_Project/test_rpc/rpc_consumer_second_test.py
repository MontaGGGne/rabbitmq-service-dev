import requests, json, struct, pika, sys, os, time
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

        current_time = time.time()
        local_time = time.localtime(current_time)
        cur_day = local_time.tm_mday
        cur_mounth = local_time.tm_mon
        cur_year = local_time.tm_year
        cur_hours = local_time.tm_hour
        cur_minuts = local_time.tm_min
        cur_seconds = local_time.tm_sec

        formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time)

        current_dir = os.path.join(SCRIPT_DIR, f"unit_number_{unit_number}")
        time_dir = os.path.join(current_dir, f"{cur_day}-{cur_mounth}-{cur_year}_{cur_hours}-{cur_minuts}")
        if Path(current_dir).exists() is False:
            os.mkdir(current_dir)
            os.mkdir(time_dir)
            tik_time = time.time()
            tik_local_time = time.localtime(tik_time)
            tik_ormatted_time = time.strftime("%Y-%m-%d %H:%M:%S", tik_local_time)
            print(tik_ormatted_time)
            print(time_dir)
            with open(os.path.join(time_dir, f"time_in_cycles_{time_in_cycles}.json"), 'w') as f:
                json.dump(body_dict, f)
        else:
            for dir in os.listdir(current_dir):
                dir_name_as_list = dir.split('_')
                dir_date = dir_name_as_list[0].split('-')
                dir_time = dir_name_as_list[1].split('-')
                if cur_day==int(dir_date[0]) and cur_mounth==int(dir_date[1]) and cur_year==int(dir_date[2]) and cur_hours==int(dir_time[0]) and abs(cur_minuts-int(dir_time[1]))<5:
                    with open(os.path.join(current_dir, dir, f"time_in_cycles_{time_in_cycles}.json"), 'w') as f:
                        json.dump(body_dict, f)
                else:
                    os.mkdir(time_dir)
                    with open(os.path.join(time_dir, f"time_in_cycles_{time_in_cycles}.json"), 'w') as f:
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