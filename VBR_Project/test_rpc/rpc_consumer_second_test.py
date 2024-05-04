import requests, json, struct, pika, sys, os

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

        ch.basic_publish(exchange=EXCHANGE, 
                        routing_key=ROUTING_KEY_RESPONSE, 
                        body=body)

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