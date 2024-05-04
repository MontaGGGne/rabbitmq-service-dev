import requests, json, struct, pika, sys, os

# from Slicer_pb2_grpc import SlicerServerStub
# from Slicer_pb2 import SlicerEngineRequest

def main():
    # connection = pika.BlockingConnection(pika.ConnectionParameters(host='host.docker.internal', port=5672))
    
    connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='localhost',
                    port=7801,
                    credentials=pika.PlainCredentials(
                        'rmuser',
                        'rmpassword')))
    
    channel = connection.channel()

    channel.exchange_declare(exchange='dataset-reader', exchange_type='direct', durable=True)

    channel.queue_declare(queue='dataset-reader-request', durable=True)
    channel.queue_bind(exchange='dataset-reader', queue='dataset-reader-request', routing_key='request')

    channel.queue_declare(queue='dataset-reader-response', durable=True)
    channel.queue_bind(exchange='dataset-reader', queue='dataset-reader-response', routing_key='response')

    def callback(ch, method, properties, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        print(body)

        ch.basic_publish(exchange='dataset-reader', 
                        routing_key="response", 
                        body=body)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='dataset-reader-request', on_message_callback=callback)

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