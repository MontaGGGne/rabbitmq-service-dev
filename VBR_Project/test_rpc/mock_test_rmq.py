import pika 
# from amqp_mock import
 
# Create a mock connection to the RabbitMQ server 
connection = MockConnection() 
 
# Create a channel on the connection 
channel = connection.channel() 
 
# Declare a queue on the channel 
channel.queue_declare(queue='test_queue') 
 
# Publish a message to the queue 
channel.basic_publish(exchange='', routing_key='test_queue', body='Hello, world!') 
 
# Consume a message from the queue 
method, properties, body = channel.basic_get(queue='test_queue', auto_ack=True) 
print(body)  # Output: b'Hello, world!' 