FROM rabbitmq:3.12.7 as build

USER root

RUN apt-get update -y && apt-get install python3-pip -y && pip install requests numpy boto3 pika logger pandas --upgrade

ENV EXCHANGE=dataset-reader
ENV QUEUE_REQUEST=dataset-reader-request
ENV QUEUE_RESPONSE=dataset-reader-response
ENV ROUTING_KEY_REQUEST=request
ENV ROUTING_KEY_RESPONSE=response

WORKDIR /workspace/rabbit_producer

# COPY rpc_producer.py .
# EXPOSE 5672

# ADD . .

# CMD python rpc_producer.py