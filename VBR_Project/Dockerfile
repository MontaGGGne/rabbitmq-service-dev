FROM rabbitmq:3.12.7 as build

USER root

RUN apt-get update -y && apt-get upgrade -y && apt-get install python3-pip -y && pip install requests numpy boto3 pika logger pandas --upgrade

WORKDIR /workspace


# EXPOSE 5672

# ADD . .

# CMD python rpc_producer.py