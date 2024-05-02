FROM rabbitmq:3.12.7 as build

USER root

COPY for_pip_install/* .
COPY rabbit_consumer/* ./workspace/rabbit_consumer_1/

RUN apt-get update -y && apt-get install python3-pip -y && pip install ./rmq_custom_pack-0.0.1-py3-none-any.whl
RUN ["python3", "/workspace/rabbit_consumer_1/rpc_consumer.py"]