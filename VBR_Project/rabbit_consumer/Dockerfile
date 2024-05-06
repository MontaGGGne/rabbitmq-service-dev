# FROM rabbitmq:3.12.7 as build

# Set arguments
ARG BASE_CONTAINER=python:3.10
ARG ENV

# Set the base image. 
FROM --platform=linux/amd64 $BASE_CONTAINER

# Adds metadata to image.
LABEL maintainer="MontaGGGne"

USER root
RUN apt-get update && apt-get install -y && apt-get clean
# RUN systemctl start rabbitmq-server
# RUN systemctl enable rabbitmq-server
# RUN rabbitmq-server

COPY for_pip_install/* .
COPY requirements.txt .
COPY rabbit_consumer/* ./workspace/rabbit_consumer/
COPY .env ./workspace/rabbit_consumer/.env

# RUN apt-get update -y && apt-get install python3-pip -y && pip install ./rmq_custom_pack-0.0.1-py3-none-any.whl && pip install -r ./requirements.txt
RUN pip install --upgrade pip && pip install ./rmq_custom_pack-0.0.1-py3-none-any.whl && pip install -r ./requirements.txt
# RUN ["python3", "/workspace/rabbit_consumer_1/rpc_consumer.py"]