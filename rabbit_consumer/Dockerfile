# Set arguments
ARG BASE_CONTAINER=python:3.10
ARG ENV
# Set the base image. 
FROM --platform=linux/amd64 $BASE_CONTAINER
# Adds metadata to image.
LABEL maintainer="MontaGGGne"

USER root
RUN apt-get update && apt-get install -y && apt-get clean

COPY for_pip_install/* .
COPY requirements.txt .
COPY rabbit_consumer/* ./workspace/rabbit_consumer/

RUN pip install --upgrade pip && pip install ./rmq_custom_pack-0.0.1-py3-none-any.whl && pip install -r ./requirements.txt