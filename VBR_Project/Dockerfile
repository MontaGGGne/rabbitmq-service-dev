FROM rabbitmq:3.12.7 as build

USER root

COPY ./for_pip_install/* .

RUN apt-get update -y && apt-get install python3-pip -y && pip install ./rmq_custom_pack-0.0.1-py3-none-any.whl