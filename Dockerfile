FROM ubuntu:noble
LABEL author="pinisok" maintainer="prain3ps@gmail.com"

RUN apt update
RUN apt install -y git python3 python3-pip && apt clean

WORKDIR /app
RUN git clone https://github.com/pinisok/GakuToolkit ./GakuToolkit
RUN pip install -r /app/GakuToolkit/requirements.txt --break-system-packages

WORKDIR /app/GakuToolkit
RUN [ "/bin/bash", "/app/GakuToolkit/docker-entrypoint.sh"]