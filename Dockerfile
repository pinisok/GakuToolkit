FROM ubuntu:noble
LABEL author="pinisok" maintainer="prain3ps@gmail.com"

RUN apt update
RUN apt install -y git python3 python3-pip && apt clean
RUN curl https://rclone.org/install.sh | bash

WORKDIR /app
RUN git clone https://github.com/pinisok/GakuToolkit ./GakuToolkit

WORKDIR /app/GakuToolkit
RUN pip install -r ./requirements.txt --break-system-packages
CMD [ "/bin/bash", "/app/GakuToolkit/docker-entrypoint.sh"]