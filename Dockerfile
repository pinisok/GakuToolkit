FROM ubuntu:noble
LABEL author="pinisok" maintainer="prain3ps@gmail.com"

# Python
RUN apt update
RUN apt install -y git python3 python3-pip curl
RUN apt install -y unzip
RUN apt clean

# Rclone
RUN curl https://rclone.org/install.sh | bash


# Repo

WORKDIR /app
RUN git clone https://github.com/pinisok/GakuToolkit ./GakuToolkit

WORKDIR /app/GakuToolkit
RUN pip install -r ./requirements.txt --break-system-packages
CMD [ "/bin/bash", "/app/GakuToolkit/docker-entrypoint.sh"]