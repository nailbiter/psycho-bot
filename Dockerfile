FROM python:3.9-slim-buster
LABEL maintainer="nailbiter"

RUN apt-get update && apt-get install -y git curl

ENV TZ=Asia/Tokyo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY client.py .
COPY server.py .
RUN mkdir _common
COPY _common/__init__.py _common

CMD ["false"]
