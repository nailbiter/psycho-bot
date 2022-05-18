FROM python:3.9-slim-buster
LABEL maintainer="nailbiter"

RUN apt-get update && apt-get install -y git curl

ENV TZ=Asia/Tokyo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY src/server.py .

RUN mkdir _common
COPY src/_common/__init__.py _common
COPY src/_common/base.py _common

RUN mkdir data
COPY src/data/psycho.json data

RUN mkdir templates
COPY src/templates/psycho.jinja.md templates

CMD ["flask","run","--host=0.0.0.0"]
