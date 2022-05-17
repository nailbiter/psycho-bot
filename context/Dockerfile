FROM python:3.9-slim-buster
LABEL maintainer="nailbiter"

RUN apt-get update && apt-get install -y git curl

ENV TZ=Asia/Tokyo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY server.py .

RUN mkdir _common
COPY _common/__init__.py _common

RUN mkdir data
COPY data/psycho.json data

RUN mkdir templates
COPY templates/psycho.jinja.md templates

CMD ["flask","run","--host=0.0.0.0"]
