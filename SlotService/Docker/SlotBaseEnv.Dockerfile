FROM python:3.12.6-slim

ENV TZ="Asia/Taipei"
COPY Docker/requirements.txt .
RUN pip install --trusted-host=pypi.python.org -r requirements.txt

RUN apt-get update
RUN apt-get update && apt-get install -y vim


