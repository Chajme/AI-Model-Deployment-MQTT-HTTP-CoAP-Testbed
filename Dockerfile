#FROM ubuntu:latest
#LABEL authors="erikm"
#
#ENTRYPOINT ["top", "-b"]

FROM python:3.13.3-slim
WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1