#FROM ubuntu:latest
#LABEL authors="erikm"
#
#ENTRYPOINT ["top", "-b"]

FROM python:3.13.3-slim
WORKDIR /app

# Install iproute2 to get the 'tc' (Traffic Control) utility
RUN apt-get update && \
    apt-get install -y iproute2 && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1