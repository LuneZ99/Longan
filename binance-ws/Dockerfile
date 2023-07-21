FROM python:3.9.16-slim

WORKDIR /app

COPY . /app

RUN chmod -R 0755 /app

RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

