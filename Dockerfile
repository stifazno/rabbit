FROM python:3.11

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir fastapi uvicorn pika requests

CMD ["python", "main.py"]