import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)

channel = connection.channel()
channel.queue_declare(queue='test_queue', durable=True)

message = {
    "id": 1,
    "text": "Hello"
}

channel.basic_publish(
    exchange='',
    routing_key='test_queue',
    body=json.dumps(message)
)

print("Message sent")

connection.close()