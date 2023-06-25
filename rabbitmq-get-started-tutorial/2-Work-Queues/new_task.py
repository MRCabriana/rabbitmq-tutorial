"""Script that sends a message to RabbitMQ."""
import sys
import time

import pika
from pika import spec

# Establish a connection to RabbitMQ then create a channel
connection_parameters = pika.ConnectionParameters('127.0.0.1', 5672)
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()

# Declare a queue that will store the messages
# Setting the parameter 'durable' to True will keep the queue even when RabbitMQ crashes
QUEUE_NAME = 'task_queue'
channel.queue_declare(queue=QUEUE_NAME, durable=True)

# Send message with the default exchange ('') routed to the created queue
# Set a message property to persist the message when RabbitMQ crashes
MESSAGE = ' '.join(sys.argv[1:]) or "Hello World!"
channel.basic_publish(
    exchange='',
    routing_key=QUEUE_NAME,
    body=MESSAGE,
    properties=pika.BasicProperties(
        delivery_mode=spec.PERSISTENT_DELIVERY_MODE
    )
)
print(f" [x] Sent '{MESSAGE}'")

# Close connection
connection.close()
