"""Script that sends a message to RabbitMQ."""
import sys

import pika
from pika.exchange_type import ExchangeType

# Establish a connection to RabbitMQ then create a channel
connection_parameters = pika.ConnectionParameters('127.0.0.1', 5672)
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()

# Declare the exchange that delivers our messages to the queues
# Setting the parameter 'auto_delete' to True will delete the exchange when all queues are unbounded
EXCHANGE_NAME = 'logs'
channel.exchange_declare(
    exchange=EXCHANGE_NAME,
    exchange_type=ExchangeType.fanout,
    auto_delete=True
)

# Send message to all binded queues in the exchange (set routing key to '')
MESSAGE = ' '.join(sys.argv[1:]) or "info: Hello World!"
channel.basic_publish(
    exchange=EXCHANGE_NAME,
    routing_key='',
    body=MESSAGE
)
print(f" [x] Sent '{MESSAGE}'")

# Close connection
connection.close()
