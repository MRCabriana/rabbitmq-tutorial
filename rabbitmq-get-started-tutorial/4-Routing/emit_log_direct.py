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
EXCHANGE_NAME = 'direct_logs'
channel.exchange_declare(
    exchange=EXCHANGE_NAME,
    exchange_type=ExchangeType.direct,
    auto_delete=True
)

# Send message to all binded queues in the exchange (set routing key to '')
SEVERITY = sys.argv[1] if len(sys.argv) > 2 else 'info'
MESSAGE = ' '.join(sys.argv[2:]) or 'Hello World!'
channel.basic_publish(
    exchange=EXCHANGE_NAME,
    routing_key=SEVERITY,
    body=MESSAGE
)
print(f" [x] Sent {SEVERITY}:{MESSAGE}")

# Close connection
connection.close()
