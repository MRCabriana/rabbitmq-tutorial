"""Script that sends a message to RabbitMQ."""
import pika

# Establish a connection to RabbitMQ then create a channel
connection_parameters = pika.ConnectionParameters('127.0.0.1', 5672)
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()

# Declare a queue that will store the messages
# Setting the parameter 'auto_delete' to True will delete the queue when the consumer is stopped
QUEUE_NAME = 'hello'
channel.queue_declare(queue=QUEUE_NAME, auto_delete=True)

# Send message with the default exchange ('') routed to the created queue
MESSAGE = 'Hello World!'
channel.basic_publish(
    exchange='',
    routing_key=QUEUE_NAME,
    body=MESSAGE
)
print(f" [x] Sent '{MESSAGE}'")

# Close connection
connection.close()
