"""Script that sends a message to RabbitMQ."""
import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

# Establish a connection to RabbitMQ then create a channel
connection_parameters = pika.ConnectionParameters('127.0.0.1', 5672)
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()

# Declare the queue that the server reads off requests
# Setting the parameter 'auto_delete' to True will delete the queue when the consumer is stopped
QUEUE_NAME = 'rpc_queue'
channel.queue_declare(queue=QUEUE_NAME, auto_delete=True)

# Declare the fibonacci function
def fib(n_term):
    """Returns the number in the fibonacci series that specified using the 'n' term.

    Args:
        n_term (int): The nth term of the series.

    Returns:
        int: The value of the nth term of the series.
    """
    if n_term == 0:
        return 0
    elif n_term == 1:
        return 1
    else:
        return fib(n_term - 1) + fib(n_term - 2)

# Create a callback that process the messages received from the rpc_queue
def on_request(ch: BlockingChannel,
               method: Basic.Deliver,
               properties: BasicProperties,
               body: bytes):
    """The method that process the messages received from the queue.

    Args:
        ch (pika.adapters.blocking_connection.BlockingChannel): 
        The channel formed from the connection.
        method (spec.Basic.Deliver): The properties of how the message was sent.
        properties (spec.BasicProperties): The properties of the sent message.
        body (bytes): The message received from the queue.
    """
    # Calculate the nth term of the fibonacci series
    n_term = int(body)

    print(f" [.] fib({n_term})")
    response = fib(n_term)

    # Publish response to the default exchange in the indicated callback queue
    # Indicate the correlation_id of the request in the message properties
    ch.basic_publish(
        exchange='',
        routing_key=properties.reply_to,
        properties=BasicProperties(
            correlation_id=properties.correlation_id
        ),
        body=str(response)
    )

    # Acknowledge message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Set a prefetch_count to set the number of messages
# the consumer is expected to consume at a time
channel.basic_qos(prefetch_count=1)

# Declare message consume in the channel
channel.basic_consume(
    queue=QUEUE_NAME,
    on_message_callback=on_request
)

# Start consuming the messages
print(" [x] Awaiting RPC requests")
channel.start_consuming()
