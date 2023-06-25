"""Script that consumes a message from RabbitMQ."""
import sys
import os

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from pika.exchange_type import ExchangeType

def main():
    """Starts program."""
    # Establish a connection to RabbitMQ then create a channel
    connection_parameters = pika.ConnectionParameters('127.0.0.1', 5672)
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()

    # Declare the exchange that delivers our messages to the queues
    # Setting the parameter 'auto_delete' to True will delete the exchange
    # when all queues are unbounded
    exchange_name = 'direct_logs'
    channel.exchange_declare(
        exchange=exchange_name,
        exchange_type=ExchangeType.direct,
        auto_delete=True
    )

    # Declare a temporary queue that the consumer will listen for messages
    # Setting the parameter 'exclusive' to True will delete the queue
    # when the consumer that declared the queue is stopped
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    # Retrieve routing keys
    severities = sys.argv[1:]
    if not severities:
        sys.stderr.write(f"Usage: {sys.argv[0]} [info] [warning] [error]\n")
        sys.exit(1)

    # Bind the queues to the declared exchange with the retrieved routing keys
    for severity in severities:
        channel.queue_bind(
            queue=queue_name,
            exchange=exchange_name,
            routing_key=severity
        )

    # Create a callback that process the messages received from the queue
    def callback(channel: BlockingChannel,
                 method: Basic.Deliver,
                 properties: BasicProperties,
                 body: bytes):
        """The method that process the messages received from the queue.

        Args:
            channel (pika.adapters.blocking_connection.BlockingChannel): 
            The channel formed from the connection.
            method (spec.Basic.Deliver): The properties of how the message was sent.
            properties (spec.BasicProperties): The properties of the sent message.
            body (bytes): The message received from the queue.
        """
        print(f" [x] {method.routing_key}:{body}")

    # Declare message consume in the channel
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True
    )

    # Start consuming the messages
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
