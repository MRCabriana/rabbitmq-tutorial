"""Script that consumes a message from RabbitMQ."""
import sys
import os

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

def main():
    """Starts program."""
    # Establish a connection to RabbitMQ then create a channel
    connection_parameters = pika.ConnectionParameters('127.0.0.1', 5672)
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()

    # Declare a queue that the consumer will listen for messages
    # Setting the parameter 'auto_delete' to True will delete the queue when the consumer is stopped
    queue_name = 'hello'
    channel.queue_declare(queue=queue_name, auto_delete=True)

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
        print(f" [x] Received {body}")

    # Declare message consume in the channel
    channel.basic_consume(
        queue=queue_name,
        auto_ack=True,
        on_message_callback=callback
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
