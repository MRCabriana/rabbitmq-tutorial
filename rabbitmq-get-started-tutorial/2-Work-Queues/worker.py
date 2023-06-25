"""Script that consumes a message from RabbitMQ."""
import sys
import os
import time

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
    # Setting the parameter 'durable' to True will keep the queue even when RabbitMQ crashes
    queue_name = 'task_queue'
    channel.queue_declare(queue=queue_name, durable=True)

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
        print(f" [x] Received {body.decode()}")
        time.sleep(body.count(b'.'))
        print(" [x] Done")

        # Acknowledge message
        channel.basic_ack(delivery_tag=method.delivery_tag)

    # Set a prefetch_count to set the number of messages
    # the consumer is expected to consume at a time
    channel.basic_qos(prefetch_count=1)

    # Declare message consume in the channel
    channel.basic_consume(
        queue=queue_name,
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
