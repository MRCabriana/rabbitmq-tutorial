"""Script that consumes a message from RabbitMQ."""
import sys
import uuid

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties


class FibonacciRpcClient:
    def __init__(self):
        # Establish a connection to RabbitMQ then create a channel
        connection_parameters = pika.ConnectionParameters('127.0.0.1', 5672)
        self.connection = pika.BlockingConnection(connection_parameters)
        self.channel = self.connection.channel()

        # Declare a temporary queue that the consumer will listen for responses
        # Setting the parameter 'exclusive' to True will delete the queue
        # when the consumer that declared the queue is stopped
        self.__result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = self.__result.method.queue

        # Declare message consume in the channel
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response
        )

        self.response = None
        self.corr_id = None

    def on_response(self,
                    channel: BlockingChannel,
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
        # Assign body to response if self.corr_id is equal to the message's correlation_id
        # Acknowledge message if condition is met
        if self.corr_id == properties.correlation_id:
            self.response = body
            channel.basic_ack(delivery_tag=method.delivery_tag)
            # self.channel.stop_consuming()

    def call(self, n_term):
        """Sends a request to fetch the nth term of the fibonacci series.

        Args:
            n_term (int): The nth term of the series.

        Returns:
            int: The value of the nth term of the series.
        """
        self.response = None
        self.corr_id = str(uuid.uuid4())

        # Send request to the default exchange in the rpc_queue
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=BasicProperties(
                correlation_id=self.corr_id,
                reply_to=self.callback_queue
            ),
            body=str(n_term)
        )

        # Await reply
        self.connection.process_data_events(time_limit=None)
        # self.channel.start_consuming()

        return int(self.response)

fibonacci_rpc = FibonacciRpcClient()

N_TERM = sys.argv[1] if len(sys.argv) > 1 else 30
print(f" [x] Requesting fib({N_TERM})")
RESPONSE = fibonacci_rpc.call(N_TERM)
print(f" [.] Got {RESPONSE}")
