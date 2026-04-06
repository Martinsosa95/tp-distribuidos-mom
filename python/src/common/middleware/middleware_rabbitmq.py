import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareCloseError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Error connecting to RabbitMQ: {e}")


    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            try:
                ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
                nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body, ack, nack)
            except Exception as e:
                raise MessageMiddlewareMessageError(f"Error processing message from RabbitMQ: {e}")
            
            try:
                self.channel.basic_consume(
                    queue=self.queue_name,
                    on_message_callback=callback
                )
            except Exception as e:
                raise MessageMiddlewareDisconnectedError(f"Error consuming from RabbitMQ: {e}")

    def stop_consuming(self):
        try:
            self.channel.stop_consumming()
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Error stopping consuming from RabbitMQ: {e}")


    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='', 
                routing_key=self.queue_name, 
                body=message
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error sending message to RabbitMQ: {e}")


    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error closing connection to RabbitMQ: {e}")


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass

	def start_consuming(self, on_message_callback):
		pass
	

	def stop_consuming(self):
		pass
	

	def send(self, message):
		pass

	def close(self):
		pass