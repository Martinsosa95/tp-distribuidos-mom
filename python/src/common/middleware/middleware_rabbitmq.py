import time
import pika
from .middleware import (MessageMiddlewareQueue,
                         MessageMiddlewareExchange,
                         MessageMiddlewareDisconnectedError,
                         MessageMiddlewareMessageError,
                         MessageMiddlewareCloseError
)

MAX_RETRIES = 10
QOS_PREFETCH_COUNT = 1


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    """RabbitMQ implementation of MessageMiddlewareQueue.
    Asumo que la concurrencia se maneja con multiprocessing, por lo que no se requieren locks para 
    manejar las conexiones generadas por Pika, dado a que son conexiones que no son thread-safe. """

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.channel = None
        self.connection = None
        self._is_consuming = False
        try:
            self._connect()
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(
                f"Error connecting to RabbitMQ: {e}"
                ) from e


    def _connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def _is_connected(self):
        return (
            self.connection is not None and
            self.channel is not None and
            self.connection.is_open and
            self.channel.is_open
        )

    def start_consuming(self, on_message_callback):
        self._is_consuming = True
        def callback(ch, method, properties, body):
            try: 
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body, ack, nack)
            except Exception:
                nack()
        while self._is_consuming:
            try:
                if not self._is_connected():
                    self._connect()
                self.channel.basic_qos(prefetch_count=QOS_PREFETCH_COUNT)
                #Modificamos la cantidad de mensajes que envia el broker antes de recibir
                #un ack (valor bajo para evitar que sature)

                self.channel.basic_consume(
                    queue=self.queue_name,
                    on_message_callback=callback
                    )
                self.channel.start_consuming()
            except pika.exceptions.AMQPConnectionError:
                time.sleep(2)  # Espera antes de reintentar la conexión
                continue
            except Exception as e:
                raise MessageMiddlewareDisconnectedError(
                    f"Error consuming from RabbitMQ: {e}"
                ) from e

    def stop_consuming(self):
        self._is_consuming = False
        try:
            if self._is_connected():
                self.channel.stop_consuming()
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(
                f"Error stopping consuming from RabbitMQ: {e}"
            ) from e


    def send(self, message):
        retries = 0

        if not isinstance(message, (bytes, str)):
            raise MessageMiddlewareMessageError("Message must be a string or bytes.")

        if isinstance(message, str):
            message = message.encode("utf-8")

        while retries < MAX_RETRIES:
            try:
                if not self._is_connected():
                    self._connect()

                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.queue_name,
                    body=message
                )
                return
            except pika.exceptions.AMQPConnectionError:
                retries += 1
                time.sleep(2 ** retries)
                try:
                    self._connect()
                except Exception :
                    pass #Reintenta en el siguiente ciclo
        raise MessageMiddlewareMessageError(
            f"Error sending message to RabbitMQ Queue after {MAX_RETRIES} retries"
            )


    def close(self):
        if self.channel is not None:
            try:
                if self.channel.is_open:
                    self.channel.close()
            except Exception:
                pass
            finally:
                self.channel = None

        if self.connection is not None:
            try:
                if self.connection.is_open:
                    self.connection.close()
            except Exception as e:
                raise MessageMiddlewareCloseError(
                    f"Error closing connection to RabbitMQ: {e}"
                ) from e
            finally:
                self.connection = None



class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.channel = None
        self.connection = None
        self.temp_queue_name = None
        self._is_consuming = False
        try:
            self._connect()
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(
                f"Error connecting to RabbitMQ: {e}"
            ) from e


    def _connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type='direct'
            )

    def _is_connected(self):
        return (
            self.connection is not None and
            self.channel is not None and
            self.connection.is_open and
            self.channel.is_open
        )

    def start_consuming(self, on_message_callback):
        self._is_consuming = True
        while self._is_consuming:
            try:
                if not self._is_connected():
                    self._connect()

                result = self.channel.queue_declare(queue='', exclusive=True)
                self.temp_queue_name = result.method.queue

                for routing_key in self.routing_keys:
                    self.channel.queue_bind(
                        exchange=self.exchange_name,
                        queue=self.temp_queue_name,
                        routing_key=routing_key
                    )


                def callback(ch, method, properties, body):
                    try:
                        def ack():
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                        def nack():
                            ch.basic_nack(delivery_tag=method.delivery_tag)
                        on_message_callback(body, ack, nack)
                    except Exception:
                        nack()

                self.channel.basic_qos(prefetch_count=QOS_PREFETCH_COUNT)
                self.channel.basic_consume(
                    queue=self.temp_queue_name,
                    on_message_callback=callback
                )

                self.channel.start_consuming()

            except pika.exceptions.AMQPConnectionError:
                time.sleep(2)  # Espera antes de reintentar la conexión
                continue
            except Exception as e:
                raise MessageMiddlewareDisconnectedError(
                    f"Error consuming from RabbitMQ: {e}"
                    ) from e

    def stop_consuming(self):
        self._is_consuming = False
        try:
            if self._is_connected():
                self.channel.stop_consuming()
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(
                f"Error stopping consuming from RabbitMQ: {e}"
                ) from e


    def send(self, message):

        retries = 0
        if not self.routing_keys:
            raise MessageMiddlewareMessageError(
                "No routing keys provided for exchange, cannot send message."
            )
        routing_key = self.routing_keys[0]

        if not isinstance(message, (bytes, str)):
            raise MessageMiddlewareMessageError(
                "Invalid message type. Message must be a string or bytes."
                )

        if isinstance(message, str):
            message = message.encode("utf-8")
        while retries < MAX_RETRIES:
            try:
                if not self._is_connected():
                    self._connect()

                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message
                )
                return
            except pika.exceptions.AMQPConnectionError:
                retries += 1
                time.sleep(2 ** retries)
                try:
                    self._connect()
                except Exception:
                    pass
        raise MessageMiddlewareDisconnectedError(
            f"Error sending message to RabbitMQ Exchange after {MAX_RETRIES} tries."
            )

    def close(self):
        if self.channel is not None:
            try:
                if self.channel.is_open:
                    self.channel.close()
            except Exception:
                pass
            finally:
                self.channel = None

        if self.connection is not None:
            try:
                if self.connection.is_open:
                    self.connection.close()
            except Exception as e:
                raise MessageMiddlewareCloseError(
                    f"Error closing connection to RabbitMQ: {e}"
                    ) from e
            finally:
                self.connection = None
