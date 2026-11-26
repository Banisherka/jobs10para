import json
import logging
import os
from threading import Event

import pika

TASKS = "tasks"
RESULTS = "results"
log = logging.getLogger(__name__)


def connect():
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=os.environ.get("AMQP_HOST", "rabbitmq"),
        credentials=pika.PlainCredentials(
            os.environ["AMQP_USER"], os.environ["AMQP_PASSWORD"]
        ),
        heartbeat=30,
        socket_timeout=3,
        stack_timeout=5,
        blocked_connection_timeout=5,
    ))


def declare_queues(channel):
    for queue in (TASKS, RESULTS):
        channel.queue_declare(queue=queue, durable=True)


def publish(channel, queue, event_type, message):
    # Канал должен быть в режиме publisher confirms.
    channel.basic_publish(
        exchange="",
        routing_key=queue,
        body=json.dumps(message, ensure_ascii=False).encode("utf-8"),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            message_id=message["id"],
            type=event_type,
        ),
        mandatory=True,
    )


def consume(queue, event_type, fields, handle, stop=None, *, confirms=False):
    stop = stop if stop is not None else Event()

    def on_message(channel, delivery, properties, body):
        try:
            message = json.loads(body)
            if (properties.type != event_type or not isinstance(message, dict)
                    or not all(isinstance(message.get(key), str) for key in fields)):
                raise ValueError("unexpected message schema")
        except ValueError as exc:
            log.warning("Invalid message in %s: %s", queue, exc)
            channel.basic_reject(delivery.delivery_tag, requeue=False)
            return

        handle(channel, message, delivery.redelivered)
        channel.basic_ack(delivery.delivery_tag)

    while not stop.is_set():
        try:
            with connect() as connection:
                channel = connection.channel()
                declare_queues(channel)
                if confirms:
                    channel.confirm_delivery()
                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue, on_message, auto_ack=False)
                log.info("Listening on %s", queue)
                while not stop.is_set() and channel.is_open and channel.consumer_tags:
                    connection.process_data_events(time_limit=1)
        except (pika.exceptions.AMQPError, OSError) as exc:
            # Закрытие соединения возвращает доставку без ack в очередь.
            log.warning("AMQP error on %s: %s", queue, exc)
        stop.wait(1)
