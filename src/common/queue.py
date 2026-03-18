from __future__ import annotations

import json
from typing import Iterable

import pika

from common.config import RabbitMQConfig


def open_rabbitmq_connection(config: RabbitMQConfig) -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(config.user, config.password)
    params = pika.ConnectionParameters(
        host=config.host,
        port=config.port,
        credentials=credentials,
    )
    return pika.BlockingConnection(params)


def declare_durable_queue(channel, queue_name: str) -> None:
    channel.queue_declare(queue=queue_name, durable=True)


def publish_json_messages(
    channel,
    *,
    queue_name: str,
    payloads: Iterable[dict],
) -> int:
    published = 0
    for payload in payloads:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),
        )
        published += 1
    return published

