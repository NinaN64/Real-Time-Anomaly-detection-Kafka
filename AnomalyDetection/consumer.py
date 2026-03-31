import json
import logging
from typing import Callable

from confluent_kafka import Consumer, KafkaException
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger("consumer")


def build_consumer(window_mode: str = config.WINDOW_MODE) -> Consumer:
    topic = (
        config.INPUT_TOPIC_TUMBLING
        if window_mode == "tumbling"
        else config.INPUT_TOPIC_SLIDING
    )

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([topic])
    log.info("Subscribed to topic: %s", topic)
    return consumer


def run(batch_handler: Callable[[dict], None],
        window_mode: str = config.WINDOW_MODE) -> None:
    consumer = build_consumer(window_mode)
    log.info("Consumer started. Waiting for batches...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                batch = json.loads(msg.value().decode("utf-8"))
                doc_count = batch.get("documentCount", 0)
                log.info(
                    "Received batch | window=%s | docs=%d | start=%d",
                    batch.get("windowType"), doc_count, batch.get("windowStart", 0)
                )
                if doc_count > 0:
                    batch_handler(batch)
            except json.JSONDecodeError as e:
                log.warning("Failed to deserialize batch: %s", e)

    except KeyboardInterrupt:
        log.info("Consumer stopped by user.")
    finally:
        consumer.close()
        log.info("Consumer closed.")