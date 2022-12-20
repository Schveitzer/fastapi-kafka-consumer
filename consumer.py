import json
import threading
from aiokafka import AIOKafkaConsumer
import asyncio

from kafka import KafkaConsumer
import pytest

stop_event = threading.Event()

consumer = KafkaConsumer('Topic1',
                        bootstrap_servers='localhost:9092',
                        auto_offset_reset='latest',
                        enable_auto_commit='False',
                        consumer_timeout_ms=1000)
consumer.subscribe(['Topic1'])

while not stop_event.is_set():
    for message in consumer:
        print(json.loads(message.value))
        if stop_event.is_set():
            break

consumer.close()