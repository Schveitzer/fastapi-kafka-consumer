from ast import Set
from asyncio import log
import json
from confluent_kafka import Consumer, TopicPartition

settings = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "group-106",
    "enable.auto.commit": False,
    "session.timeout.ms": 6000,
    "default.topic.config": {"auto.offset.reset": "largest"},
}

consumer = Consumer(settings)

from confluent_kafka import Consumer
from time import sleep

async def get_last_message(consumer):
    # get cluster layout and join group
    await consumer.start()

    partitions: Set[TopicPartition] = consumer.assignment()
    nr_partitions = len(partitions)
    if nr_partitions != 1:
        log.warning(f'Found {nr_partitions} partitions for topic {KAFKA_TOPIC}. Expecting '
                    f'only one, remaining partitions will be ignored!')
    for tp in partitions:

        # get the log_end_offset
        end_offset_dict = await consumer.end_offsets([tp])
        end_offset = end_offset_dict[tp]

        if end_offset == 0:
            log.warning(f'Topic ({KAFKA_TOPIC}) has no messages (log_end_offset: '
                        f'{end_offset}), skipping initialization ...')
            return

        log.debug(f'Found log_end_offset: {end_offset} seeking to {end_offset - 1}')
        consumer.seek(tp, end_offset - 1)
        msg = await consumer.getone()
        log.info(f'Initializing API with data from msg: {msg}')

        # update the API state
        print(msg)
        return msg

# print(json.loads(msg.value()))
