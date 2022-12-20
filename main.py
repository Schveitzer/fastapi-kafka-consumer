from typing import Set, Any
from fastapi import FastAPI, Request
from kafka import TopicPartition, errors
from kafka.admin import KafkaAdminClient, NewTopic

import uvicorn
import aiokafka
import asyncio
import json
import logging
import os

# instantiate the API
app = FastAPI()

# global variables
consumer_task = None
consumer = None
producer = None
_last_message = None

# env variables
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'Topic1')
KAFKA_CONSUMER_GROUP_PREFIX = os.getenv('KAFKA_CONSUMER_GROUP_PREFIX', 'group')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# initialize logger
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
log = logging.getLogger(__name__)


@app.on_event("startup")
async def startup_event():
    log.info('Initializing API ...')
    await initialize()
    await init_consume_task()


@app.on_event("shutdown")
async def shutdown_event():
    log.info('Shutting down API')
    consumer_task.cancel()
    await consumer.stop()
    log.warning('Stopping producer')
    await producer.stop()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/last_user")
async def state():
    return {"status": "SUCESS",
            "user": _last_message}


@app.post("/user", status_code=201)
async def create_message(user_data: Request):
    user_data = await user_data.body()
    await send_message(producer, user_data)
    return json.loads(user_data)


async def initialize():

    try:
        # Create topic

        admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
        client_id='test'
        )

        topic_list = []
        topic_list.append(NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except errors.TopicAlreadyExistsError:
        pass

    loop = asyncio.get_event_loop()

    global producer

    producer = aiokafka.AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    global consumer
    group_id = 'group-108'
    log.debug(f'Initializing KafkaConsumer for topic {KAFKA_TOPIC}, group_id {group_id}'
              f' and using bootstrap servers {KAFKA_BOOTSTRAP_SERVERS}')
    consumer = aiokafka.AIOKafkaConsumer(KAFKA_TOPIC, loop=loop,
                                         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                         group_id=group_id)
    await get_last_message(consumer)

    return


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
        _update_api_state(msg)
        return


async def init_consume_task():
    global consumer_task
    consumer_task = asyncio.create_task(consume_message(consumer))


async def consume_message(consumer):
    try:
        # consume messages
        async for msg in consumer:
            # update the API state
            _update_api_state(msg)
    finally:
        # will leave consumer group; perform autocommit if enabled
        log.warning('Stopping consumer')
        await consumer.stop()


async def send_message(producer, user_data):
    # produce message
    log.info(f'Sending message with value: {user_data}')
    await producer.send_and_wait(KAFKA_TOPIC, user_data)


def _update_api_state(message: Any) -> None:
    parsed_message = json.loads(message.value)
    global _last_message
    _last_message = parsed_message


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
