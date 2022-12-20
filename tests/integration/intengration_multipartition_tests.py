
from ast import Set
from asyncore import loop
import json
from aiokafka import TopicPartition
import aiokafka
import requests
import pytest
from http import HTTPStatus
from kafka import KafkaConsumer
import asyncio

class IntegrationTests:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.base_url = 'http://localhost:8000'
        self.client = requests.Session()
        self.client.headers.update({'Content-Type': 'application/json'})

    async def init_consume_task(self):
        global consumer_task
        consumer_task = await asyncio.create_task(self.get_last_kafka_mesage())

    async def get_last_kafka_mesage(self):

        KAFKA_TOPIC = 'Topic1'
        KAFKA_CONSUMER_GROUP = 'group-106'
        KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

        loop = asyncio.get_event_loop()

        consumer = aiokafka.AIOKafkaConsumer(KAFKA_TOPIC, loop=loop,
                                        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                        group_id=KAFKA_CONSUMER_GROUP)

        # get cluster layout and join group
        await consumer.start()

        try:
        # consume messages
            async for msg in consumer:
                # update the API state
                self._update_last_message(msg)
        finally:
            # will leave consumer group; perform autocommit if enabled
            await consumer.stop()

    def _update_last_message(message):
            parsed_message = json.loads(message.value)
            global _last_message
            _last_message = parsed_message

    @pytest.mark.asyncio
    async def post_new_user_integration_test(self):

        await self.init_consume_task()

        self.endpoint = '/user'

        self.url = str(self.base_url + self.endpoint)

        self.payload = json.dumps({
        "name": "Alan",
        "email": "mail@gmail.alan"
        })

        response = self.client.post(self.url, self.payload)
        response_body = response.json()

        assert response.status_code == HTTPStatus.CREATED
        assert response_body['name'] == 'Alan'
        assert response_body['email'] == 'mail@gmail.alan'

        assert _last_message['name'] == 'Alan'
        assert _last_message['email'] == 'mail@gmail.alan'

        print(_last_message)