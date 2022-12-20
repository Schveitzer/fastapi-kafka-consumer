
import json
import requests
import pytest
from http import HTTPStatus
from confluent_kafka import Consumer, TopicPartition


class IntegrationTests:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.base_url = 'http://localhost:8000'
        self.client = requests.Session()
        self.client.headers.update({'Content-Type': 'application/json'})

    def get_last_kafka_mesage(self):
    
        try:
            self.consumer = Consumer({
                'bootstrap.servers': 'localhost:9092',
                'group.id': 'group-106',
                'auto.offset.reset': 'earliest',
                'session.timeout.ms' : 6000,
                "default.topic.config": {"auto.offset.reset": "largest"},
            })

            def on_assign(a_consumer, partitions):
                # get offset tuple from the first partition
                last_offset = a_consumer.get_watermark_offsets(partitions[0])
                # position [1] being the last index
                partitions[0].offset = last_offset[1] - 1
                self.consumer.assign(partitions)

            self.consumer.subscribe(["Topic1"], on_assign=on_assign)

            self.msg = self.consumer.poll(6.0)

            if self.msg is None:
                pytest.fail("No mensage found on Topic1")
            
            formated_msg = json.loads(self.msg.value())

            self.consumer.close()
            
            return formated_msg
            
        except Exception as err:
            pytest.fail(err.doc)


    def post_new_user_integration_test(self):

        self.endpoint = '/user'

        self.url = str(self.base_url + self.endpoint)

        self.payload = json.dumps({
        "name": "Alan",
        "email": "mail@gmail.test"
        })

        response = self.client.post(self.url, self.payload)
        response_body = response.json()

        assert response.status_code == HTTPStatus.CREATED
        assert response_body['name'] == 'Alan'
        assert response_body['email'] == 'mail@gmail.test'

        self.mesage = self.get_last_kafka_mesage()

        assert self.mesage['name'] == 'Alan'
        assert self.mesage['email'] == 'mail@gmail.test'

        print(self.mesage)