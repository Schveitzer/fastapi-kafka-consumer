# FastAPI with Kafka Consumer

This project shows how to use a **Kafka Consumer** inside a Python Web API built using 
**FastAPI**. This can be very useful for use cases where one is building a Web API that 
needs updated by receiving a message from a 
message broker (in this case Kafka).

One example of this could be a Web API, that is recivied user data and process asynchronously, enabling the processing and storage of a amount of information without impacting the user.

![alt text](https://github.com/Schveitzer/fastapi-kafka-consumer/api_diagram.png?raw=true)

## Technologies

The implementation was done in `python>=3.7` using the web framework `fastapi`, and for 
interacting with **Kafka** the `aiokafka` library was chosen. The latter fits very well
within an `async` framework like `fastapi` is.

## How to Run

The first step will be to have **Kafka** broker and zookeeper running, by default the bootstrap server is expected to be running on `localhost:9092`. This can be changed using the 
environment variable `KAFKA_BOOTSTRAP_SERVERS`. 

Next, the following environment variable `KAFKA_TOPIC` should be defined with desired for the topic used to send messages.

```bash
$ export KAFKA_TOPIC=<my_topic>
```

The topic can be created using the command line, or it can be automatically created by 
the consumer inside the Web API.

Start the Web API by running:

```bash
$ python main.py
```

Send a user data using `POST` request on the `/user`, then producer sends data to kfaka topic. One can confirm that the state of the
Web API is being updated with user data by performing a `GET` request on the `/last_user` endpoint.

POST:
```
curl --location --request POST 'http://localhost:8000/user' \
--header 'Content-Type: application/json' \
--data-raw '{
    "data": {
        "Name": "Alan",
        "email": "alan.schveitzer@gmail.com"
    }
}'
```

GET:
```
curl --location --request GET 'http://localhost:8000/last_user'
```

<details>
    <summary>Result</summary>

```
{
    "status": "SUCESS",
    "user": {
        "name": "Alan",
        "email": "mail@gmail.co"
    }
}
```
</details>

## Integration tests
