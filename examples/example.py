import json
import os
import time
from diaspora_event_sdk import Client as GlobusClient
from diaspora_stream.api import Driver

c = GlobusClient()
key_result = c.create_key()
region = "us-east-1"
subject = c.subject_openid
authorization = c.web_client.authorizer.get_authorization_header()
namespace = f"ns-{subject.replace('-', '')[-12:]}"

os.environ["AWS_REGION"] = region
os.environ["AWS_SECRET_ACCESS_KEY"] = key_result["secret_key"]
os.environ["AWS_ACCESS_KEY_ID"] = key_result["access_key"]
os.environ["OCTOPUS_SUBJECT"] = subject
os.environ["OCTOPUS_AUTHORIZATION"] = authorization

driver_config = {
    "kafka": {
        "bootstrap.servers": key_result["endpoint"].split(",")
    },
    "aws_msk_iam": {
        "region": region
    },
    "octopus": {
        "subject_env": "OCTOPUS_SUBJECT",
        "authorization_env": "OCTOPUS_AUTHORIZATION"
    },
    "namespace": namespace
}

driver = Driver(backend="octopus",
                options=driver_config)

topic_name = f"test-topic-{int(time.time())}"

if not driver.topic_exists(topic_name):
    driver.create_topic(name=topic_name)
    print(f"Created topic '{topic_name}'")

assert driver.topic_exists(topic_name), f"Topic '{topic_name}' should exist after creation"
print(f"Topic '{topic_name}' exists")

time.sleep(10)

topic = driver.open_topic(topic_name)
print(f"Opened topic '{topic_name}'")

producer = topic.producer("test-producer")
num_events = 5
for i in range(num_events):
    producer.push({"index": i, "greeting": f"hello {i}"}).wait(timeout_ms=10000)
producer.flush().wait(timeout_ms=10000)
print(f"Produced {num_events} events")

# Use pull() with retry instead of iterator, because the consumer group
# rebalance with MSK IAM auth can take longer than the iterator's 5s timeout.
consumer = topic.consumer("test-consumer")
received = 0
for i in range(num_events):
    future_event = consumer.pull()
    event = None
    start = time.time()
    while event is None:
        event = future_event.wait(timeout_ms=1000)
        if time.time() - start > 60:
            break
    if event is None:
        print(f"  Timed out waiting for event {i}")
        break
    if event.event_id is None:
        break
    print(f"  event {received}: metadata={event.metadata}")
    event.acknowledge()
    received += 1
print(f"Consumed {received}/{num_events} events")
