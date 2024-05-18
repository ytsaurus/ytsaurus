from yt_env_setup import YTEnvSetup

from yt_queue_agent_test_base import (
    TestQueueAgentBase, ReplicatedObjectBase)

from yt_commands import (
    authors, get, create, sync_mount_table, insert_rows, sync_create_cells)

from confluent_kafka import (
    Consumer, TopicPartition)

import functools
import time

##################################################################


def _get_token(value, config):
    return "some_token_123", time.time() + 3600


class TestKafkaProxy(TestQueueAgentBase, ReplicatedObjectBase, YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1
    NUM_KAFKA_PROXIES = 1

    DEFAULT_QUEUE_SCHEMA = [
        {"name": "$timestamp", "type": "uint64"},
        {"name": "$cumulative_data_weight", "type": "int64"},
        {"name": "surname", "type": "string"},
        {"name": "number", "type": "int64"},
    ]

    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "queue_agent": {
            "controller": {
                "enable_automatic_trimming": True,
            },
        },
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    def _create_cells(self):
        sync_create_cells(1)

    @staticmethod
    def _create_queue(path, **kwargs):
        attributes = {
            "dynamic": True,
            "schema": TestKafkaProxy.DEFAULT_QUEUE_SCHEMA,
        }
        attributes.update(kwargs)
        create("table", path, attributes=attributes)
        sync_mount_table(path)

    @authors("nadya73")
    def test_check_cypress(self):
        assert len(get("//sys/kafka_proxies")) == 1

    @authors("nadya73")
    def test_basic(self):
        self._create_cells()

        address = self.Env.get_kafka_proxy_address()

        queue_path = "primary://tmp/queue"
        consumer_path = "primary://tmp/consumer"

        TestKafkaProxy._create_queue(queue_path)
        self._create_registered_consumer(consumer_path, queue_path)

        insert_rows(queue_path, [
            {"surname": "foo-0", "number": 0},
            {"surname": "foo-1", "number": 1},
            {"surname": "foo-2", "number": 2},
        ])

        consumer_config = {
            "bootstrap.servers": address,
            "security.protocol": "SASL_PLAINTEXT",  # "PLAINTEXT",
            "sasl.mechanisms": "OAUTHBEARER",
            'oauth_cb': functools.partial(_get_token, 123),
            "client.id": "1234567",
            "group.id": consumer_path,
            "log_level": 7,
            "debug": 'all'
        }
        c = Consumer(consumer_config)

        c.assign([TopicPartition(queue_path, 0)])

        message_count = 0
        none_message_count = 0
        error_count = 0

        while True:
            msg = c.poll(1.0)

            if msg is None:
                none_message_count += 1
                if none_message_count > 100:
                    assert False
                continue

            if msg.error():
                error_count += 1
                if error_count > 10:
                    assert not msg.error()
                continue

            assert msg.key().decode() == ""
            # value = yson.loads(msg.value().decode())
            # assert value["surname"] == f"foo-{message_count}"
            # assert value["number"] == message_count

            message_count += 1
            if message_count >= 3:
                break

        assert message_count == 3

    @authors("nadya73")
    def test_unsupported_sasl_mechanism(self):
        address = self.Env.get_kafka_proxy_address()

        queue_path = "primary://tmp/queue"
        consumer_path = "primary://tmp/consumer"

        consumer_config = {
            "bootstrap.servers": address,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "user123",
            "sasl.password": "password123",
            "client.id": "123",
            "group.id": consumer_path,
            "log_level": 7,
            "debug": 'all'
        }
        c = Consumer(consumer_config)

        c.assign([TopicPartition(queue_path, 0)])

        poll_count = 0
        while True:
            msg = c.poll(1.0)

            assert msg is None or msg.error()
            poll_count += 1

            if poll_count > 10:
                break
