from yt_env_setup import YTEnvSetup

from yt_queue_agent_test_base import (
    TestQueueAgentBase, ReplicatedObjectBase)

from yt_commands import (
    authors, get, ls, create, sync_mount_table, insert_rows, sync_create_cells,
    create_user, issue_token, raises_yt_error, pull_queue, pull_consumer, set,
    make_ace)

from confluent_kafka import (
    Consumer, TopicPartition)

from flaky import flaky

import functools
import time

##################################################################


def _get_token(token, config):
    return token, time.time() + 3600


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

    def _consume_messages(self, queue_path, consumer_path, token, message_count=3):
        address = self.Env.get_kafka_proxy_address()

        consumer_config = {
            "bootstrap.servers": address,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "OAUTHBEARER",
            'oauth_cb': functools.partial(_get_token, token),
            "client.id": "1234567",
            "group.id": consumer_path,
            "debug": "all",
        }
        c = Consumer(consumer_config)

        c.assign([TopicPartition(queue_path, 0)])

        none_message_count = 0
        error_count = 0

        messages = []

        while True:
            msg = c.poll(1.0)

            if msg is None:
                none_message_count += 1
                if none_message_count > 100:
                    assert False, "Too much none messages"
                continue

            if msg.error():
                error_count += 1
                if error_count > 10:
                    assert not msg.error()
                continue

            messages += [msg]

            assert msg.key().decode() == ""
            # value = yson.loads(msg.value().decode())
            # assert value["surname"] == f"foo-{message_count}"
            # assert value["number"] == message_count

            if len(messages) >= message_count:
                break

        c.close()

        return messages

    @authors("nadya73")
    def test_check_cypress(self):
        address = self.Env.get_kafka_proxy_address()

        assert len(get("//sys/kafka_proxies/instances")) == 1
        assert ls("//sys/kafka_proxies/instances")[0] == address

    @authors("nadya73")
    @flaky(max_runs=3)
    def test_basic(self):
        username = "u"
        create_user(username)
        token, _ = issue_token(username)

        self._create_cells()

        queue_path = "primary://tmp/queue"
        consumer_path = "primary://tmp/consumer"

        TestKafkaProxy._create_queue(queue_path)
        self._create_registered_consumer(consumer_path, queue_path)

        insert_rows(queue_path, [
            {"surname": "foo-0", "number": 0},
            {"surname": "foo-1", "number": 1},
            {"surname": "foo-2", "number": 2},
        ])

        set(f"{queue_path}/@inherit_acl", False)
        set(f"{consumer_path}/@inherit_acl", False)

        with raises_yt_error("permission"):
            pull_queue(queue_path, authenticated_user=username, partition_index=0, offset=0)

        with raises_yt_error("permission"):
            pull_consumer(consumer_path, queue_path, authenticated_user=username, partition_index=0, offset=0)

        set(f"{queue_path}/@acl/end", make_ace("allow", "u", ["read"]))
        set(f"{consumer_path}/@acl/end", make_ace("allow", "u", ["read"]))

        messages = self._consume_messages(queue_path, consumer_path, token, message_count=3)
        assert len(messages) == 3

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
