from yt_env_setup import (
    YTEnvSetup, Restarter, KAFKA_PROXIES_SERVICE)

from yt_queue_agent_test_base import TestQueueAgentBase

from yt_commands import (
    authors, get, ls, create, sync_mount_table, insert_rows, sync_create_cells,
    create_user, issue_token, raises_yt_error, pull_queue, pull_consumer, set,
    make_ace, select_rows, sync_unmount_table, wait)

import yt.yson

from confluent_kafka import (
    Consumer, TopicPartition, Producer, KafkaError)

from confluent_kafka.serialization import StringSerializer

import builtins
import functools
import time

##################################################################


class KafkaMessageHelper:
    def __init__(self, kafka_message):
        self.kafka_message = kafka_message

    def assert_matching(self, expected_key: str | bytes, expected_value: dict | str | bytes, is_kafka_message: bool, checked_sys_fields: list[str] | None = None):
        if isinstance(expected_key, str):
            expected_key = expected_key.encode("utf-8")
        if isinstance(expected_value, str):
            expected_value = expected_value.encode("utf-8")

        if checked_sys_fields is None:
            checked_sys_fields = []

        assert self.kafka_message.key() == expected_key
        if is_kafka_message:
            assert isinstance(expected_value, bytes), "type of \"expected_value\" should be \"str\" or \"bytes\" if \"is_kafka_message\" is True"
            assert self.kafka_message.value() == expected_value
        else:
            assert isinstance(expected_value, dict), "type of \"expected_value\" should be \"dict\" if \"is_kafka_message\" is False"

            kafka_value = yt.yson.loads(self.kafka_message.value())

            fields = builtins.set(kafka_value.keys()) | builtins.set(expected_value.keys())
            filtered_fields = {field for field in fields if not field.startswith("$") or field in checked_sys_fields}

            filtered_kafka_value = {checked_field: kafka_value.get(checked_field, None) for checked_field in filtered_fields}
            filtered_value = {checked_field: expected_value.get(checked_field, None) for checked_field in filtered_fields}

            assert filtered_kafka_value == filtered_value


class KafkaMessageListHelper(list):
    def assert_matching(self, messages: list[dict], is_kafka_message: bool):
        for kafka_message, message in zip(self, messages):
            kafka_message.assert_matching(message["key"], message["value"], is_kafka_message)


##################################################################

def _get_token(token, config):
    return token, time.time() + 3600


def _fail_on_error(err, msg):
    assert err is None


def _check_error(code, err, msg):
    assert isinstance(err, KafkaError) and err.code() == code


class TestKafkaProxy(TestQueueAgentBase, YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1
    NUM_KAFKA_PROXIES = 1

    DEFAULT_QUEUE_SCHEMA = [
        {"name": "$timestamp", "type": "uint64"},
        {"name": "$cumulative_data_weight", "type": "int64"},
        {"name": "surname", "type": "string"},
        {"name": "number", "type": "int64"},
    ]

    KAFKA_QUEUE_SCHEMA = [
        {"name": "$timestamp", "type": "uint64"},
        {"name": "$cumulative_data_weight", "type": "int64"},
        {"name": "key", "type": "string"},
        {"name": "value", "type": "string", "required": True},
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

    @staticmethod
    def _create_kafka_queue(path, **kwargs):
        attributes = {
            "dynamic": True,
            "schema": TestKafkaProxy.KAFKA_QUEUE_SCHEMA,
        }
        attributes.update(kwargs)
        create("table", path, attributes=attributes)
        sync_mount_table(path)

    def _consume_messages(self, queue_path, consumer_path, token, message_count=3, sasl_mechanism="OAUTHBEARER", assign_partitions=None):
        address = self.Env.get_kafka_proxy_address()

        consumer_config = {
            "bootstrap.servers": address,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": sasl_mechanism,
            "client.id": "1234567",
            "group.id": consumer_path,
            "debug": "all",
        }

        if sasl_mechanism == "OAUTHBEARER":
            consumer_config["oauth_cb"] = functools.partial(_get_token, token)
        elif sasl_mechanism == "PLAIN":
            consumer_config["sasl.username"] = "u"
            consumer_config["sasl.password"] = token

        c = Consumer(consumer_config)

        if assign_partitions:
            c.assign([TopicPartition(queue_path, partition_index) for partition_index in assign_partitions])
        else:
            c.subscribe([queue_path])

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

            if len(messages) >= message_count:
                break

            c.commit(msg)

        c.close()

        messages = KafkaMessageListHelper([KafkaMessageHelper(message) for message in messages])
        return messages

    @authors("nadya73")
    def test_check_cypress(self):
        address = self.Env.get_kafka_proxy_address()

        assert len(get("//sys/kafka_proxies/instances")) == 1
        assert ls("//sys/kafka_proxies/instances")[0] == address

    @authors("nadya73")
    def test_dynamic_config(self):
        proxy_name = ls("//sys/kafka_proxies/instances")[0]

        local_host_name = "123"
        set(
            "//sys/kafka_proxies/@config",
            {"local_host_name": local_host_name},
        )

        def config_updated():
            config = get("//sys/kafka_proxies/instances/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            return "local_host_name" in config and local_host_name == config["local_host_name"]

        wait(config_updated)

        with Restarter(self.Env, KAFKA_PROXIES_SERVICE):
            pass

        wait(config_updated, ignore_exceptions=True)

        set(
            "//sys/kafka_proxies/@config",
            {},
        )

        def config_cleared():
            config = get("//sys/kafka_proxies/instances/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            return "local_host_name" not in config

        wait(config_cleared)

    @authors("nadya73")
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
        set(f"{consumer_path}/@acl/end", make_ace("allow", "u", ["read", "write"]))

        messages = self._consume_messages(queue_path, consumer_path, token, message_count=3, assign_partitions=[0])
        assert len(messages) == 3

        assert select_rows("* from [//tmp/consumer]")[0]["offset"] == len(messages)

    @authors("nadya73")
    def test_basic_plain_sasl_mechanism(self):
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
        set(f"{consumer_path}/@acl/end", make_ace("allow", "u", ["read", "write"]))

        messages = self._consume_messages(queue_path, consumer_path, token, message_count=3, sasl_mechanism="PLAIN", assign_partitions=[0])
        assert len(messages) == 3

        assert select_rows("* from [//tmp/consumer]")[0]["offset"] == len(messages)

    @authors("nadya73")
    def test_producer(self):
        username = "u"
        create_user(username)
        token, _ = issue_token(username)

        self._create_cells()

        queue_path = "primary://tmp/queue"

        TestKafkaProxy._create_kafka_queue(queue_path, tablet_count=3)

        address = self.Env.get_kafka_proxy_address()
        producer_config = {
            "bootstrap.servers": address,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "OAUTHBEARER",
            "oauth_cb": functools.partial(_get_token, token),
            "client.id": "1234567",
            "debug": "all",
        }
        p = Producer(producer_config)

        serializer = StringSerializer('utf_8')

        rows_count = 20
        messages = [[f"key_{i}", f"value_{i}"] for i in range(int(rows_count / 2))]
        messages += [[f"value_{i}"] for i in range(int(rows_count / 2), rows_count)]

        for msg_index, msg in enumerate(messages):
            p.poll(0.0)

            if len(msg) > 1:
                p.produce(topic=queue_path,
                          key=serializer(msg[0]),
                          value=serializer(msg[1]),
                          on_delivery=_fail_on_error)
            else:
                p.produce(topic=queue_path,
                          value=serializer(msg[0]),
                          on_delivery=_fail_on_error)
            if msg_index % 4 == 0:
                p.flush()

        p.flush()

        def _check_rows(expected_rows_count):
            rows = select_rows("* from [//tmp/queue]")
            rows = sorted(rows, key=lambda row: int(row["value"][6:]))
            assert len(rows) == expected_rows_count

            for row_index, row in enumerate(rows):
                if row_index < rows_count / 2:
                    assert row["key"] == f"key_{row_index}"
                else:
                    assert row["key"] == ""
                assert row["value"] == f"value_{row_index}"

        _check_rows(rows_count)

        # Failed to write rows.
        sync_unmount_table(queue_path)

        p.poll(0.0)

        p.produce(topic=queue_path,
                  key=serializer("key_20"),
                  value=serializer("value_20"),
                  on_delivery=functools.partial(_check_error, KafkaError.UNKNOWN))

        p.flush()

        sync_mount_table(queue_path)

        _check_rows(rows_count)

        # No write permission.
        set(f"{queue_path}/@inherit_acl", False)

        p.poll(0.0)

        p.produce(topic=queue_path,
                  key=serializer("key_20"),
                  value=serializer("value_20"),
                  on_delivery=functools.partial(_check_error, KafkaError.TOPIC_AUTHORIZATION_FAILED))

        p.flush()

        set(f"{queue_path}/@inherit_acl", True)

        _check_rows(rows_count)

    @authors("apachee")
    def test_fetch(self):
        username = "u"
        create_user(username)
        token, _ = issue_token(username)

        self._create_cells()

        queue_path = "primary://tmp/queue"
        kafka_queue_path = "primary://tmp/kafka_queue"
        consumer_path = "primary://tmp/consumer"
        kafka_consumer_path = "primary://tmp/kafka_consumer"

        TestKafkaProxy._create_queue(queue_path)
        TestKafkaProxy._create_kafka_queue(kafka_queue_path)
        self._create_registered_consumer(consumer_path, queue_path)
        self._create_registered_consumer(kafka_consumer_path, kafka_queue_path)

        queue_rows = [
            {"surname": "foo-0", "number": 0},
            {"surname": "foo-1", "number": 1},
            {"surname": "foo-2", "number": 2},
        ]

        kafka_queue_rows = [
            {"key": "foo", "value": "foo-value"},
            {"key": "bar", "value": "bar-value"},
            {"key": "baz", "value": "baz-value"},
        ]

        insert_rows(queue_path, queue_rows)
        insert_rows(kafka_queue_path, kafka_queue_rows)

        def generic_queue_message(row):
            return {
                "key": "",
                "value": row,
            }

        messages = self._consume_messages(queue_path, consumer_path, token, message_count=3, assign_partitions=[0])
        messages.assert_matching([generic_queue_message(row) for row in queue_rows], False)

        messages = self._consume_messages(kafka_queue_path, consumer_path, token, message_count=3, assign_partitions=[0])
        messages.assert_matching([
            {
                "key": row["key"],
                "value": row["value"],
            } for row in kafka_queue_rows
        ], True)

    @authors("nadya73")
    def test_consumer_group_coordinator(self):
        username = "u"
        create_user(username)
        token, _ = issue_token(username)

        self._create_cells()

        queue_path = "primary://tmp/queue"
        consumer_path = "primary://tmp/consumer"

        tablet_count = 6
        row_count = tablet_count

        TestKafkaProxy._create_queue(queue_path, tablet_count=tablet_count)
        self._create_registered_consumer(consumer_path, queue_path)

        consumer_count = 3
        rows = [
            {"surname": "foo-0", "number": i, "$tablet_index": i} for i in range(row_count)
        ]

        insert_rows(queue_path, rows)

        address = self.Env.get_kafka_proxy_address()

        consumers = []
        for consumer_id in range(consumer_count):
            consumer_config = {
                "bootstrap.servers": address,
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanisms": "PLAIN",
                "client.id": f"consumer-{consumer_id}",
                "group.id": consumer_path,
                "debug": "all",
                "sasl.username": "u",
                "sasl.password": token,
                "partition.assignment.strategy": "range",
                "heartbeat.interval.ms": 150,
            }

            c = Consumer(consumer_config)
            c.subscribe([queue_path])
            consumers.append(c)

        none_message_count = 0
        error_count = 0

        messages = []
        consumer_message_counts = [0] * len(consumers)

        for consumer_index, consumer in enumerate(consumers):
            while True:
                msg = consumer.poll(0.3)

                if msg is None:
                    none_message_count += 1
                    if none_message_count > 20:
                        break
                    continue

                if msg.error():
                    error_count += 1
                    if error_count > 20:
                        assert not msg.error()
                    continue

                messages += [msg]
                consumer_message_counts[consumer_index] += 1

                consumer.commit(msg)

        assert len(messages) == row_count

        for consumer_message_count in consumer_message_counts:
            assert consumer_message_count == 2

        consumer_rows = select_rows("* from [//tmp/consumer]")
        for consumer_row in consumer_rows:
            consumer_row["offset"] == 1

        # Create more consumers.
        for consumer_id in range(consumer_count, consumer_count * 2):
            consumer_config = {
                "bootstrap.servers": address,
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanisms": "PLAIN",
                "client.id": f"consumer-{consumer_id}",
                "group.id": consumer_path,
                "debug": "all",
                "sasl.username": "u",
                "sasl.password": token,
                "partition.assignment.strategy": "range",
                "heartbeat.interval.ms": 300,
            }

            c = Consumer(consumer_config)
            c.subscribe([queue_path])
            consumers.append(c)

        # Wait rebalancing.
        for _ in range(5):
            for consumer_index, consumer in enumerate(consumers):
                consumer.poll(0.2)

        consumer_count *= 2
        rows *= 2
        row_count *= 2
        messages = []
        none_message_count = 0
        error_count = 0

        insert_rows(queue_path, rows)

        consumer_message_counts = [0] * len(consumers)

        while True:
            if none_message_count > 30:
                break
            for consumer_index, consumer in enumerate(consumers):
                msg = consumer.poll(0.3)

                if msg is None:
                    none_message_count += 1
                    continue

                if msg.error():
                    error_count += 1
                    if error_count > 100:
                        assert not msg.error()
                    continue

                messages += [msg]
                consumer_message_counts[consumer_index] += 1

                consumer.commit(msg)

        for consumer in consumers:
            consumer.close()

        assert len(messages) == row_count

        for consumer_message_count in consumer_message_counts:
            assert consumer_message_count == 2

        consumer_rows = select_rows("* from [//tmp/consumer]")
        for consumer_row in consumer_rows:
            consumer_row["offset"] == 2
