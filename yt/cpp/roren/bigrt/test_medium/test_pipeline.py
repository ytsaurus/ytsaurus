import gzip
import json
import logging
import os
import re
import sys
import time
import urllib.request
from datetime import timedelta
from collections.abc import Iterable
from typing import Any, Callable

import pytest

import yatest.common

import bigrt.py_lib as bigrt

from mapreduce.yt.python.yt_stuff import YtConfig

import kikimr.public.sdk.python.persqueue.grpc_pq_streaming_api as pqlib
from kikimr.public.sdk.python.persqueue.pq_control_plane_client import PQControlPlaneClient

from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

import ydb


FILTER_PIPELINE_BIN = yatest.common.binary_path("yt/cpp/roren/bigrt/test_medium/filter_pipeline/filter_pipeline")
METRICS_PIPELINE_BIN = yatest.common.binary_path("yt/cpp/roren/bigrt/test_medium/metrics_pipeline/metrics_pipeline")
IN_MEMORY_DICT_RESOLVER_PIPELINE_BIN = yatest.common.binary_path("yt/cpp/roren/bigrt/test_medium/in_memory_dict_resolver/in_memory_dict_resolver")
MULTIPLE_CONSUMING_SYSTEMS_BIN = yatest.common.binary_path("yt/cpp/roren/bigrt/test_medium/multiple_consuming_systems/multiple_consuming_systems")
WRITE_YTDYNTABLE_NODE_BIN = yatest.common.binary_path("yt/cpp/roren/bigrt/test_medium/write_ytdyntable_node/write_ytdyntable_node")
WRITE_YTDYNTABLE_UNVERSIONEDROW_BIN = yatest.common.binary_path("yt/cpp/roren/bigrt/test_medium/write_ytdyntable_unversionedrow/write_ytdyntable_unversionedrow")
STATEFUL_PIPELINE_BIN = yatest.common.binary_path("yt/cpp/roren/bigrt/test_medium/stateful_pipeline/stateful_pipeline")


@pytest.fixture(scope="module")
def yt_config(request):
    return YtConfig(wait_tablet_cell_initialization=True)


#
# Logbroker library
#


class LogbrokerErrorResponse(RuntimeError):
    pass


class LogbrokerStuff:
    def __init__(self):
        port = os.environ.get("LOGBROKER_PORT", None)
        if port is None:
            raise RuntimeError(
                "Logbroker seems to be not running.\n"
                "Does your ya.make contain\n"
                "  INCLUDE(${ARCADIA_ROOT}/kikimr/public/tools/lbk_recipe/recipe_stable.inc)\n ")

        self.host = "localhost"
        self.port = int(port)
        self.address = f"localhost:{port}"

        # TODO: we need to properly shutdown streaming api
        self.pq_streaming_api = pqlib.PQStreamingAPI("localhost", port)
        self.pq_streaming_api.start().result(timeout=10)
        self._ydb_driver = ydb.Driver(ydb.DriverConfig(f"localhost:{port}"))
        self._ydb_driver.wait()
        self.pq_control_plane_client = PQControlPlaneClient(self._ydb_driver)


@pytest.fixture(scope="module")
def logbroker_stuff(request) -> LogbrokerStuff:
    return LogbrokerStuff()


def verify_logbroker_response(response):
    if response.operation.status == StatusIds.StatusCode.SUCCESS:
        return

    raise LogbrokerErrorResponse(str(response.operation))


def logbroker_create_topic(logbroker_stuff: LogbrokerStuff, topic_name: str, shards: int = 1):
    # In tests logboker is run without configuration server
    # and topics must be created using old style names.
    #
    # You might consider it black magic
    topic_name = f"rt3.dc1--{topic_name}"
    response = logbroker_stuff.pq_control_plane_client.create_topic(topic_name, shards)
    verify_logbroker_response(response)


def logbroker_write_topic(logbroker_stuff: LogbrokerStuff, topic_name: str, data: list[str], source_id: str = "TestWriter"):
    max_seq_no = None

    configurator = pqlib.ProducerConfigurator(topic_name, source_id)

    producer = logbroker_stuff.pq_streaming_api.create_producer(configurator)
    start_future = producer.start()  # Also available with producer.start_future()
    start_result = start_future.result(timeout=10)

    # Result of start should be verified. An error could occur.
    if start_result.HasField("init"):
        max_seq_no = start_result.init.max_seq_no
    else:
        raise RuntimeError("Producer failed to start with error {}".format(start_result))

    write_responses = []
    for message in data:
        max_seq_no += 1
        response = producer.write(max_seq_no, message)
        write_responses.append(response)

    # Step 7. Now wait for confirmation. WARNING - if you stop/kill producer or PqLib object before receiving these Acks,
    # it is possible that data don't actually get written to Logbroker
    for r in write_responses:
        write_result = r.result(timeout=10)
        if not write_result.HasField("ack"):
            raise RuntimeError("Message write failed with error {}".format(write_result))

    producer.stop()


def logbroker_read_topic(logbroker_stuff: LogbrokerStuff, topic_name: str, expected_message_count: int, client_id: str = "TestReader"):
    # TODO: we need to use RequestPartitionStatus request
    #   to get actual count of messages inside queue
    #   and not rely on expected_message_count
    #   but this request is not supported by LB for now
    #
    # https://t.me/c/1090265974/73788
    # https://t.me/c/1090265974/73835
    configurator = pqlib.ConsumerConfigurator(topic_name, client_id)

    consumer = logbroker_stuff.pq_streaming_api.create_consumer(configurator)

    start_future = consumer.start()  # Also available with consumer.start_future()
    start_result = start_future.result(timeout=10)
    # Result of start should be verified. An error could occur.
    if not start_result.HasField("init"):
        raise RuntimeError("Consumer failed to start with error {}".format(start_result))

    def _process_single_batch(consumer_message):
        for batch in consumer_message.data.message_batch:
            for message in batch.message:
                yield message

    # In this sample we don't process data, ust collect it in a list. Put here your processing instead.
    while expected_message_count > 0:
        # next_event() returns concurrent.Future object. Result will be set to Future when response is actually received
        # from server
        result = consumer.next_event().result(timeout=10)
        # Actually, more result types exist. But here we didn't call commit() yet
        # and don't use Assign (aka Lock/Release) functionality so currently can expect only responses with data.
        assert result.type == pqlib.ConsumerMessageType.MSG_DATA
        for r in _process_single_batch(result.message):
            yield r
            expected_message_count -= 1
            if expected_message_count <= 0:
                break

    consumer.reads_done()
    consumer.stop()


#
# YT queue library functions
#


def yt_queue_create(
    yt_client: "yt.wrapper.YtClient",
    queue_path: str,
    shards: int = 1
):
    """
    Create YT queue

    :param: yt_client -- client to be used
    :param: queue_path -- path of the queue
    :param: shards -- number of shards (it's also number of ordered table tablet count)
    """
    attributes = {
        "tablet_count": shards,
        "primary_medium": "default",
        "commit_ordering": "strong",
    }

    bigrt.create_queue(yt_client, bigrt.YtQueuePath(queue_path), attributes, logging.info, swift=False)


# TODO: Roren should automatically create consumers in tests
def yt_queue_create_consumer(
    yt_client: "yt.wrapper.YtClient" ,
    queue_path: str,
    consumer_name: str
):
    bigrt.create_consumer(yt_client, bigrt.YtQueuePath(queue_path), consumer_name, ignore_in_trimming=False, logger=logging.info)


def yt_queue_write(
    yt_client: "yt.wrapper.YtClient",
    queue_path: str,
    data: Iterable[tuple[int, bytes | str]]
):
    """
    Write data to YT queue
    """
    cluster = yt_client.config['proxy']['url']

    sharded_data = {}
    for shard_id, s in data:
        if isinstance(s, str):
            s = s.encode("utf-8")
        sharded_data.setdefault(shard_id, []).append(s)

    queue = bigrt.YtQueue({"path": queue_path, "cluster": cluster})
    queue.write(sharded_data)


def read_yt_queue(
    yt_client: "yt.wrapper.YtClient",
    queue_path: str
):
    cluster = yt_client.config['proxy']['url']
    queue = bigrt.YtQueue({"path": queue_path, "cluster": cluster})
    shard_infos = queue.get_shard_infos()
    for shard_idx, info in enumerate(shard_infos):
        total_row_count = info["total_row_count"]
        read_result = queue.read(shard_idx, 0, total_row_count)
        for row in read_result["rows"]:
            yield (shard_idx, row)


def create_yt_dyntable(yt_client, yt_path, schema):
    attrs = {
        'dynamic': True,
        'schema': schema
    }
    yt_client.create(type='table', path=yt_path, attributes=attrs)
    yt_client.mount_table(yt_path)


def read_yt_dyntable(yt_client, yt_path):
    query = f'* FROM [{yt_path}]'
    rows = yt_client.select_rows(query, allow_full_scan=True)
    return rows


def yt_queue_wait_consumed(
    yt_client: "yt.wrapper.YtClient",
    queue_path: str,
    consumer: str,
    circuit_breaker: Callable[[], Any] | None = None,
    timeout: timedelta = timedelta(seconds=60)
):
    deadline = time.time() + timeout.total_seconds()
    cluster = yt_client.config['proxy']['url']
    queue = bigrt.YtQueue({"path": queue_path, "cluster": cluster})
    shard_infos = queue.get_shard_infos()
    while True:
        if time.time() > deadline:
            raise TimeoutError(f"Timeout exceeded while waiting for consumtion of {queue_path}")
        consumer_offsets = queue.get_consumer_offsets(consumer)
        shard_status_list = []
        all_read = True
        for shard_idx, shard_info in enumerate(shard_infos):
            actual = consumer_offsets[shard_idx]
            expected = shard_info["total_row_count"] - 1
            if actual != expected and actual != queue.unreachable_offset:
                all_read = False
            shard_status_list.append(f"{shard_idx}: {actual}/{expected}")

        shard_status = '; '.join(shard_status_list)
        logging.info(f"Shard read status: {shard_status}")
        if all_read:
            logging.info("All messages in all shards are read")
            return
        if circuit_breaker is not None:
            circuit_breaker()
        time.sleep(0.1)


def get_metric_value(solomon_metrics: bytes, sensor_name: str):
    parsed = json.loads(solomon_metrics)
    for item in parsed["sensors"]:
        if item["labels"]["sensor"] == sensor_name:
            return item["value"]
    raise KeyError(f"Sensor \"{sensor_name}\" is not found")


class ExecutionWatcher:
    def __init__(self, execution):
        self._execution = execution

    def __call__(self):
        if not self._execution.running:
            try:
                self._execution.wait(timeout=0)
            except yatest.common.ExecutionError:
                sys.stderr.write(f"stderr of $ {self._execution.command}")
                sys.stderr.buffer.write(self._execution.stderr)
                sys.stderr.flush()
                raise


@pytest.fixture
def test_home_ypath(request, yt_stuff):
    escaped_name = re.sub("[^a-zA-Z0-9_:]", "_", request.node.nodeid)

    yt_client = yt_stuff.get_yt_client()
    home_ypath = "//home/" + escaped_name
    yt_client.create("map_node", home_ypath)

    yield home_ypath


@pytest.fixture
def yt_client(yt_stuff):
    yield yt_stuff.get_yt_client()


@pytest.fixture
def yt_cluster(yt_stuff):
    yield yt_stuff.get_server()


parametrize_enable_v2_graph_parsing = pytest.mark.parametrize("enable_v2_graph_parsing", [
    pytest.param("false", id="v1_graph_parsing"),
    pytest.param("true", id="v2_graph_parsing"),
])


@parametrize_enable_v2_graph_parsing
def test_smoke(yt_cluster, yt_client, test_home_ypath, enable_v2_graph_parsing: str):
    os.environ["SET_DEFAULT_LOCAL_YT_PARAMETERS"] = "1"

    input_queue = f"{test_home_ypath}/input"
    yt_queue_create(yt_client, input_queue)
    yt_queue_create_consumer(yt_client, input_queue, "consumer")

    output_queue = f"{test_home_ypath}/output"
    yt_queue_create(yt_client, output_queue)

    with open("filter_pipeline_config", "w") as outf:
        outf.write(f"""\
{{
    "RorenConfig": {{
        "EnableV2GraphParsing": {enable_v2_graph_parsing},
        "Consumers": [
            {{
                "ConsumingSystemConfig": {{
                    "Cluster": "{yt_cluster}",
                    "MainPath": "{test_home_ypath}/consuming_system",
                    "MaxShards": 1
                }}
            }}
        ],
        "EasyInputs": [
            {{
                "Alias": "input",
                "YtSupplier": {{
                    "Cluster": "{yt_cluster}",
                    "QueuePath": "{input_queue}",
                    "QueueConsumer": "consumer"
                }}
            }}
        ]
    }},
    "YtQueueOutput": {{
        "Path": "{output_queue}",
        "ShardsCount": 1,
        "CompressionCodec": "null"
    }}
}}
""")

    cmd = [FILTER_PIPELINE_BIN, "--config-json=filter_pipeline_config"]

    execution = yatest.common.execute(cmd, wait=False)
    yt_queue_write(
        yt_client,
        input_queue,
        [
            (0, """{"data": "foo", "keep": true}"""),
            (0, """{"data": "bar"}"""),
            (0, """{"data": "qux", "keep": true}"""),
        ]
    )

    yt_queue_wait_consumed(yt_client, input_queue, "consumer", circuit_breaker=ExecutionWatcher(execution))

    execution.terminate()
    result = read_yt_queue(yt_client, output_queue)
    result = [(shard_idx, json.loads(data)) for shard_idx, data in result]
    assert result == [
        (0, {"data": "foo", "keep": True}),
        (0, {"data": "qux", "keep": True})
    ]


@parametrize_enable_v2_graph_parsing
def test_bigrt_execution_context(yt_cluster, yt_client, test_home_ypath, enable_v2_graph_parsing: str):
    os.environ["SET_DEFAULT_LOCAL_YT_PARAMETERS"] = "1"

    input_queue = f"{test_home_ypath}/input"
    yt_queue_create(yt_client, input_queue, shards=2)
    yt_queue_create_consumer(yt_client, input_queue, "consumer")

    output_queue = f"{test_home_ypath}/output"
    yt_queue_create(yt_client, output_queue)

    with open("filter_pipeline_config", "w") as outf:
        outf.write(f"""\
{{
    "RorenConfig": {{
        "EnableV2GraphParsing": {enable_v2_graph_parsing},
        "Consumers": [
            {{
                "ConsumingSystemConfig": {{
                    "Cluster": "{yt_cluster}",
                    "MainPath": "{test_home_ypath}/consuming_system",
                    "MaxShards": 2
                }}
            }}
        ],
        "EasyInputs": [
            {{
                "Alias": "input",
                "YtSupplier": {{
                    "Cluster": "{yt_cluster}",
                    "QueuePath": "{input_queue}",
                    "QueueConsumer": "consumer"
                }}
            }}
        ]
    }},
    "YtQueueOutput": {{
        "Path": "{output_queue}",
        "ShardsCount": 1,
        "CompressionCodec": "null"
    }}
}}
""")

    cmd = [FILTER_PIPELINE_BIN, "--config-json=filter_pipeline_config"]

    execution = yatest.common.execute(cmd, wait=False)
    yt_queue_write(
        yt_client,
        input_queue,
        [
            (0, """{"data": "foo", "keep": true}"""),
            (0, """{"data": "bar"}"""),
            (0, """{"data": "qux", "keep": true}"""),
            (1, """{"data": "abc", "keep": true}"""),  # It will be filtered because of shard id.
        ]
    )

    yt_queue_wait_consumed(yt_client, input_queue, "consumer", circuit_breaker=ExecutionWatcher(execution))

    execution.terminate()
    result = read_yt_queue(yt_client, output_queue)
    result = [(shard_idx, json.loads(data)) for shard_idx, data in result]
    assert result == [
        (0, {"data": "foo", "keep": True}),
        (0, {"data": "qux", "keep": True})
    ]


@parametrize_enable_v2_graph_parsing
def test_ytdyntable_write_node(yt_client, yt_cluster, test_home_ypath, enable_v2_graph_parsing: str):
    os.environ["SET_DEFAULT_LOCAL_YT_PARAMETERS"] = "1"

    input_queue = f"{test_home_ypath}/input"
    yt_queue_create(yt_client, input_queue, shards=2)
    yt_queue_create_consumer(yt_client, input_queue, "consumer")
    yt_dyntable_output = f"{test_home_ypath}/output"
    output_schema = [
        {'name': 'shard', 'type': 'uint64', 'required': 'true', 'sort_order': 'ascending'},
        {'name': 'key',   'type': 'string', 'required': 'true', 'sort_order': 'ascending'},
        {'name': 'value', 'type': 'string', 'required': 'true'},
    ]
    create_yt_dyntable(yt_client=yt_client, yt_path=yt_dyntable_output, schema=output_schema)
    cfg_filename = "write_ytdyntable_config"

    with open(cfg_filename, "w") as outf:
        outf.write(f"""\
{{
    "RorenConfig": {{
        "EnableV2GraphParsing": {enable_v2_graph_parsing},
        "Consumers": [
            {{
                "ConsumingSystemConfig": {{
                    "Cluster": "{yt_cluster}",
                    "MainPath": "{test_home_ypath}/consuming_system",
                    "MaxShards": 2
                }}
            }}
        ],
        "EasyInputs": [
            {{
                "Alias": "input",
                "YtSupplier": {{
                    "Cluster": "{yt_cluster}",
                    "QueuePath": "{input_queue}",
                    "QueueConsumer": "consumer"
                }}
            }}
        ]
    }},
    "YtDynTableOutputPath": "{yt_dyntable_output}"
}}
""")

    cmd = [WRITE_YTDYNTABLE_NODE_BIN, f"--config-json={cfg_filename}"]

    execution = yatest.common.execute(cmd, wait=False)
    yt_queue_write(
        yt_client,
        input_queue,
        [
            (0, """{"key": "key1", "value": "value1"}"""),
            (0, """{"key": "key2", "value": "value2"}"""),
            (1, """{"key": "key3", "value": "value3"}"""),
            (1, """{"key": "key4", "value": "value4"}"""),
        ]
    )

    yt_queue_wait_consumed(yt_client, input_queue, "consumer", circuit_breaker=ExecutionWatcher(execution))
    execution.terminate()

    rows = read_yt_dyntable(yt_client, yt_dyntable_output)
    rows = [row for row in rows]
    expected_result = [
        {"shard": 0, "key": "key1", "value": "value1"},
        {"shard": 0, "key": "key2", "value": "value2"},
        {"shard": 1, "key": "key3", "value": "value3"},
        {"shard": 1, "key": "key4", "value": "value4"},
    ]
    assert (rows == expected_result)


@parametrize_enable_v2_graph_parsing
def test_ytdyntable_unversionedrow_write(yt_client, yt_cluster, test_home_ypath, enable_v2_graph_parsing):
    os.environ["SET_DEFAULT_LOCAL_YT_PARAMETERS"] = "1"

    input_queue = f"{test_home_ypath}/input"
    yt_queue_create(yt_client, input_queue, shards=2)
    yt_queue_create_consumer(yt_client, input_queue, "consumer")
    yt_dyntable_output = f"{test_home_ypath}/output"
    output_schema = [
        {'name': 'shard', 'type': 'uint64', 'required': 'true', 'sort_order': 'ascending'},
        {'name': 'key',   'type': 'string', 'required': 'true', 'sort_order': 'ascending'},
        {'name': 'value', 'type': 'string', 'required': 'true'},
    ]
    create_yt_dyntable(yt_client=yt_client, yt_path=yt_dyntable_output, schema=output_schema)
    cfg_filename = "write_ytdyntable_config"

    with open(cfg_filename, "w") as outf:
        outf.write(f"""\
{{
    "RorenConfig": {{
        "EnableV2GraphParsing": {enable_v2_graph_parsing},
        "Consumers": [
            {{
                "ConsumingSystemConfig": {{
                    "Cluster": "{yt_cluster}",
                    "MainPath": "{test_home_ypath}/consuming_system",
                    "MaxShards": 2
                }}
            }}
        ],
        "EasyInputs": [
            {{
                "Alias": "input",
                "YtSupplier": {{
                    "Cluster": "{yt_cluster}",
                    "QueuePath": "{input_queue}",
                    "QueueConsumer": "consumer"
                }}
            }}
        ]
    }},
    "YtDynTableOutputPath": "{yt_dyntable_output}"
}}
""")

    cmd = [WRITE_YTDYNTABLE_UNVERSIONEDROW_BIN, f"--config-json={cfg_filename}"]

    execution = yatest.common.execute(cmd, wait=False)
    yt_queue_write(
        yt_client,
        input_queue,
        [
            (0, """{"key": "key1", "value": "value1"}"""),
            (0, """{"key": "key2", "value": "value2"}"""),
            (1, """{"key": "key3", "value": "value3"}"""),
            (1, """{"key": "key4", "value": "value4"}"""),
        ]
    )

    yt_queue_wait_consumed(yt_client, input_queue, "consumer", circuit_breaker=ExecutionWatcher(execution))
    execution.terminate()

    rows = read_yt_dyntable(yt_client, yt_dyntable_output)
    rows = [row for row in rows]
    expected_result = [
        {"shard": 0, "key": "key1", "value": "value1"},
        {"shard": 0, "key": "key2", "value": "value2"},
        {"shard": 1, "key": "key3", "value": "value3"},
        {"shard": 1, "key": "key4", "value": "value4"},
    ]
    assert (rows == expected_result)


@parametrize_enable_v2_graph_parsing
def test_logbroker_write(yt_client, yt_cluster, test_home_ypath, logbroker_stuff, enable_v2_graph_parsing: str):
    os.environ["SET_DEFAULT_LOCAL_YT_PARAMETERS"] = "1"

    input_queue = f"{test_home_ypath}/input"
    yt_queue_create(yt_client, input_queue)
    yt_queue_create_consumer(yt_client, input_queue, "consumer")

    logbroker_create_topic(logbroker_stuff, "output_topic")

    with open("filter_pipeline_config", "w") as outf:
        outf.write(f"""\
{{
    "RorenConfig": {{
        "EnableV2GraphParsing": {enable_v2_graph_parsing},
        "Consumers": [
            {{
                "ConsumingSystemConfig": {{
                    "Cluster": "{yt_cluster}",
                    "MainPath": "{test_home_ypath}/consuming_system",
                    "MaxShards": 1
                }}
            }}
        ],
        "EasyInputs": [
            {{
                "Alias": "input",
                "YtSupplier": {{
                    "Cluster": "{yt_cluster}",
                    "QueuePath": "{test_home_ypath}/input",
                    "QueueConsumer": "consumer"
                }}
            }}
        ]
    }},
    "LogbrokerOutput": {{
        "YDBPersQueueConfig": {{}},
        "ProducerConfig": {{
            "TopicConfig": {{
                "Topic": "output_topic",
                "Partitions": 1
            }},
            "ServerConfig": {{
                "Server": "{logbroker_stuff.host}",
                "Port": {logbroker_stuff.port}
            }}
        }}
    }}
}}
""")

    cmd = [FILTER_PIPELINE_BIN, "--config-json=filter_pipeline_config"]

    execution = yatest.common.execute(cmd, wait=False)
    yt_queue_write(
        yt_client,
        input_queue,
        [
            (0, """{"data": "foo", "keep": true, "seq_no": 0}"""),
            (0, """{"data": "bar", "seq_no": 1}"""),
            (0, """{"data": "qux", "keep": true, "seq_no": 2}"""),
        ]
    )

    yt_queue_wait_consumed(yt_client, input_queue, "consumer", circuit_breaker=ExecutionWatcher(execution))

    execution.terminate()
    actual = list(logbroker_read_topic(logbroker_stuff, "output_topic", 2))
    actual = [json.loads(gzip.decompress(a.data)) for a in actual]
    assert actual == [
        {"data": "foo", "keep": True, "seq_no": 0},
        {"data": "qux", "keep": True, "seq_no": 2},
    ]


@parametrize_enable_v2_graph_parsing
def test_metrics(yt_client, yt_cluster, test_home_ypath, enable_v2_graph_parsing: str):
    os.environ["SET_DEFAULT_LOCAL_YT_PARAMETERS"] = "1"

    input_queue = f"{test_home_ypath}/input"
    yt_queue_create(yt_client, input_queue)
    yt_queue_create_consumer(yt_client, input_queue, "consumer")

    with yatest.common.network.PortManager() as pm:
        http_port = pm.get_port()
        with open("metric_pipeline_config", "w") as outf:
            outf.write(f"""\
    {{
        "RorenConfig": {{
            "EnableV2GraphParsing": {enable_v2_graph_parsing},
            "Consumers": [
                {{
                    "ConsumingSystemConfig": {{
                        "Cluster": "{yt_cluster}",
                        "MainPath": "{test_home_ypath}/consuming_system",
                        "MaxShards": 1
                    }}
                }}
            ],
            "HttpServerConfig": {{
                "Port": {http_port}
            }},
            "EasyInputs": [
                {{
                    "Alias": "input",
                    "YtSupplier": {{
                        "Cluster": "{yt_cluster}",
                        "QueuePath": "{test_home_ypath}/input",
                        "QueueConsumer": "consumer"
                    }}
                }}
            ]
        }}
    }}
    """)

        cmd = [METRICS_PIPELINE_BIN, "--config-json=metric_pipeline_config"]

        execution = yatest.common.execute(cmd, wait=False)
        yt_queue_write(
            yt_client,
            input_queue,
            [
                (0, """{"data": "foo", "keep": true, "seq_no": 0}"""),
                (0, """{"data": "bar", "seq_no": 1}"""),
                (0, """{"data": "qux", "keep": true, "seq_no": 2}"""),
                (0, """{"data": "afsas", "seq_no": 1}"""),
                (0, """{"data": "sdsad", "keep": true, "seq_no": 2}"""),
            ]
        )

        yt_queue_wait_consumed(yt_client, input_queue, "consumer", circuit_breaker=ExecutionWatcher(execution))
        # TODO: check why we need sleep here
        time.sleep(10)

        sensors_json = urllib.request.urlopen(f"http://localhost:{http_port}/sensors").read()
        assert get_metric_value(sensors_json, "metrics_pipeline.total_processed") == 5
        execution.terminate()


@parametrize_enable_v2_graph_parsing
def test_in_memory_dict_resolver(yt_stuff, test_home_ypath, enable_v2_graph_parsing: str):
    os.environ["SET_DEFAULT_LOCAL_YT_PARAMETERS"] = "1"
    yt_cluster = yt_stuff.get_server()
    yt_client = yt_stuff.get_yt_client()

    test_home_ypath = test_home_ypath

    schema = [
        {"name": "key", "type": "string", "sort_order": "ascending"},
        {"name": "value", "type": "string"},
    ]
    dict_table = f"{test_home_ypath}/dict_table"
    yt_client.create("table", dict_table, attributes={"dynamic": True, "schema": schema})
    yt_client.mount_table(dict_table, sync=True)
    yt_client.insert_rows(dict_table, [
        {"key": "key1", "value": "dict1"},
        {"key": "key3", "value": "dict3"},
    ])

    input_queue = f"{test_home_ypath}/input"
    yt_queue_create(yt_client, input_queue, shards=2)
    yt_queue_create_consumer(yt_client, input_queue, "consumer")

    output_queue = f"{test_home_ypath}/output"
    yt_queue_create(yt_client, output_queue)

    with open("pipeline_config", "w") as outf:
        outf.write(f"""\
{{
    "RorenConfig": {{
        "EnableV2GraphParsing": {enable_v2_graph_parsing},
        "Consumers": [
            {{
                "ConsumingSystemConfig": {{
                    "Cluster": "{yt_cluster}",
                    "MainPath": "{test_home_ypath}/consuming_system",
                    "MaxShards": 2
                }}
            }}
        ],
        "EasyInputs": [
            {{
                "Alias": "input",
                "YtSupplier": {{
                    "Cluster": "{yt_cluster}",
                    "QueuePath": "{input_queue}",
                    "QueueConsumer": "consumer"
                }}
            }}
        ]
    }},
    "YtQueueOutput": {{
        "Path": "{output_queue}",
        "ShardsCount": 1,
        "CompressionCodec": "null"
    }},
    "DictTable": "{dict_table}",
    "DictRefreshPeriodMs": 1000
}}
""")

    cmd = [IN_MEMORY_DICT_RESOLVER_PIPELINE_BIN, "--config-json=pipeline_config"]

    execution = yatest.common.execute(cmd, wait=False)
    yt_queue_write(
        yt_client,
        input_queue,
        [
            (0, """{"key": "key1", "value": "foo"}"""),
            (0, """{"key": "key2", "value": "bar"}"""),
            (0, """{"key": "key3", "value": "qux"}"""),
        ]
    )

    yt_queue_wait_consumed(yt_client, input_queue, "consumer", circuit_breaker=ExecutionWatcher(execution))

    yt_client.insert_rows(dict_table, [
        {"key": "key1", "value": "dict1_new"},
        {"key": "key2", "value": "dict2_new"},
    ])

    time.sleep(5)

    yt_queue_write(
        yt_client,
        input_queue,
        [
            (1, """{"key": "key1", "value": "foo_new"}"""),
            (1, """{"key": "key2", "value": "bar_new"}"""),
            (1, """{"key": "key3", "value": "qux_new"}"""),
        ]
    )

    yt_queue_wait_consumed(yt_client, input_queue, "consumer", circuit_breaker=ExecutionWatcher(execution))

    execution.terminate()

    result = read_yt_queue(yt_client, output_queue)
    result = [(shard_idx, json.loads(data)) for shard_idx, data in result]
    assert result == [
        (0, {"key": "key1", "value": "foo_dict1"}),
        (0, {"key": "key2", "value": "bar_<none>"}),
        (0, {"key": "key3", "value": "qux_dict3"}),

        (0, {"key": "key1", "value": "foo_new_dict1_new"}),
        (0, {"key": "key2", "value": "bar_new_dict2_new"}),
        (0, {"key": "key3", "value": "qux_new_dict3"}),
    ]


@parametrize_enable_v2_graph_parsing
def test_multiple_consuming_systems(yt_stuff, test_home_ypath, enable_v2_graph_parsing: str):
    os.environ["SET_DEFAULT_LOCAL_YT_PARAMETERS"] = "1"
    yt_cluster = yt_stuff.get_server()
    yt_client = yt_stuff.get_yt_client()

    input_queue1 = f"{test_home_ypath}/input1"
    yt_queue_create(yt_client, input_queue1)
    yt_queue_create_consumer(yt_client, input_queue1, "cons1")

    input_queue2 = f"{test_home_ypath}/input2"
    yt_queue_create(yt_client, input_queue2)
    yt_queue_create_consumer(yt_client, input_queue2, "cons2")

    output_queue = f"{test_home_ypath}/output"
    yt_queue_create(yt_client, output_queue)

    with open("pipeline_config", "w") as outf:
        outf.write(f"""\
{{
    "RorenConfig": {{
        "EnableV2GraphParsing": {enable_v2_graph_parsing},
        "Consumers": [
            {{
                "ConsumingSystemConfig": {{
                    "Cluster": "{yt_cluster}",
                    "MainPath": "{test_home_ypath}/consuming_system1",
                    "MaxShards": 2
                }},
                "InputTag": "in1"
            }},
            {{
                "ConsumingSystemConfig": {{
                    "Cluster": "{yt_cluster}",
                    "MainPath": "{test_home_ypath}/consuming_system2",
                    "MaxShards": 2
                }},
                "InputTag": "in2"
            }}
        ],
        "EasyInputs": [
            {{
                "Alias": "in1",
                "YtSupplier": {{
                    "Cluster": "{yt_cluster}",
                    "QueuePath": "{input_queue1}",
                    "QueueConsumer": "cons1"
                }}
            }},
            {{
                "Alias": "in2",
                "YtSupplier": {{
                    "Cluster": "{yt_cluster}",
                    "QueuePath": "{input_queue2}",
                    "QueueConsumer": "cons2"
                }}
            }}
        ]
    }},
    "YtQueueOutput": {{
        "Path": "{output_queue}",
        "ShardsCount": 1,
        "CompressionCodec": "null"
    }}
}}
""")

    cmd = [MULTIPLE_CONSUMING_SYSTEMS_BIN, "--config-json=pipeline_config"]

    execution = yatest.common.execute(cmd, wait=False)
    yt_queue_write(
        yt_client,
        input_queue1,
        [
            (0, """{"data1": "foo1"}"""),
            (0, """{"data1": "bar1"}"""),
            (0, """{"data1": "qux1"}"""),
        ]
    )
    yt_queue_write(
        yt_client,
        input_queue2,
        [
            (0, """{"data2": "foo2"}"""),
            (0, """{"data2": "bar2"}"""),
            (0, """{"data2": "qux2"}"""),
        ]
    )

    yt_queue_wait_consumed(yt_client, input_queue1, "cons1", circuit_breaker=ExecutionWatcher(execution))
    yt_queue_wait_consumed(yt_client, input_queue2, "cons2", circuit_breaker=ExecutionWatcher(execution))

    execution.terminate()
    result = read_yt_queue(yt_client, output_queue)
    result = [(shard_idx, list(json.loads(data).items())) for shard_idx, data in result]
    assert sorted(result) == sorted([
        (0, [("data1", "foo1")]),
        (0, [("data1", "bar1")]),
        (0, [("data1", "qux1")]),
        (0, [("data2", "foo2")]),
        (0, [("data2", "bar2")]),
        (0, [("data2", "qux2")]),
    ])


@parametrize_enable_v2_graph_parsing
def test_multiple_inputs(yt_stuff, test_home_ypath, enable_v2_graph_parsing: str):
    os.environ["SET_DEFAULT_LOCAL_YT_PARAMETERS"] = "1"
    yt_cluster = yt_stuff.get_server()
    yt_client = yt_stuff.get_yt_client()

    input_queue1 = f"{test_home_ypath}/input1"
    yt_queue_create(yt_client, input_queue1)
    yt_queue_create_consumer(yt_client, input_queue1, "cons1")

    input_queue2 = f"{test_home_ypath}/input2"
    yt_queue_create(yt_client, input_queue2)
    yt_queue_create_consumer(yt_client, input_queue2, "cons2")

    output_queue = f"{test_home_ypath}/output"
    yt_queue_create(yt_client, output_queue)

    with open("pipeline_config", "w") as outf:
        outf.write(f"""\
{{
    "RorenConfig": {{
        "EnableV2GraphParsing": {enable_v2_graph_parsing},
        "Consumers": [
            {{
                "ConsumingSystemConfig": {{
                    "Cluster": "{yt_cluster}",
                    "MainPath": "{test_home_ypath}/consuming_system",
                    "MaxShards": 2
                }},
                "InputTag": "tag"
            }}
        ],
        "Inputs": [
            {{
                "InputTag": "tag",
                "Suppliers": [
                    {{
                        "Alias": "input1",
                        "YtSupplier": {{
                            "Cluster": "{yt_cluster}",
                            "QueuePath": "{input_queue1}",
                            "QueueConsumer": "cons1"
                        }}
                    }},
                    {{
                        "Alias": "input2",
                        "YtSupplier": {{
                            "Cluster": "{yt_cluster}",
                            "QueuePath": "{input_queue2}",
                            "QueueConsumer": "cons2"
                        }}
                    }}
                ]
            }}
        ]
    }},
    "YtQueueOutput": {{
        "Path": "{output_queue}",
        "ShardsCount": 1,
        "CompressionCodec": "null"
    }}
}}
""")

    cmd = [MULTIPLE_CONSUMING_SYSTEMS_BIN, "--config-json=pipeline_config"]

    execution = yatest.common.execute(cmd, wait=False)
    yt_queue_write(
        yt_client,
        input_queue1,
        [
            (0, """{"data1": "foo1"}"""),
            (0, """{"data1": "bar1"}"""),
            (0, """{"data1": "qux1"}"""),
        ]
    )
    yt_queue_write(
        yt_client,
        input_queue2,
        [
            (0, """{"data2": "foo2"}"""),
            (0, """{"data2": "bar2"}"""),
            (0, """{"data2": "qux2"}"""),
        ]
    )

    yt_queue_wait_consumed(yt_client, input_queue1, "cons1", circuit_breaker=ExecutionWatcher(execution))
    yt_queue_wait_consumed(yt_client, input_queue2, "cons2", circuit_breaker=ExecutionWatcher(execution))

    execution.terminate()
    result = read_yt_queue(yt_client, output_queue)
    result = [(shard_idx, list(json.loads(data).items())) for shard_idx, data in result]
    assert sorted(result) == sorted([
        (0, [("data1", "foo1")]),
        (0, [("data1", "bar1")]),
        (0, [("data1", "qux1")]),
        (0, [("data2", "foo2")]),
        (0, [("data2", "bar2")]),
        (0, [("data2", "qux2")]),
    ])


def test_stateful_processing(yt_cluster, yt_client, test_home_ypath):
    os.environ["SET_DEFAULT_LOCAL_YT_PARAMETERS"] = "1"

    state_schema = [
        {"name": "key", "type": "string", "sort_order": "ascending"},
        {"name": "value", "type": "int64"},
    ]
    state_table = f"{test_home_ypath}/state_table"
    yt_client.create("table", state_table, attributes={"dynamic": True, "schema": state_schema})
    yt_client.mount_table(state_table, sync=True)

    input_queue = f"{test_home_ypath}/input"
    yt_queue_create(yt_client, input_queue, shards=2)
    yt_queue_create_consumer(yt_client, input_queue, "consumer")

    output_queue = f"{test_home_ypath}/output"
    yt_queue_create(yt_client, output_queue)

    with open("pipeline_config", "w") as outf:
        outf.write(f"""\
{{
    "RorenConfig": {{
        "EnableV2GraphParsing": true,
        "Consumers": [
            {{
                "ConsumingSystemConfig": {{
                    "Cluster": "{yt_cluster}",
                    "MainPath": "{test_home_ypath}/consuming_system",
                    "MaxShards": 1
                }}
            }}
        ],
        "EasyInputs": [
            {{
                "Alias": "input",
                "YtSupplier": {{
                    "Cluster": "{yt_cluster}",
                    "QueuePath": "{test_home_ypath}/input",
                    "QueueConsumer": "consumer"
                }}
            }}
        ]
    }},
    "YtQueueOutput": {{
        "Path": "{output_queue}",
        "ShardsCount": 1,
        "CompressionCodec": "null"
    }},
    "KvStateManagerConfig": {{
        "StateTable": "{state_table}",
        "KeyColumn": "key",
        "ValueColumn": "value"
    }}
}}
""")

    cmd = [STATEFUL_PIPELINE_BIN, "--config-json=pipeline_config"]

    execution = yatest.common.execute(cmd, wait=False)
    yt_queue_write(
        yt_client,
        input_queue,
        [
            (0, """{"key": "key1", "value": 1}"""),
            (0, """{"key": "key2", "value": 2}"""),
            (0, """{"key": "key3", "value": 3}"""),
            (0, """{"key": "key1", "value": 4}"""),
            (0, """{"key": "key2", "value": 5}"""),
            (0, """{"key": "key3", "value": 6}"""),
        ]
    )

    yt_queue_wait_consumed(yt_client, input_queue, "consumer", circuit_breaker=ExecutionWatcher(execution))

    assert list(read_yt_dyntable(yt_client, state_table)) == [
        {"key": "key1", "value": 5},
        {"key": "key2", "value": 7},
        {"key": "key3", "value": 9},
    ]

    yt_queue_write(
        yt_client,
        input_queue,
        [
            (0, """{"key": "key1", "value": 9}"""),
            (0, """{"key": "key2", "value": 8}"""),
            (0, """{"key": "key3", "value": 7}"""),
            (0, """{"key": "key1", "value": 6}"""),
            (0, """{"key": "key2", "value": 5}"""),
            (0, """{"key": "key3", "value": 4}"""),
        ]
    )

    yt_queue_wait_consumed(yt_client, input_queue, "consumer", circuit_breaker=ExecutionWatcher(execution))

    assert list(read_yt_dyntable(yt_client, state_table)) == [
        {"key": "key1", "value": 20},
        {"key": "key2", "value": 20},
        {"key": "key3", "value": 20},
    ]
