import itertools
import json
import os
import os.path

import pytest

import yatest.common

import library.python.ydb.federated_topic_client as federated_topic_client

import contrib.ydb.library.yql.tools.solomon_emulator.client.client as solomon_emulator_client

import logbroker.tools.lib.recipe_helpers.cm_requests as cm_requests

from library.python.port_manager import PortManager

from contextlib import closing

from yt.environment import init_operations_archive
from yt.environment.helpers import assert_items_equal
from yt.wrapper.flow_commands import PipelineState
from yt.yql.tests.common.test_framework.test_utils import (
    wait_pipeline_state_or_failed_jobs,
    create_flow_logs_replicators,
    dump_pipeline_jobs_stderr,
)

from yt_commands import (
    authors, create, sync_mount_table, insert_rows, select_rows,
    list_queue_consumer_registrations, raises_yt_error,
)

from yt_queries import start_query
from yt_queue_agent_test_base import TestQueueAgentBase


LOGBROKER_FEDERATION_RECIPE_BINARY = yatest.common.binary_path(
    "kikimr/public/tools/federation_recipe/federation_recipe"
)

SOLOMON_EMULATOR_RECIPE_BINARY = yatest.common.binary_path(
    "contrib/ydb/library/yql/tools/solomon_emulator/recipe/solomon_recipe"
)

ENV_FILE = yatest.common.work_path("env.json.txt")


def has_logbroker_federation():
    return os.getenv("CM_PORT") is not None


def get_logbroker_endpoint(cluster):
    return f"localhost:{os.getenv(f'{cluster}_port')}"


class LogbrokerClient:
    # By default logbroker federation recipe creates two clusters named 'cluster_a' and 'cluster_b'
    # and two accounts named `prod` and `test`.
    # Each account also already contains 1 topic and 1 consumer named `topic` and `consumer` correspondingly.
    LOGBROKER_CLUSTER = "cluster_a"
    LOGBROKER_ACCOUNT = "test"
    LOGBROKER_DATABASE = "/Root/logbroker-federation/" + LOGBROKER_ACCOUNT
    LOGBROKER_TOPIC = "topic"

    def __init__(self, endpoint, database, cm, test_id):
        self._endpoint = endpoint
        self._database = database
        self._cm = cm
        self._test_id = test_id

        self._federation_driver = federated_topic_client.FederationDriver(
            endpoint, database
        )

        self._federation_driver.wait_init()

        self._created_topics = []
        self._topic_index_generator = itertools.count()

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def database(self):
        return self._database

    @property
    def cm(self):
        return self._cm

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def create_topic(self):
        topic_index = next(self._topic_index_generator)
        topic_path = self._test_id + "." + self.LOGBROKER_TOPIC + str(topic_index)
        full_topic_path = self.LOGBROKER_ACCOUNT + "/" + topic_path
        self._cm.exec_request((
            cm_requests.request_create_topic(full_topic_path),
        ))

        self._created_topics.append(full_topic_path)

        return topic_path

    def create_topic_writer(self, topic_path):
        return self._federation_driver.topic_writer(topic=topic_path)

    def create_topic_reader(self, topic_path, consumer):
        return self._federation_driver.topic_reader(topic=topic_path, consumer=consumer)

    def close(self):
        if self._created_topics:
            self._cm.exec_request(tuple(
                cm_requests.request_remove_topic(topic) for topic in self._created_topics
            ))

        self._federation_driver.close()


def load_env():
    with open(ENV_FILE, "r") as env_file:
        for line in env_file:
            for key, value in json.loads(line.strip()).items():
                os.environ[key] = value


def get_test_id(request):
    prefix_parts = [
        request.cls.__name__.lower() if request.cls else "test",
        request.function.__name__.lower()
    ]

    if hasattr(request.node, "callspec"):
        indices = request.node.callspec.indices
        for param in request.node.callspec.params.keys():
            prefix_parts.append(str(indices[param]))

    return ".".join(prefix_parts)


@pytest.fixture(scope="session")
def logbroker_federation():
    common_args = [
        "--build-root", yatest.common.build_path(),
        "--source-root", yatest.common.source_path(),
        "--output-dir", yatest.common.output_path(),
        "--env-file", ENV_FILE,
    ]

    yatest.common.process.execute(
        command=[LOGBROKER_FEDERATION_RECIPE_BINARY, "start"] + common_args,
    )

    load_env()

    yield

    yatest.common.process.execute(
        command=[LOGBROKER_FEDERATION_RECIPE_BINARY, "stop"] + common_args,
    )


@pytest.fixture
def logbroker_client(request, logbroker_federation):
    if not has_logbroker_federation():
        pytest.skip("Logbroker federation is not available")

    cm_port = os.getenv("CM_PORT")

    logbroker_cluster = LogbrokerClient.LOGBROKER_CLUSTER
    logbroker_endpoint = get_logbroker_endpoint(logbroker_cluster)

    cm = cm_requests.CMApiHelper(f'localhost:{cm_port}')

    test_id = get_test_id(request)

    with LogbrokerClient(
        endpoint=logbroker_endpoint,
        database=LogbrokerClient.LOGBROKER_DATABASE,
        cm=cm,
        test_id=test_id,
    ) as client:
        yield client


def has_solomon_emulator():
    return os.getenv("SOLOMON_HTTP_PORT") is not None


def get_solomon_endpoint():
    return f"localhost:{os.getenv("SOLOMON_HTTP_PORT")}"


class SolomonClient:
    SOLOMON_PROJECT = "project"
    SOLOMON_SERVICE = "service"
    SOLOMON_CLUSTER = "cluster"

    def __init__(self, endpoint, test_id):
        self._endpoint = endpoint
        self._test_id = test_id

        solomon_emulator_client.config_solomon(response_code=200)

        self._created_shards = []
        self._shard_index_generator = itertools.count()

    @property
    def endpoint(self):
        return self._endpoint

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def create_shard(self):
        shard_index = next(self._shard_index_generator)
        shard_name = "/".join([
            self.SOLOMON_PROJECT,
            self.SOLOMON_CLUSTER,
            self._test_id + "." + self.SOLOMON_SERVICE + str(shard_index),
        ])

        self._created_shards.append(shard_name)

        return shard_name

    def get_metrics(self, shard_name):
        project, cluster, shard = shard_name.split("/")
        return solomon_emulator_client.get_solomon_metrics(project, cluster, shard)

    def cleanup(self, shard_name):
        project, cluster, shard = shard_name.split("/")
        solomon_emulator_client.cleanup_solomon(project, cluster, shard)

    def close(self):
        for shard_name in self._created_shards:
            self.cleanup(shard_name)

        self._created_shards = []


@pytest.fixture(scope="session")
def solomon_emulator():
    common_args = [
        "--build-root", yatest.common.build_path(),
        "--source-root", yatest.common.source_path(),
        "--output-dir", yatest.common.output_path(),
        "--env-file", ENV_FILE,
    ]

    yatest.common.process.execute(
        command=[SOLOMON_EMULATOR_RECIPE_BINARY, "start"] + common_args + [
            "--shard", "my_project/my_cluster/my_service",
        ]
    )

    load_env()

    yield

    yatest.common.process.execute(
        command=[SOLOMON_EMULATOR_RECIPE_BINARY, "stop"] + common_args,
    )


@pytest.fixture
def solomon_client(request, solomon_emulator):
    if not has_solomon_emulator():
        pytest.skip("Solomon emulator is not available")

    solomon_endpoint = get_solomon_endpoint()
    test_id = get_test_id(request)

    with SolomonClient(endpoint=solomon_endpoint, test_id=test_id) as client:
        yield client


class TestYtflowBase(TestQueueAgentBase):
    NUM_MASTERS = 1
    NUM_DISCOVERY_SERVERS = 1
    NUM_NODES = 3
    NUM_HTTP_PROXIES = 1
    NUM_RPC_PROXIES = 1
    NUM_SCHEDULERS = 1
    NUM_YQL_AGENTS = 1
    NUM_QUEUE_AGENTS = 1
    NUM_QUERY_TRACKER = 1
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True
    USE_DYNAMIC_TABLES = True

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    COPY_YTSERVER = False

    MAX_YQL_VERSION = '2025.04'
    DEFAULT_YQL_UI_VERSION = '2025.01'

    PIPELINE_PATH = '//tmp/pipeline'

    YT_TABLE_PATH = '//tmp/Table'
    YT_CONSUMER_PATH = '//tmp/main_consumer'
    YT_PRODUCER_PATH = '//tmp/main_producer'

    # By default logbroker federation recipe creates two clusters named 'cluster_a' and 'cluster_b'
    # and two accounts named `prod` and `test`.
    # Each account also already contains 1 topic and 1 consumer named `topic` and `consumer` correspondingly.
    LOGBROKER_CONSUMER = "consumer"
    LOGBROKER_COMPRESSION_CODEC = "raw"
    LOGBROKER_COMPRESSION_LEVEL = "0"

    # Solomon emulator creates project, service and cluster on first write
    SOLOMON_PROJECT = "project"
    SOLOMON_SERVICE = "service"
    SOLOMON_CLUSTER = "cluster"

    def setup_method(self, method):
        super(TestYtflowBase, self).setup_method(method)
        init_operations_archive.create_tables_latest_version(self.Env.create_client())

    @classmethod
    def modify_yql_agent_config(cls, config):
        config['yql_agent']['ytflow_gateway_config'] = dict(
            ytflow_worker_bin=yatest.common.binary_path("yt/yql/tools/ytflow_worker/ytflow_worker"),
            gateway_threads=1,
            default_settings=[
                dict(name='_RpcTimeout', value='10s'),
                dict(name='_FiniteStreams', value='1'),
                dict(name='_UseCpuAwareBalancer', value='false'),
                dict(name='_ControllerWriteFullLogsToYT', value='true'),
                dict(name='_ControllerWriteLogsToFile', value='false'),
                dict(name='_ControllerLogLevel', value='debug'),
                dict(name='_WorkerWriteLogsToYT', value='true'),
                dict(name='_WorkerWriteLogsToFile', value='false'),
                dict(name='_WorkerLogLevel', value='debug'),
                dict(name='YtPartitionCount', value='1'),
            ],
            cluster_mapping=[dict(
                name=cls.Env.id,
                real_name=cls.Env.id,
                proxy_url=cls.Env.get_http_proxy_address(),
            )],
        )

        if has_logbroker_federation():
            logbroker_endpoint = get_logbroker_endpoint(LogbrokerClient.LOGBROKER_CLUSTER)
            config['yql_agent']['pq_gateway_config'] = dict(
                cluster_mapping=[dict(
                    name='logbroker',
                    endpoint=logbroker_endpoint,
                    database=LogbrokerClient.LOGBROKER_DATABASE,
                )]
            )

        config['yql_agent']['solomon_gateway_config'] = dict(
            cluster_mapping=[dict(
                name='solomon',
                cluster=get_solomon_endpoint(),
            )]
        )

        yt_gateway_config = config['yql_agent']['gateway_config']
        yt_gateway_config['mr_job_udfs_dir'] = ";".join([
            yt_gateway_config['mr_job_udfs_dir'],
            yatest.common.binary_path("yt/yql/tests/agent/throwing_udf"),
        ])

    @pytest.fixture(autouse=True)
    def setup_yt_utils(self):
        self.yt_table_index_generator = itertools.count()

    def _create_yt_table(self, input_table_attrs):
        table_idx = next(self.yt_table_index_generator)
        table_path = self.YT_TABLE_PATH + str(table_idx)
        input_table_attrs.update(dynamic=True)
        create("table", table_path, attributes=input_table_attrs)
        sync_mount_table(table_path)
        return table_path

    def _write_yt_table(self, table_path, rows):
        insert_rows(table_path, rows)

    def _read_yt_table(self, table_path):
        result = list(select_rows(f"* from [{table_path}]"))
        self._remove_system_columns(result)
        return result

    def _assert_yt_table_content(self, table_path, expected_rows):
        assert_items_equal(self._read_yt_table(table_path), expected_rows)

    def _write_logbroker_topic(self, topic_path, data, logbroker_client):
        with closing(logbroker_client.create_topic_writer(topic_path)) as topic_writer:
            topic_writer.write(data)

    def _read_logbroker_topic(self, topic_path, logbroker_client):
        with closing(
            logbroker_client.create_topic_reader(topic_path, self.LOGBROKER_CONSUMER)
        ) as topic_reader:
            batch = topic_reader.receive_batch()
            topic_reader.commit_with_ack(batch)
            return batch.messages

    def _assert_logbroker_topic_content(self, topic_path, expected_data, logbroker_client):
        actual_data = [
            lb_data.message.data.decode()
            for lb_data in self._read_logbroker_topic(topic_path, logbroker_client)
        ]

        assert_items_equal(actual_data, expected_data)

    def _assert_solomon_shard_content(self, shard_name, expected_data, solomon_client, sensors={"counter"}):
        result_metrics = solomon_client.get_metrics(shard_name)

        expected_metrics = []
        for row in expected_data:
            timestamp = None
            labels = {}
            sensor_values = {}
            for key, value in row.items():
                if key in sensors:
                    sensor_values[key] = value
                elif isinstance(value, str):
                    labels[key] = value
                elif isinstance(value, int) or value is None:
                    timestamp = value
                else:
                    assert False, f"Unexpected type of value: {type(value)}"

            if timestamp is None:
                continue

            for sensor_name, value in sensor_values.items():
                metric = {
                    "labels": sorted([["sensor", sensor_name]] + [[key, value] for key, value in labels.items()]),
                    "value": value,
                    "ts": timestamp
                }
                expected_metrics.append(metric)

        assert_items_equal(result_metrics, sorted(expected_metrics, key=lambda x: (x["ts"], x["labels"])))

    def _convert_solomon_metrics_to_yt_format(self, metrics, timestamp_column="metric_timestamp"):
        def convert_timestamp(metric):
            converted_metric = metric.copy()
            if converted_metric[timestamp_column] is not None:
                converted_metric[timestamp_column] *= 1000000
            return converted_metric
        return list(map(convert_timestamp, metrics))

    @pytest.fixture
    def run_query(self, request):
        def impl(query_text):
            with PortManager() as port_manager:
                pipeline_path = self.PIPELINE_PATH

                query_text_header = f"""
use primary;

pragma Engine = "ytflow";

pragma Ytflow.Cluster = "primary";
pragma Ytflow.PipelinePath = "{pipeline_path}";

pragma Ytflow.YtConsumerPath = "{self.YT_CONSUMER_PATH}";
pragma Ytflow.YtProducerPath = "{self.YT_PRODUCER_PATH}";
pragma Ytflow.ControllerRpcPort = "{port_manager.get_port()}";
pragma Ytflow.ControllerMonitoringPort = "{port_manager.get_port()}";
pragma Ytflow.WorkerRpcPort = "{port_manager.get_port()}";
pragma Ytflow.WorkerMonitoringPort = "{port_manager.get_port()}";
pragma Ytflow.ControllerCount = "1";

pragma Ytflow.WorkerCount = "1";

pragma Ytflow.LogbrokerConsumerPath = "{self.LOGBROKER_CONSUMER}";
pragma Ytflow.LogbrokerWriteCompressionCodec = "{self.LOGBROKER_COMPRESSION_CODEC}";
pragma Ytflow.LogbrokerWriteCompressionLevel = "{self.LOGBROKER_COMPRESSION_LEVEL}";
"""

                query_text = '\n'.join([query_text_header, query_text])

                client = self.Env.create_client()

                test_id = get_test_id(request)

                controller_logs_replicator, worker_logs_replicator = create_flow_logs_replicators(
                    self.PIPELINE_PATH,
                    yatest.common.output_path(),
                    logs_batch_size=1000,
                    output_file_prefix=test_id,
                    yt_client=client)

                with controller_logs_replicator, worker_logs_replicator:
                    query = start_query("yql", query_text)
                    query.track()

                    try:
                        wait_pipeline_state_or_failed_jobs(
                            PipelineState.Completed, pipeline_path,
                            client=client,
                            timeout=600)

                    finally:
                        dump_pipeline_jobs_stderr(
                            self.PIPELINE_PATH,
                            os.path.join(yatest.common.output_path(), f"{test_id}_pipeline_jobs.stderr"),
                            client=client)

        return impl

    def _remove_system_columns(self, rows):
        system_columns = ["$tablet_index", "$row_index", "$timestamp", "$cumulative_data_weight"]

        for row in rows:
            for system_column in system_columns:
                row.pop(system_column, None)

    def _make_queue_schema(self, schema):
        return [
            {"name": "$timestamp", "type": "uint64"},
            {"name": "$cumulative_data_weight", "type": "int64"},
        ] + schema


class TestYtflow(TestYtflowBase):
    NUM_TEST_PARTITIONS = 16

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_select(self, query_tracker, yql_agent, run_query):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "string_field", "type": "string"},
                {"name": "int64_field", "type": "int64"},
            ]),
        ))
        self._write_yt_table(input_table_path, [
            {"string_field": "foo", "int64_field": 1},
            {"string_field": "bar", "int64_field": 10},
            {"string_field": "foobar", "int64_field": 100},
        ])

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "string_field", "type": "string"},
                {"name": "int64_field", "type": "int64"},
                {"name": "bool_field", "type": "boolean"},
            ]),
        ))

        run_query(f"""
insert into `{out_table_path}`
select
    string_field || "_ytflow" as string_field,
    int64_field * 100 as int64_field,
    int64_field > 10 as bool_field
from `{input_table_path}`
where string_field = "foo" or int64_field >= 100;
""")

        self._assert_yt_table_content(out_table_path, [
            {"string_field": "foo_ytflow", "int64_field": 100, "bool_field": False},
            {"string_field": "foobar_ytflow", "int64_field": 10000, "bool_field": True},
        ])

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_throwing_udf(self, query_tracker, yql_agent, run_query):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "value", "type": "string"},
                {"name": "need_throw", "type": "boolean"},
            ]),
        ))
        self._write_yt_table(input_table_path, [
            {"value": "foo", "need_throw": False},
            {"value": "bar", "need_throw": True},
            {"value": "foobar", "need_throw": False},
        ])

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "parsed_value", "type": "string"},
            ]),
        ))

        run_query(f"""
insert into `{out_table_path}`
select
    ThrowingUdf::ParseWithThrow(value, need_throw) as parsed_value
from `{input_table_path}`
""")

        self._assert_yt_table_content(out_table_path, [
            {"parsed_value": "foo"},
            {'parsed_value': '(yexception) yt/yql/tests/agent/throwing_udf/throwing_udf.cpp:14: expected exception'},
            {"parsed_value": "foobar"},
        ])

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_udf_terminate(self, query_tracker, yql_agent, run_query):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "value", "type": "string"},
            ]),
        ))

        self._write_yt_table(input_table_path, [
            {"value": "foo"},
        ])

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "value", "type": "string"},
            ]),
        ))

        query = f"""
$lambda = ($row) -> {{
    $value = If($row.value is not null, Nothing(String?), $row.value);
    return AsStruct(
        Unwrap($value) as value,
    );
}};

$stream = process `{input_table_path}` using $lambda(TableRow());

insert into `{out_table_path}`
select * from $stream;
"""

        with raises_yt_error("Failed to unwrap empty optional"):
            run_query(query)

    @authors("ngc224")
    @pytest.mark.timeout(180)
    @pytest.mark.parametrize("vital", [False, True])
    def test_consumer_vitality(self, query_tracker, yql_agent, run_query, vital):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "value", "type": "string"},
            ]),
        ))
        self._write_yt_table(input_table_path, [
            {"value": "foo"},
            {"value": "bar"},
        ])

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "value", "type": "string"},
            ]),
        ))

        run_query(f"""
pragma Ytflow.YtConsumerVital = "{vital}";

insert into `{out_table_path}`
select value
from `{input_table_path}`
""")

        self._assert_yt_table_content(out_table_path, [
            {"value": "foo"},
            {"value": "bar"},
        ])

        registrations = list_queue_consumer_registrations(
            queue_path=input_table_path,
            consumer_path=self.YT_CONSUMER_PATH,
        )

        assert len(registrations) == 1
        assert registrations[0]["vital"] == vital

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_multiple_outputs_in_lambda(self, query_tracker, yql_agent, run_query):
        FIELD_GOOD = "int64_field"
        FIELD_BAD = "string_field"

        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": FIELD_GOOD, "type": "int64"},
            ]),
        ))
        self._write_yt_table(input_table_path, [
            {FIELD_GOOD: 1},
            {FIELD_GOOD: 10},
            {FIELD_GOOD: 100},
        ])

        out_table_good_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": FIELD_GOOD, "type": "int64"},
            ]),
        ))
        out_table_bad_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": FIELD_BAD, "type": "string"},
            ]),
        ))

        run_query(f"""
$lambda = ($row) -> {{
    $good_row_type = TypeOf($row);
    $bad_row_type = Struct<'{FIELD_BAD}':optional<string>>;
    $variant_type = Variant<$good_row_type, $bad_row_type>;

    $val = $row.{FIELD_GOOD};
    return If(
        $val == 10,
        Variant($row, "0", $variant_type),
        Variant(<|{FIELD_BAD}:cast($val as optional<string>)|>, "1", $variant_type)
    );
}};

$good_stream, $bad_stream = process `{input_table_path}` using $lambda(TableRow());

insert into `{out_table_good_path}`
select * from $good_stream;

insert into `{out_table_bad_path}`
select * from $bad_stream;
""")

        self._assert_yt_table_content(out_table_good_path, [
            {FIELD_GOOD: 10},
        ])
        self._assert_yt_table_content(out_table_bad_path, [
            {FIELD_BAD: "1"},
            {FIELD_BAD: "100"},
        ])

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_using_same_stream_in_multiple_sinks(self, query_tracker, yql_agent, run_query):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "value", "type": "int64"},
            ]),
        ))
        input_data = [{"value": value} for value in range(5)]
        self._write_yt_table(input_table_path, input_data)

        out_table_paths = [self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "value", "type": "int64"},
            ]),
        )) for _ in range(2)]

        run_query(f"""
$stream = select value + 1 as value from `{input_table_path}`;

insert into `{out_table_paths[0]}`
select * from $stream;

insert into `{out_table_paths[1]}`
select * from $stream;
""")

        expected_data = [{"value": row["value"] + 1} for row in input_data]
        for out_table in out_table_paths:
            self._assert_yt_table_content(out_table, expected_data)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    @pytest.mark.parametrize("column_name", ["Value", "UnexpectedColumnName"])
    def test_with_truncate(self, query_tracker, yql_agent, run_query, column_name):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Value", "type": "int64"},
            ]),
        ))
        input_data = [{"Value": value} for value in range(5)]
        self._write_yt_table(input_table_path, input_data)

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": column_name, "type": "int64"},
            ]),
        ))

        run_query(f"""
$stream = select Value + 1 as Value from `{input_table_path}`;

insert into `{out_table_path}` with truncate
select * from $stream;
""")

        expected_data = [{"Value": row["Value"] + 1} for row in input_data]
        self._assert_yt_table_content(out_table_path, expected_data)


class TestYtflowLogbroker(TestYtflowBase):
    NUM_TEST_PARTITIONS = 16

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_logbroker_read(self, query_tracker, yql_agent, run_query, logbroker_client):
        input_topic_path = logbroker_client.create_topic()
        self._write_logbroker_topic(input_topic_path, ["a", "b", "c"], logbroker_client)

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Data", "type": "string"},
            ]),
        ))

        run_query(f"""
$stream = select Data || "_ytflow" as Data from logbroker.`{input_topic_path}`;

insert into `{out_table_path}`
select * from $stream;
""")

        self._assert_yt_table_content(out_table_path, [
            {"Data": "a_ytflow"},
            {"Data": "b_ytflow"},
            {"Data": "c_ytflow"},
        ])

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_logbroker_write(self, query_tracker, yql_agent, run_query, logbroker_client):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Data", "type": "string"},
            ]),
        ))
        self._write_yt_table(input_table_path, [
            {"Data": "AB"},
            {"Data": "CD"},
            {"Data": "EF"},
        ])

        out_topic_path = logbroker_client.create_topic()

        run_query(f"""
$stream = select coalesce(Data, "Empty!") as Data from `{input_table_path}`;

insert into logbroker.`{out_topic_path}`
select * from $stream;
""")

        self._assert_logbroker_topic_content(out_topic_path, ["AB", "CD", "EF"], logbroker_client)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_read_yt_write_yt_logbroker(self, query_tracker, yql_agent, run_query, logbroker_client):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Data", "type": "string"},
            ]),
        ))
        self._write_yt_table(input_table_path, [
            {"Data" : "yt"},
            {"Data" : "logbroker"},
            {"Data" : "logbroker"},
        ])

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Data", "type": "string"},
            ]),
        ))
        out_topic_path = logbroker_client.create_topic()

        run_query(f"""
$stream = select * from `{input_table_path}`;

$lambda = ($row) -> {{
    $good_row_type = Struct<'Data':optional<string>>;
    $bad_row_type = Struct<'Data':string>;
    $variant_type = Variant<$good_row_type, $bad_row_type>;

    return If(
        $row.Data == "yt",
        Variant($row, "0", $variant_type),
        Variant(<|Data:coalesce($row.Data, "Empty!")|>, "1", $variant_type)
    );
}};

$good_stream, $bad_stream = process $stream using $lambda(TableRow());

insert into `{out_table_path}`
select * from $good_stream;

insert into logbroker.`{out_topic_path}`
select * from $bad_stream;
""")

        self._assert_yt_table_content(out_table_path, [
            {"Data": "yt"}
        ])
        self._assert_logbroker_topic_content(out_topic_path, ["logbroker", "logbroker"], logbroker_client)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_read_logbroker_write_yt_logbroker(self, query_tracker, yql_agent, run_query, logbroker_client):
        input_topic_path = logbroker_client.create_topic()
        self._write_logbroker_topic(input_topic_path, ["yt", "logbroker", "logbroker"], logbroker_client)

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Data", "type": "string", "required": True},
            ]),
        ))
        out_topic_path = logbroker_client.create_topic()

        run_query(f"""
$stream = select * from logbroker.`{input_topic_path}`;

$lambda = ($row) -> {{
    $row_type = TypeOf($row);
    $variant_type = Variant<$row_type, $row_type>;

    return If(
        $row.Data == "yt",
        Variant($row, "0", $variant_type),
        Variant($row, "1", $variant_type)
    );
}};

$good_stream, $bad_stream = process $stream using $lambda(TableRow());

insert into `{out_table_path}`
select * from $good_stream;

insert into logbroker.`{out_topic_path}`
select * from $bad_stream;
""")

        self._assert_yt_table_content(out_table_path, [
            {"Data": "yt"}
        ])
        self._assert_logbroker_topic_content(out_topic_path, ["logbroker", "logbroker"], logbroker_client)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_many_logbroker_outputs(self, query_tracker, yql_agent, run_query, logbroker_client):
        input_topic_path = logbroker_client.create_topic()
        self._write_logbroker_topic(input_topic_path, [str(i) for i in range(5)], logbroker_client)

        out_topics = [logbroker_client.create_topic() for _ in range(5)]

        run_query(f"""
$stream = select * from logbroker.`{input_topic_path}`;

$lambda = ($row) -> {{
    $row_type = TypeOf($row);
    $variant_type = Variant<$row_type, $row_type, $row_type, $row_type, $row_type>;

    return case $row.Data
        {"\n".join([f"""when "{i}" then Variant($row, "{i}", $variant_type)""" for i in range(5)])}
        else Variant(<|Data:"Unexpected!"|>, "0", $variant_type)
    end;
}};

$stream0, $stream1, $stream2, $stream3, $stream4 = process $stream using $lambda(TableRow());

{"\n".join([f"""insert into logbroker.`{out_topic_path}` select * from $stream{idx};""" for idx, out_topic_path in enumerate(out_topics)])}

""")
        for idx, out_topic in enumerate(out_topics):
            self._assert_logbroker_topic_content(out_topic, [f"{idx}"], logbroker_client)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_yt_yt_logbroker_output(self, query_tracker, yql_agent, run_query, logbroker_client):
        input_topic_path = logbroker_client.create_topic()
        self._write_logbroker_topic(input_topic_path, [str(i) for i in range(3)], logbroker_client)

        out_tables = [self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Data", "type": "string", "required": True},
            ]),
        )) for _ in range(2)]

        out_topic_path = logbroker_client.create_topic()

        run_query(f"""
$stream = select * from logbroker.`{input_topic_path}`;

$lambda = ($row) -> {{
    $row_type = TypeOf($row);
    $variant_type = Variant<$row_type, $row_type, $row_type>;

    return case $row.Data
        {"\n".join([f"""when "{i}" then Variant($row, "{i}", $variant_type)""" for i in range(3)])}
        else Variant(<|Data:"Unexpected!"|>, "0", $variant_type)
    end;
}};

$stream0, $stream1, $stream2 = process $stream using $lambda(TableRow());

insert into `{out_tables[0]}`
select * from $stream0;

insert into `{out_tables[1]}`
select * from $stream1;

insert into logbroker.`{out_topic_path}`
select * from $stream2;
""")

        for idx, out_table in enumerate(out_tables):
            self._assert_yt_table_content(out_table, [
                {"Data": f"{idx}"}
            ])

        self._assert_logbroker_topic_content(out_topic_path, ["2"], logbroker_client)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_remove_system_columns_from_write(self, query_tracker, yql_agent, run_query, logbroker_client):
        input_topic_path = logbroker_client.create_topic()
        input_data = [str(i) for i in range(3)]
        self._write_logbroker_topic(input_topic_path, input_data, logbroker_client)

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Data", "type": "string"},
            ]),
        ))

        run_query(f"""
$stream = select * from logbroker.`{input_topic_path}`;

$lambda = ($row) -> {{
    return ReplaceMember($row, "Data", $row.Data || "_processed");
}};

$processed_stream = process $stream using $lambda(TableRow());

insert into `{out_table_path}`
select * from $processed_stream;
""")

        self._assert_yt_table_content(out_table_path, [{"Data": data + "_processed"} for data in input_data])


class TestYtflowSolomon(TestYtflowBase):
    NUM_TEST_PARTITIONS = 16

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_solomon_write(self, query_tracker, yql_agent, run_query, solomon_client):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "counter", "type": "uint64"},
                {"name": "metric_timestamp", "type": "timestamp"},
                {"name": "label", "type": "string", "required": True}
            ]),
        ))

        expected_data = [
            {"counter": 135 + i, "label": f"label_{i}", "metric_timestamp": 1750000000 + i} for i in range(5)
        ]
        self._write_yt_table(input_table_path, self._convert_solomon_metrics_to_yt_format(expected_data))

        out_shard_path = solomon_client.create_shard()

        run_query(f"""
$stream = select coalesce(counter, 0) as counter, metric_timestamp, label from `{input_table_path}`;

insert into solomon.`{out_shard_path}`
select * from $stream;
""")

        self._assert_solomon_shard_content(out_shard_path, expected_data, solomon_client)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_yt_solomon_write(self, query_tracker, yql_agent, run_query, solomon_client):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "counter", "type": "uint64"},
                {"name": "metric_timestamp", "type": "timestamp"},
                {"name": "yt_data", "type": "string"},
            ]),
        ))

        solomon_expected_data = [
            {"counter": 135 + i, "metric_timestamp": 1750000000 + i} for i in range(5)
        ]
        yt_expected_data = [
            {"yt_data": f"data_{i}"} for i in range(5)
        ]

        self._write_yt_table(input_table_path, self._convert_solomon_metrics_to_yt_format(solomon_expected_data) + yt_expected_data)

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "yt_data", "type": "string"}
            ]),
        ))
        out_shard_path = solomon_client.create_shard()

        run_query(f"""
$stream = select * from `{input_table_path}`;

$lambda = ($row) -> {{
    $yt_row_type = Struct<'yt_data':optional<string>>;
    $solomon_row_type = Struct<'counter':uint64, 'ts':timestamp>;
    $variant_type = Variant<$yt_row_type, $solomon_row_type>;

    return If(
        $row.yt_data is not null,
        Variant(<|yt_data:$row.yt_data|>, "0", $variant_type),
        Variant(<|counter:coalesce($row.counter, 0), ts:coalesce($row.metric_timestamp, CurrentUtcTimestamp())|>, "1", $variant_type)
    );
}};

$yt_stream, $solomon_stream = process $stream using $lambda(TableRow());

insert into `{out_table_path}`
select * from $yt_stream;

insert into solomon.`{out_shard_path}`
select * from $solomon_stream;
""")

        self._assert_yt_table_content(out_table_path, yt_expected_data)
        self._assert_solomon_shard_content(out_shard_path, solomon_expected_data, solomon_client)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_logbroker_solomon_write(self, query_tracker, yql_agent, run_query, logbroker_client, solomon_client):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "counter", "type": "uint64"},
                {"name": "metric_timestamp", "type": "timestamp"},
                {"name": "logbroker_data", "type": "string"},
            ]),
        ))

        solomon_expected_data = [
            {"counter": 135 + i, "metric_timestamp": 1750000000 + i} for i in range(5)
        ]
        logbroker_expected_data = [f"data_{i}" for i in range(5)]

        input_data = [{"logbroker_data": data} for data in logbroker_expected_data]
        input_data += self._convert_solomon_metrics_to_yt_format(solomon_expected_data)
        self._write_yt_table(input_table_path, input_data)

        out_topic_path = logbroker_client.create_topic()
        out_shard_path = solomon_client.create_shard()

        run_query(f"""
$stream = select * from `{input_table_path}`;

$lambda = ($row) -> {{
    $logbroker_row_type = Struct<'data':string>;
    $solomon_row_type = Struct<'counter':uint64, 'ts':timestamp>;
    $variant_type = Variant<$logbroker_row_type, $solomon_row_type>;

    return If(
        $row.logbroker_data is not null,
        Variant(<|data:coalesce($row.logbroker_data, "Empty!")|>, "0", $variant_type),
        Variant(<|counter:coalesce($row.counter, 0), ts:coalesce($row.metric_timestamp, CurrentUtcTimestamp())|>, "1", $variant_type)
    );
}};

$logbroker_stream, $solomon_stream = process $stream using $lambda(TableRow());

insert into logbroker.`{out_topic_path}`
select * from $logbroker_stream;

insert into solomon.`{out_shard_path}`
select * from $solomon_stream;
""")

        self._assert_logbroker_topic_content(out_topic_path, logbroker_expected_data, logbroker_client)
        self._assert_solomon_shard_content(out_shard_path, solomon_expected_data, solomon_client)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_multiple_solomon_metrics_in_row(self, query_tracker, yql_agent, run_query, solomon_client):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "counter", "type": "uint64"},
                {"name": "gauge", "type": "float"},
                {"name": "igauge", "type": "int64"},
                {"name": "metric_timestamp", "type": "timestamp"},
            ]),
        ))

        solomon_expected_data = [
            {"counter": 135 + i, "gauge": 235.0 + float(i), "igauge": 335 + i, "metric_timestamp": 1750000000 + i} for i in range(5)
        ]
        self._write_yt_table(input_table_path, self._convert_solomon_metrics_to_yt_format(solomon_expected_data))

        out_shard_path = solomon_client.create_shard()

        run_query(f"""
$stream = select coalesce(counter, 0) as counter, coalesce(gauge, 0.0) as gauge, coalesce(igauge, 0) as igauge, metric_timestamp from `{input_table_path}`;

insert into solomon.`{out_shard_path}`
select * from $stream;
""")

        self._assert_solomon_shard_content(out_shard_path, solomon_expected_data, solomon_client, {"counter", "gauge", "igauge"})

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_solomon_with_null_timestamp(self, query_tracker, yql_agent, run_query, solomon_client):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "counter", "type": "uint64"},
                {"name": "metric_timestamp", "type": "timestamp"},
            ]),
        ))

        solomon_expected_data = [
            {
                "counter": 135 + i,
                "metric_timestamp": 1750000000 + i if i % 3 != 0 else None
            } for i in range(5)
        ]
        self._write_yt_table(input_table_path, self._convert_solomon_metrics_to_yt_format(solomon_expected_data))

        out_shard_path = solomon_client.create_shard()

        run_query(f"""
$stream = select coalesce(counter, 0) as counter, metric_timestamp from `{input_table_path}`;

insert into solomon.`{out_shard_path}`
select * from $stream;
""")

        self._assert_solomon_shard_content(out_shard_path, solomon_expected_data, solomon_client)
