import itertools
import json
import os
import os.path
import shutil

import pytest

import yatest.common

import library.python.ydb.federated_topic_client as federated_topic_client

import yt.yson as yson

from yt.yt.experiments.private.flow.yandex.extensions.monium.python.mock import _MoniumServerMock

import logbroker.tools.lib.recipe_helpers.cm_requests as cm_requests
from logbroker.public.api.admin import config_manager_admin_pb2

from library.python.port_manager import PortManager

from contextlib import closing

from yt.environment import init_operations_archive
from yt.environment.helpers import (
    assert_items_equal,
    read_config,
    wait_for_dynamic_config_update,
)

from yt.wrapper.flow_commands import PipelineState

from yt.yql.tests.common.test_framework.test_utils import (
    wait_pipeline_state_or_failed_jobs,
    create_flow_logs_replicators,
    dump_pipeline_jobs_stderr,
    convert_gateways_config_to_proto_text,
    wait_for_debug,
    FlowDebugHelper,
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

ENV_FILE = yatest.common.work_path("env.json.txt")


def has_logbroker_federation():
    return os.getenv("CM_PORT") is not None


def get_logbroker_endpoint(cluster):
    return f"localhost:{os.getenv(f'{cluster}_port')}"


def get_logbroker_cm_endpoint():
    return f"localhost:{os.getenv("CM_PORT")}"


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

    def make_create_topic_request(self, path):
        request = config_manager_admin_pb2.SingleModifyRequest()
        request.create_topic.path.path = path
        request.create_topic.parent_template = 'default'
        request.create_topic.properties.partitions_count.user_defined = 1
        request.create_topic.properties.auto_partitioning_strategy.user_defined = "disabled"
        return request

    def create_topic(self):
        topic_index = next(self._topic_index_generator)
        topic_path = self._test_id + "." + self.LOGBROKER_TOPIC + str(topic_index)
        full_topic_path = self.LOGBROKER_ACCOUNT + "/" + topic_path
        self._cm.exec_request((
            self.make_create_topic_request(full_topic_path),
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


# Module-level singleton holding the in-process gRPC mock server for the
# duration of the test session.  Populated by the session-scoped
# ``solomon_emulator`` fixture and consumed by the function-scoped
# ``solomon_client`` fixture.
_monium_mock_singleton = None


@pytest.fixture(scope="session")
def solomon_emulator():
    """Session-scoped fixture that starts the in-process Monium gRPC mock.

    Replaces the old external solomon_emulator_recipe process.  Sets the env
    vars that ``modify_yql_agent_config`` reads (SOLOMON_MOCK_ENDPOINT) and
    that the TMoniumDriver uses for OAuth auth (MONIUM_TOKEN).
    """
    global _monium_mock_singleton

    mock = _MoniumServerMock()
    mock.start()
    _monium_mock_singleton = mock

    os.environ["MONIUM_TOKEN"] = "test-token"
    os.environ["SOLOMON_MOCK_ENDPOINT"] = mock.endpoint

    try:
        yield
    finally:
        os.environ.pop("MONIUM_TOKEN", None)
        os.environ.pop("SOLOMON_MOCK_ENDPOINT", None)
        mock.stop()
        _monium_mock_singleton = None


@pytest.fixture
def solomon_client(solomon_emulator):
    """Per-test fixture that wraps the session Monium mock with shard tracking.

    Provides the same ``create_shard`` / ``get_metrics`` / ``cleanup`` API that
    the old SolomonClient had, so no test-body changes are needed.  All shards
    created during a test are cleaned up in the fixture teardown.
    """
    mock = _monium_mock_singleton
    created_shards = []

    class _Client:
        @property
        def endpoint(self):
            return mock.endpoint

        def create_shard(self):
            shard_path = mock.create_shard()
            created_shards.append(shard_path)
            return shard_path

        def get_metrics(self, shard_name):
            return mock.get_metrics(shard_name)

        def cleanup(self, shard_name):
            parts = shard_name.split("/", 2)
            mock.cleanup_solomon(*parts)

    client = _Client()
    try:
        yield client
    finally:
        for shard_path in created_shards:
            client.cleanup(shard_path)


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

    YTFLOW_WORKER_BIN = yatest.common.binary_path("yt/yql/tools/ytflow_worker/ytflow_worker")

    def setup_method(self, method):
        super(TestYtflowBase, self).setup_method(method)
        init_operations_archive.create_tables_latest_version(self.Env.create_client())

    @classmethod
    def modify_yql_agent_config(cls, config):
        run_vanilla_operation = cls.debug_flow_output_directory is None

        config['yql_agent']['ytflow_gateway_config'] = dict(
            ytflow_worker_bin=cls.YTFLOW_WORKER_BIN,
            gateway_threads=1,
            default_settings=[
                dict(name='_RpcTimeout', value='10s'),
                dict(name='_MasterLockTimeout', value='2m'),
                dict(name='_MasterLockPingPeriod', value='30s'),
                dict(name='_FiniteStreams', value=str(run_vanilla_operation)),
                dict(name='_UseCpuAwareBalancer', value='false'),
                dict(name='_ControllerWriteFullLogsToYT', value='true'),
                dict(name='_ControllerWriteLogsToFile', value='false'),
                dict(name='_ControllerLogLevel', value='debug'),
                dict(name='_ControllerEnableStderrLogging', value='false'),
                dict(name='_WorkerWriteLogsToYT', value='true'),
                dict(name='_WorkerWriteLogsToFile', value='false'),
                dict(name='_WorkerLogLevel', value='debug'),
                dict(name='_WorkerEnableStderrLogging', value='false'),
                dict(name='_LogsDirectory', value='logs'),
                dict(name='YtPartitionCount', value='1'),
                dict(name='LogbrokerSubject', value='authenticated@well-known'),
                dict(name='LogbrokerTopicPartitionCount', value='3'),
                dict(name='_LogbrokerMirrorToCluster', value='all_original'),
                dict(name='_LogbrokerConfigManagerPollingPeriod', value='100ms'),
                dict(name='_SwitchComputationNodeBufferSizeBytes', value='0'),
                dict(name='_RunVanillaOperation', value=str(run_vanilla_operation)),
                dict(name='_MoniumDriverSecure', value='false'),
            ],
            cluster_mapping=[dict(
                name=cls.Env.id,
                real_name=cls.Env.id,
                proxy_url=cls.Env.get_http_proxy_address(),
            )],
        )

        if has_logbroker_federation():
            logbroker_endpoint = get_logbroker_endpoint(LogbrokerClient.LOGBROKER_CLUSTER)
            logbroker_cm_endpoint = get_logbroker_cm_endpoint()
            config['yql_agent']['pq_gateway_config'] = dict(
                cluster_mapping=[dict(
                    name='logbroker',
                    endpoint=logbroker_endpoint,
                    token="dummy_token",
                    database=LogbrokerClient.LOGBROKER_DATABASE,
                    config_manager_endpoint=logbroker_cm_endpoint
                )]
            )

        config['yql_agent']['solomon_gateway_config'] = dict(
            cluster_mapping=[dict(
                name='solomon',
                cluster=os.getenv("SOLOMON_MOCK_ENDPOINT", "localhost:0"),
                token="dummy_token",
            )]
        )

        yt_gateway_config = config['yql_agent']['gateway_config']
        yt_gateway_config['mr_job_udfs_dir'] = ";".join([
            yt_gateway_config['mr_job_udfs_dir'],
            yatest.common.binary_path("yt/yql/tests/agent/throwing_udf"),
        ])

    @classmethod
    def set_default_setting(cls, name, value, client):
        config = client.get("//sys/yql_agent/config")

        current_map = config
        for key in ("yql_agent", "gateways", "ytflow"):
            current_map = current_map.setdefault(key, yson.YsonMap())

        settings = current_map.setdefault("default_settings", yson.YsonList())

        result_setting = yson.YsonMap({
            "name": name,
            "value": value
        })

        found = False
        for setting in settings:
            if setting["name"] == name:
                found = True
                setting.update(result_setting)

        if not found:
            settings.append(result_setting)

        client.set("//sys/yql_agent/config", config)

        wait_for_dynamic_config_update(client, config, "//sys/yql_agent/instances")

    @classmethod
    def dump_gateways_from_yql_agent_config_as_proto_text(
        cls, yql_agent_config_path, destination_gateways_conf_path
    ):
        yql_agent_config = read_config(yql_agent_config_path)

        gateways_config = {
            "yt": yql_agent_config['yql_agent']['gateway_config'],
            "ytflow": yql_agent_config['yql_agent']['ytflow_gateway_config'],
            "solomon": yql_agent_config['yql_agent']['solomon_gateway_config'],
        }

        yt_cluster_mapping = gateways_config['yt']['cluster_mapping']
        assert len(yt_cluster_mapping) == 1
        yt_cluster_mapping[0]['YTToken'] = "dummy_token"

        ytflow_cluster_mapping = gateways_config['ytflow']['cluster_mapping']
        assert len(ytflow_cluster_mapping) == 1
        ytflow_cluster_mapping[0]['token'] = "dummy_token"

        if has_logbroker_federation():
            gateways_config["pq"] = yql_agent_config['yql_agent']['pq_gateway_config']

        with open(destination_gateways_conf_path, "w") as f:
            f.write(convert_gateways_config_to_proto_text(gateways_config))

    @classmethod
    def setup_yql_debug_environment(
        cls, yql_agent_config_path, destination_gateways_conf_path,
        query_text_source_path, query_text_destination_path
    ):
        cls.dump_gateways_from_yql_agent_config_as_proto_text(
            yql_agent_config_path, destination_gateways_conf_path)

        shutil.copy(query_text_source_path, query_text_destination_path)

        wait_for_debug()

    @pytest.fixture(scope="class", autouse=True)
    def setup_debug_yql_output_directory(self):
        cls = type(self)
        cls.debug_yql_output_directory = os.getenv("DEBUG_YQL_OUTPUT_DIRECTORY")
        if cls.debug_yql_output_directory is not None:
            os.makedirs(cls.debug_yql_output_directory, exist_ok=True)

    @pytest.fixture(scope="class", autouse=True)
    def setup_debug_flow_output_directory(self):
        cls = type(self)
        cls.debug_flow_output_directory = os.getenv("DEBUG_FLOW_OUTPUT_DIRECTORY")
        if cls.debug_flow_output_directory is not None:
            os.makedirs(cls.debug_flow_output_directory, exist_ok=True)

    @pytest.fixture(autouse=True)
    def setup_yt_utils(self):
        self.yt_table_index_generator = itertools.count()

    def _allocate_yt_table_path(self):
        table_index = next(self.yt_table_index_generator)
        table_path = self.YT_TABLE_PATH + str(table_index)
        return table_path

    def _create_yt_table(self, input_table_attrs):
        table_path = self._allocate_yt_table_path()
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
        test_output_directory = os.path.join(
            yatest.common.output_path(), get_test_id(request))

        os.makedirs(test_output_directory)

        query_text_path = os.path.join(test_output_directory, "query.yql")

        def wait_if_yql_debug_requested():
            if self.debug_yql_output_directory is not None:
                self.setup_yql_debug_environment(
                    os.path.join(yatest.common.output_path(), "yql_agent_configs", "yql_agent-0.yson"),
                    os.path.join(self.debug_yql_output_directory, "gateways.conf"),
                    query_text_path,
                    os.path.join(self.debug_yql_output_directory, "query.yql"))

        def wait_if_flow_debug_requested(client, port_manager):
            if self.debug_flow_output_directory is not None:
                flowDebugHelper = FlowDebugHelper(
                    "primary",
                    self.Env.get_http_proxy_address(),
                    client,
                    self.PIPELINE_PATH,
                    self.YTFLOW_WORKER_BIN,
                    port_manager)

                flowDebugHelper.setup_flow_debug_environment(
                    os.path.join(test_output_directory, "setup_pipeline_spec_config.yson"),
                    self.debug_flow_output_directory,
                    controller_wait_retries=5,
                    controller_retry_delay=5,
                    flow_command_timeout=600)

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
pragma Ytflow.ControllerCpuLimit = "1.0";
pragma Ytflow.ControllerMemoryLimit = "1G";

pragma Ytflow.WorkerCount = "1";
pragma Ytflow.WorkerCpuLimit = "1.0";
pragma Ytflow.WorkerMemoryLimit = "1G";

pragma Ytflow.LogbrokerConsumerPath = "{self.LOGBROKER_CONSUMER}";
pragma Ytflow.LogbrokerWriteCompressionCodec = "{self.LOGBROKER_COMPRESSION_CODEC}";
pragma Ytflow.LogbrokerWriteCompressionLevel = "{self.LOGBROKER_COMPRESSION_LEVEL}";
"""

                query_text = '\n'.join([query_text_header, query_text])

                with open(query_text_path, "w") as f:
                    f.write(query_text)

                wait_if_yql_debug_requested()

                client = self.Env.create_client()

                self.set_default_setting("_DumpPipelineSpecToDirectory", test_output_directory, client)

                controller_logs_replicator, worker_logs_replicator = create_flow_logs_replicators(
                    self.PIPELINE_PATH,
                    test_output_directory,
                    logs_batch_size=1000,
                    output_file_prefix="",
                    yt_client=client)

                with controller_logs_replicator, worker_logs_replicator:
                    query = start_query("yql", query_text)
                    query.track()

                    wait_if_flow_debug_requested(client, port_manager)

                    try:
                        wait_pipeline_state_or_failed_jobs(
                            PipelineState.Completed, pipeline_path,
                            client=client,
                            timeout=600)

                    finally:
                        dump_pipeline_jobs_stderr(
                            self.PIPELINE_PATH,
                            os.path.join(test_output_directory, "pipeline_jobs.stderr"),
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

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_complex_graph_with_several_maps(self, query_tracker, yql_agent, run_query):
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
        )) for _ in range(3)]

        run_query(f"""
$lambda = ($row) -> {{
    $row_type = TypeOf($row);
    $variant_type = Variant<$row_type, $row_type>;

    return If(
        $row.value == 3,
        Variant($row, "0", $variant_type),
        Variant($row, "1", $variant_type)
    );
}};

$left_stream, $right_stream = process `{input_table_path}` using $lambda(TableRow());

insert into `{out_table_paths[0]}`
select value + 1 as value from $left_stream;

insert into `{out_table_paths[1]}`
select value + 2 as value from $left_stream;

insert into `{out_table_paths[2]}`
select * from $right_stream;
""")

        self._assert_yt_table_content(out_table_paths[0], [{"value": 4}])

        self._assert_yt_table_content(out_table_paths[1], [{"value": 5}])

        expected_data = [row for row in input_data if row["value"] != 3]
        self._assert_yt_table_content(out_table_paths[2], expected_data)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_two_ytflow_maps_in_a_row(self, query_tracker, yql_agent, run_query):
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
        )) for _ in range(3)]

        run_query(f"""
$lambda = ($row) -> {{
    $row_type = TypeOf($row);
    $variant_type = Variant<$row_type, $row_type>;

    return If(
        $row.value == 3,
        Variant($row, "0", $variant_type),
        Variant($row, "1", $variant_type)
    );
}};

$left_stream1, $right_stream1 = process `{input_table_path}` using $lambda(TableRow());

insert into `{out_table_paths[0]}`
select * from $left_stream1;

$add_one_stream = select value + 1 as value from $right_stream1;

$left_stream2, $right_stream2 = process $add_one_stream using $lambda(TableRow());

insert into `{out_table_paths[1]}`
select * from $left_stream2;

$add_two_stream = select value + 2 as value from $right_stream2;

insert into `{out_table_paths[2]}`
select * from $add_two_stream;
""")

        expected_data = [{"value": 3}]
        self._assert_yt_table_content(out_table_paths[0], expected_data)

        self._assert_yt_table_content(out_table_paths[1], expected_data)

        expected_data = [{"value": value} for value in [3, 4, 7]]
        self._assert_yt_table_content(out_table_paths[2], expected_data)


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

    @authors("ngc224")
    @pytest.mark.timeout(180)
    @pytest.mark.parametrize("creation_mode", ["fresh_table", "truncate"])
    @pytest.mark.parametrize("selection_mode", ["all_columns", "exact_columns"])
    def test_logbroker_transparent_column_removal(
        self, query_tracker, yql_agent, run_query, logbroker_client,
        creation_mode, selection_mode
    ):
        input_topic_path = logbroker_client.create_topic()
        self._write_logbroker_topic(input_topic_path, ["a", "b", "c"], logbroker_client)

        if creation_mode == "fresh_table":
            out_table_path = self._allocate_yt_table_path()
            write_hint = ""
        elif creation_mode == "truncate":
            out_table_path = self._create_yt_table(dict(
                schema=self._make_queue_schema([
                    {"name": "Data", "type": "string"},
                ]),
            ))

            write_hint = " with truncate"
        else:
            raise ValueError(f"Unsupported creation mode: {creation_mode}")

        if selection_mode == "all_columns":
            select_body = "*"
        elif selection_mode == "exact_columns":
            select_body = "Data"
        else:
            raise ValueError(f"Unsupported selection mode: {selection_mode}")

        run_query(f"""
$stream = select Data || "_ytflow" as Data from logbroker.`{input_topic_path}`;

insert into `{out_table_path}`{write_hint}
select {select_body} from $stream;
""")

        self._assert_yt_table_content(out_table_path, [
            {"Data": "a_ytflow"},
            {"Data": "b_ytflow"},
            {"Data": "c_ytflow"},
        ])

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_select_star_read_lb_write_lb(self, query_tracker, yql_agent, run_query, logbroker_client):
        input_topic_path = logbroker_client.create_topic()
        self._write_logbroker_topic(input_topic_path, ["AB", "CD", "EF"], logbroker_client)

        out_topic_path = logbroker_client.create_topic()

        run_query(f"""
insert into logbroker.`{out_topic_path}`
select * from logbroker.`{input_topic_path}`;
""")

        self._assert_logbroker_topic_content(out_topic_path, ["AB", "CD", "EF"], logbroker_client)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_select_star_read_lb_write_yt(self, query_tracker, yql_agent, run_query, logbroker_client):
        input_topic_path = logbroker_client.create_topic()
        self._write_logbroker_topic(input_topic_path, ["AB", "CD", "EF"], logbroker_client)

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Data", "type": "string"},
            ]),
        ))

        run_query(f"""
insert into `{out_table_path}`
select * from logbroker.`{input_topic_path}`;
""")

        self._assert_yt_table_content(out_table_path, [
            {"Data": "AB"},
            {"Data": "CD"},
            {"Data": "EF"}
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

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_logbroker_output_topics_creation(self, query_tracker, yql_agent, run_query, logbroker_client):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Data", "type": "string", "required": True},
            ]),
        ))
        input_data = [{"Data": str(i)} for i in range(4)]
        self._write_yt_table(input_table_path, input_data)

        out_topic_paths = [
            "test/topic_dir/topic_first",
            "test/topic_dir/topic_second",
            "test/other_topics/topic",
            "test/other_topics/inner/topic"
        ]

        run_query(f"""
$stream = select Data from `{input_table_path}`;

$lambda = ($row) -> {{
    $row_type = TypeOf($row);
    $variant_type = Variant<$row_type, $row_type, $row_type, $row_type>;

    return case $row.Data
        {"\n".join([f"""when "{idx}" then Variant($row, "{idx}", $variant_type)""" for idx in range(len(input_data))])}
        else Variant(<|Data:"Unexpected!"|>, "0", $variant_type)
    end;
}};

$stream0, $stream1, $stream2, $stream3 = process $stream using $lambda(TableRow());

{"\n".join([f"""insert into logbroker.`{out_topic_path}`
select * from $stream{idx};""" for idx, out_topic_path in enumerate(out_topic_paths)])}

""")

        for idx, out_topic_path in enumerate(out_topic_paths):
            self._assert_logbroker_topic_content(out_topic_path, [f"{idx}"], logbroker_client)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_logbroker_consumer_creation(self, query_tracker, yql_agent, run_query, logbroker_client):
        input_topic_path = logbroker_client.create_topic()
        input_data = [str(i) for i in range(5)]
        self._write_logbroker_topic(input_topic_path, input_data, logbroker_client)

        out_topic_path = "test/topic_dir/topic"

        run_query(f"""
pragma Ytflow.LogbrokerConsumerPath = "test/consumer_dir/consumer";

$stream = select Data || "_dummy" as Data from logbroker.`{input_topic_path}`;

insert into logbroker.`{out_topic_path}`
select * from $stream;
""")

        self._assert_logbroker_topic_content(
            out_topic_path, [f"{input}_dummy" for input in input_data], logbroker_client)


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
