import itertools
import json
import os
import os.path
import shutil
import urllib.request

import pytest

import yatest.common

import yt.yson as yson

from library.python.port_manager import PortManager

from contextlib import ExitStack
from datetime import datetime, timezone

from yt.environment import init_operations_archive
from yt.environment.helpers import (
    assert_items_equal,
    read_config,
    wait_for_dynamic_config_update,
)

from yt.wrapper.flow_commands import PipelineState

from yt.yql.tests.common.test_framework.test_utils import (
    wait_pipeline_condition_or_failed_jobs,
    wait_pipeline_state_or_failed_jobs,
    create_flow_logs_replicators,
    dump_pipeline_jobs_stderr,
    convert_gateways_config_to_proto_text,
    wait_for_debug,
    FlowDebugHelper,
)

from yt_commands import (
    authors, create, sync_mount_table, insert_rows, select_rows,
    list_queue_consumer_registrations, raises_yt_error, get,
)

from yt_queries import start_query
from yt_queue_agent_test_base import TestQueueAgentBase


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

    MAX_YQL_VERSION = '2025.05'
    DEFAULT_YQL_UI_VERSION = '2025.01'

    PIPELINE_PATH = '//tmp/pipeline'

    YT_TABLE_PATH = '//tmp/Table'
    YT_CONSUMER_PATH = '//tmp/main_consumer'
    YT_PRODUCER_PATH = '//tmp/main_producer'

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
                # Controller timings (ms) patched into the controller job config.
                dict(name='_ControllerConfig',
                     value='{warm_up_time=1000;scheduler_period=500;publish_retry_period=1000;'
                           'controller_service={set_spec_retry_period=1000};}'),
                # Balancer timings (ms) merged into the dynamic pipeline spec's job_manager.
                dict(name='_JobManagerConfig', value='{rebalance_sync_period=500;}'),
                dict(name='_FiniteStreams', value=str(run_vanilla_operation)),
                dict(name='EnableComputationPatternResources', value='false'),
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
                dict(name='_SwitchComputationNodeBufferSizeBytes', value='0'),
                dict(name='_RunVanillaOperation', value=str(run_vanilla_operation)),
            ],
            cluster_mapping=[dict(
                name=cls.Env.id,
                real_name=cls.Env.id,
                proxy_url=cls.Env.get_http_proxy_address(),
            )],
        )

        cls.extend_yql_agent_config(config)

        yt_gateway_config = config['yql_agent']['gateway_config']
        yt_gateway_config['mr_job_udfs_dir'] = ";".join([
            yt_gateway_config['mr_job_udfs_dir'],
            yatest.common.binary_path("yt/yql/tests/agent/throwing_udf"),
        ])

    @classmethod
    def extend_yql_agent_config(cls, config):
        pass

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
        }

        yt_cluster_mapping = gateways_config['yt']['cluster_mapping']
        assert len(yt_cluster_mapping) == 1
        yt_cluster_mapping[0]['YTToken'] = "dummy_token"

        ytflow_cluster_mapping = gateways_config['ytflow']['cluster_mapping']
        assert len(ytflow_cluster_mapping) == 1
        ytflow_cluster_mapping[0]['token'] = "dummy_token"

        cls.extend_debug_gateways_config(gateways_config, yql_agent_config)

        with open(destination_gateways_conf_path, "w") as f:
            f.write(convert_gateways_config_to_proto_text(gateways_config))

    @classmethod
    def extend_debug_gateways_config(cls, gateways_config, yql_agent_config):
        pass

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

    def _get_yt_table_key_columns(self, table_path):
        schema = get(f"{table_path}/@schema")
        return [column["name"] for column in schema if "sort_order" in column]

    def _assert_yt_table_key_columns(self, table_path, expected_key_columns):
        assert self._get_yt_table_key_columns(table_path) == list(expected_key_columns)

    def _get_single_flow_worker(self, client):
        workers = client.get_flow_view(self.PIPELINE_PATH, cache=False)["state"]["workers"]
        assert len(workers) == 1
        return next(iter(workers.values()))

    @staticmethod
    def _read_flow_worker_monitoring(monitoring_address, path):
        # JSON requests are Solomon pulls by default, which converts counters to
        # per-grid rate gauges. Read cumulative counters for deterministic assertions.
        request = urllib.request.Request(
            f"http://{monitoring_address}{path}",
            headers={"X-YT-IsSolomonPull": "0"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)

    @staticmethod
    def _get_flow_worker_counter(sensors, sensor_name, required_labels):
        values = [
            sensor["value"]
            for sensor in sensors
            if sensor.get("labels", {}).get("sensor") == sensor_name
            and all(
                sensor.get("labels", {}).get(label) == value
                for label, value in required_labels.items()
            )
        ]
        assert values
        # Solomon exports the same tagged counter through several tag projections.
        return max(values)

    @pytest.fixture
    def run_query(self, request):
        test_output_directory = os.path.join(
            yatest.common.output_path(), get_test_id(request))

        os.makedirs(test_output_directory)

        def wait_if_yql_debug_requested(query_text_path, artifact_subdirectory):
            if self.debug_yql_output_directory is not None:
                debug_output_directory = self.debug_yql_output_directory
                if artifact_subdirectory is not None:
                    debug_output_directory = os.path.join(
                        debug_output_directory,
                        artifact_subdirectory,
                    )
                    os.makedirs(debug_output_directory, exist_ok=True)

                self.setup_yql_debug_environment(
                    os.path.join(yatest.common.output_path(), "yql_agent_configs", "yql_agent-0.yson"),
                    os.path.join(debug_output_directory, "gateways.conf"),
                    query_text_path,
                    os.path.join(debug_output_directory, "query.yql"))

        def wait_if_flow_debug_requested(
            client,
            port_manager,
            invocation_output_directory,
            artifact_subdirectory,
        ):
            if self.debug_flow_output_directory is not None:
                debug_output_directory = self.debug_flow_output_directory
                if artifact_subdirectory is not None:
                    debug_output_directory = os.path.join(
                        debug_output_directory,
                        artifact_subdirectory,
                    )
                    os.makedirs(debug_output_directory, exist_ok=True)

                flowDebugHelper = FlowDebugHelper(
                    "primary",
                    self.Env.get_http_proxy_address(),
                    client,
                    self.PIPELINE_PATH,
                    self.YTFLOW_WORKER_BIN,
                    port_manager)

                flowDebugHelper.setup_flow_debug_environment(
                    os.path.join(invocation_output_directory, "setup_pipeline_spec_config.yson"),
                    debug_output_directory,
                    controller_wait_retries=5,
                    controller_retry_delay=5,
                    flow_command_timeout=600)

        call_stacks = []

        def impl(
            query_text,
            yql_version=self.MAX_YQL_VERSION,
            target_state=PipelineState.Completed,
            artifact_subdirectory=None,
            on_target_state_reached=None,
            success_condition=None,
            success_condition_timeout=120,
        ):
            with ExitStack() as stack:
                port_manager = stack.enter_context(PortManager())
                pipeline_path = self.PIPELINE_PATH
                invocation_output_directory = test_output_directory
                if artifact_subdirectory is not None:
                    invocation_output_directory = os.path.join(
                        test_output_directory,
                        artifact_subdirectory,
                    )
                    os.makedirs(invocation_output_directory)

                query_text_path = os.path.join(
                    invocation_output_directory,
                    "query.yql",
                )

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

{self.get_additional_query_pragmas()}
"""

                query_text = '\n'.join([query_text_header, query_text])

                with open(query_text_path, "w") as f:
                    f.write(query_text)

                wait_if_yql_debug_requested(query_text_path, artifact_subdirectory)

                client = self.Env.create_client()

                self.set_default_setting(
                    "_DumpPipelineSpecToDirectory",
                    invocation_output_directory,
                    client,
                )

                controller_logs_replicator, worker_logs_replicator = create_flow_logs_replicators(
                    self.PIPELINE_PATH,
                    invocation_output_directory,
                    logs_batch_size=1000,
                    output_file_prefix="",
                    yt_client=client)

                stack.enter_context(controller_logs_replicator)
                stack.enter_context(worker_logs_replicator)
                query = start_query("yql", query_text, settings={"yql_version": yql_version})
                query.track()

                wait_if_flow_debug_requested(
                    client,
                    port_manager,
                    invocation_output_directory,
                    artifact_subdirectory,
                )

                try:
                    wait_pipeline_state_or_failed_jobs(
                        target_state, pipeline_path,
                        client=client,
                        timeout=600)

                    if on_target_state_reached is not None:
                        on_target_state_reached(client)

                    if success_condition is not None:
                        wait_pipeline_condition_or_failed_jobs(
                            lambda current_state: success_condition(client, current_state),
                            pipeline_path,
                            client=client,
                            timeout=success_condition_timeout,
                            condition_description="query success condition",
                            ignore_exceptions=True,
                        )

                finally:
                    dump_pipeline_jobs_stderr(
                        self.PIPELINE_PATH,
                        os.path.join(
                            invocation_output_directory,
                            "pipeline_jobs.stderr",
                        ),
                        client=client)

                call_stacks.append(stack.pop_all())
                return client

        try:
            yield impl
        finally:
            for stack in reversed(call_stacks):
                stack.close()

    def get_additional_query_pragmas(self):
        return ""

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

    @authors("spreis")
    @pytest.mark.timeout(300)
    def test_pattern_pipeline_spec_update(
        self,
        query_tracker,
        yql_agent,
        run_query,
    ):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "key", "type": "string"},
                {"name": "value", "type": "int64"},
            ]),
            tablet_count=2,
        ))
        first_input_rows = [
            {"$tablet_index": 0, "key": "first_0", "value": 1},
            {"$tablet_index": 1, "key": "first_1", "value": 2},
        ]
        second_input_rows = [
            {"$tablet_index": 0, "key": "second_0", "value": 3},
            {"$tablet_index": 1, "key": "second_1", "value": 4},
        ]
        self._write_yt_table(input_table_path, first_input_rows)

        output_schema = self._make_queue_schema([
            {"name": "key", "type": "string"},
            {"name": "value", "type": "int64"},
        ])
        first_output_path = self._create_yt_table(dict(schema=output_schema))
        second_output_path = self._create_yt_table(dict(schema=output_schema))
        first_expected_rows = [
            {"key": "first_0_processed", "value": 10},
            {"key": "first_1_processed", "value": 20},
        ]
        second_expected_rows = [
            {"key": "second_0_processed", "value": 30},
            {"key": "second_1_processed", "value": 40},
        ]

        def table_has_rows(table_path, expected_rows):
            return sorted(self._read_yt_table(table_path), key=lambda row: row["key"]) == sorted(
                expected_rows,
                key=lambda row: row["key"],
            )

        function_registry_load_sensor = (
            "yt.flow.worker.resource.custom.function_registry.load")
        computation_pattern_load_sensor = (
            "yt.flow.worker.resource.custom.computation_pattern.load")
        computation_graph_clone_sensor = (
            "yt.flow.worker.computation.custom.computation_graph.clone")

        def pattern_resource_metrics_are_ready(client):
            static_spec = client.get_pipeline_spec(self.PIPELINE_PATH)["spec"]

            def find_resource_id(resource_class_name):
                resource_ids = [
                    resource_id
                    for resource_id, resource_spec in static_spec["resources"].items()
                    if resource_spec["resource_class_name"] == resource_class_name
                ]
                assert len(resource_ids) == 1
                return resource_ids[0]

            function_registry_resource_id = find_resource_id(
                "NYql::NYtflow::TFunctionRegistryResource")
            computation_pattern_resource_id = find_resource_id(
                "NYql::NYtflow::TComputationPatternResource")
            pattern_computation_ids = [
                computation_id
                for computation_id, computation_spec in static_spec["computations"].items()
                if computation_pattern_resource_id
                in computation_spec.get("required_resource_ids", {})
            ]
            assert len(pattern_computation_ids) == 1
            pattern_computation_id = pattern_computation_ids[0]

            monitoring_address = self._get_single_flow_worker(client)["monitoring_address"]

            sensors = self._read_flow_worker_monitoring(
                monitoring_address,
                "/solomon/all",
            )["sensors"]
            return (
                self._get_flow_worker_counter(
                    sensors,
                    function_registry_load_sensor,
                    {"resource": function_registry_resource_id},
                ) == 1
                and self._get_flow_worker_counter(
                    sensors,
                    computation_pattern_load_sensor,
                    {"resource": computation_pattern_resource_id},
                ) == 1
                and self._get_flow_worker_counter(
                    sensors,
                    computation_graph_clone_sensor,
                    {"computation_id": pattern_computation_id},
                ) >= 2
            )

        def submit_query(
            output_path,
            expected_rows,
            artifact_subdirectory,
            on_target_state_reached=None,
        ):
            return run_query(f"""
pragma Ytflow.EnableComputationPatternResources = "true";

insert into `{output_path}`
select
    key || "_processed" as key,
    value * 10 as value
from `{input_table_path}`;
""",
                target_state=PipelineState.Working,
                artifact_subdirectory=artifact_subdirectory,
                on_target_state_reached=on_target_state_reached,
                success_condition=lambda client, _: (
                    table_has_rows(output_path, expected_rows)
                    and pattern_resource_metrics_are_ready(client)
                ),
            )

        config_client = self.Env.create_client()
        self.set_default_setting("_FiniteStreams", "false", config_client)
        try:
            first_client = submit_query(
                first_output_path,
                first_expected_rows,
                artifact_subdirectory="first_query",
            )
            assert first_client.get_pipeline_state(self.PIPELINE_PATH) == PipelineState.Working
            first_operation_id = first_client.get(
                f"{self.PIPELINE_PATH}/@_yql_ytflow_vanilla_info/operation_id")
            self._assert_yt_table_content(second_output_path, [])

            def write_second_input(client):
                assert client.get_pipeline_state(self.PIPELINE_PATH) == PipelineState.Working
                assert client.get(
                    f"{self.PIPELINE_PATH}/@_yql_ytflow_vanilla_info/operation_id"
                ) != first_operation_id
                self._write_yt_table(input_table_path, second_input_rows)

            second_client = submit_query(
                second_output_path,
                second_expected_rows,
                artifact_subdirectory="second_query",
                on_target_state_reached=write_second_input,
            )
            assert second_client.get_pipeline_state(self.PIPELINE_PATH) == PipelineState.Working
            second_operation_id = second_client.get(
                f"{self.PIPELINE_PATH}/@_yql_ytflow_vanilla_info/operation_id")
            assert second_operation_id != first_operation_id

            self._assert_yt_table_content(first_output_path, first_expected_rows)
            self._assert_yt_table_content(second_output_path, second_expected_rows)
        finally:
            self.set_default_setting("_FiniteStreams", "true", config_client)

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
    def test_create_sorted_table_by_order_by(self, query_tracker, yql_agent, run_query):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "key", "type": "string"},
                {"name": "value", "type": "int64"},
            ]),
        ))
        input_data = [
            {"key": "foo", "value": 1},
            {"key": "bar", "value": 10},
            {"key": "baz", "value": 100},
        ]
        self._write_yt_table(input_table_path, input_data)

        out_table_path = self._allocate_yt_table_path()

        run_query(f"""
replace into `{out_table_path}`
select key, value from `{input_table_path}`
order by key;
""")

        self._assert_yt_table_key_columns(out_table_path, ["key"])
        self._assert_yt_table_content(out_table_path, input_data)

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    @pytest.mark.parametrize("order_by_keys", [("key_a", "key_b"), ("key_b", "key_a")])
    def test_create_sorted_table_by_composite_order_by(self, query_tracker, yql_agent, run_query, order_by_keys):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "key_a", "type": "int64"},
                {"name": "key_b", "type": "string"},
                {"name": "value", "type": "int64"},
            ]),
        ))
        input_data = [
            {"key_a": 1, "key_b": "foo", "value": 5},
            {"key_a": 2, "key_b": "foo", "value": 15},
            {"key_a": 1, "key_b": "bar", "value": 25},
        ]
        self._write_yt_table(input_table_path, input_data)

        out_table_path = self._allocate_yt_table_path()

        run_query(f"""
replace into `{out_table_path}`
select key_a, key_b, value from `{input_table_path}`
order by {", ".join(order_by_keys)};
""")

        self._assert_yt_table_key_columns(out_table_path, order_by_keys)
        self._assert_yt_table_content(out_table_path, input_data)

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

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    @pytest.mark.parametrize("yql_version", ["2025.01", "2025.05"])
    def test_datetime_udf_with_different_langver(self, query_tracker, yql_agent, run_query, yql_version):
        input_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "value", "type": "uint32"},
            ]),
        ))

        datetime_format = "%Y-%m-%d %H:%M:%S"

        test_timestamp = int(datetime.strptime("2026-07-14 19:11:35", datetime_format).timestamp())
        input_timestamps = [test_timestamp + i for i in range(5)]
        input_data = [{"value": timestamp} for timestamp in input_timestamps]
        self._write_yt_table(input_table_path, input_data)

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "time", "type": "string"},
            ]),
        ))

        run_query(f"""
insert into `{out_table_path}`
select DateTime::Format('{datetime_format}')(DateTime::FromSeconds(value)) as time from `{input_table_path}`;
""", yql_version)

        expected_data = [{"time": datetime.fromtimestamp(timestamp, timezone.utc).strftime(datetime_format)} for timestamp in input_timestamps]
        self._assert_yt_table_content(out_table_path, expected_data)
