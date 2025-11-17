from yt_queries import start_query

from yt.environment.helpers import assert_items_equal
from yt.wrapper.flow_commands import wait_pipeline_state, PipelineState

from yt_commands import (
    authors, create, sync_mount_table, insert_rows, select_rows,
    list_queue_consumer_registrations,
)

from yt_queue_agent_test_base import TestQueueAgentBase

import pytest

import yatest.common


class TestYtflowBase(TestQueueAgentBase):
    NUM_MASTERS = 1
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

    INPUT_TABLE = '//tmp/Input'
    OUTPUT_TABLE_PATH = '//tmp/Output'
    PIPELINE_PATH = '//tmp/pipeline'
    CONSUMER_PATH = '//tmp/main_consumer'
    PRODUCER_PATH = '//tmp/main_producer'

    @classmethod
    def modify_yql_agent_config(cls, config):
        config['yql_agent']['ytflow_gateway_config'] = dict(
            ytflow_worker_bin=yatest.common.binary_path("yt/yql/tools/ytflow_worker/ytflow_worker"),
            default_settings=[
                dict(name='FiniteStreams', value='1'),
                dict(name='GatewayThreads', value='1'),
                dict(name='YtPartitionCount', value='1'),
            ],
            cluster_mapping=[dict(
                name=cls.Env.id,
                real_name=cls.Env.id,
                proxy_url=cls.Env.get_http_proxy_address(),
            )],
        )

        yt_gateway_config = config['yql_agent']['gateway_config']
        yt_gateway_config['mr_job_udfs_dir'] = ";".join([
            yt_gateway_config['mr_job_udfs_dir'],
            yatest.common.binary_path("yt/yql/tests/agent/throwing_udf"),
        ])

    def _run_test(
        self, input_table_attrs, input_rows,
        query_text,
        output_tables_attrs, expected_output_rows
    ):
        with yatest.common.network.PortManager() as port_manager:
            pipeline_path = self.PIPELINE_PATH

            query_text_header = f"""
use primary;

pragma Engine = "ytflow";

pragma Ytflow.Cluster = "primary";
pragma Ytflow.PipelinePath = "{pipeline_path}";

pragma Ytflow.YtConsumerPath = "{self.CONSUMER_PATH}";
pragma Ytflow.YtProducerPath = "{self.PRODUCER_PATH}";
pragma Ytflow.ControllerRpcPort = "{port_manager.get_port()}";
pragma Ytflow.ControllerMonitoringPort = "{port_manager.get_port()}";
pragma Ytflow.WorkerRpcPort = "{port_manager.get_port()}";
pragma Ytflow.WorkerMonitoringPort = "{port_manager.get_port()}";
"""

            input_table = self.INPUT_TABLE
            input_table_attrs.update(dynamic=True)
            create("table", input_table, attributes=input_table_attrs)
            sync_mount_table(input_table)

            output_tables = []
            for index, output_table_attrs in enumerate(output_tables_attrs):
                current_output_table = self.OUTPUT_TABLE_PATH + str(index)
                output_table_attrs.update(dynamic=True)
                create("table", current_output_table, attributes=output_table_attrs)
                sync_mount_table(current_output_table)

                output_tables.append(current_output_table)

            insert_rows(input_table, input_rows)

            query_text = '\n'.join([query_text_header, query_text])

            query = start_query("yql", query_text)
            query.track()

            wait_pipeline_state(
                PipelineState.Completed, pipeline_path,
                client=self.Env.create_client(),
                timeout=600)

            for table, expected_rows in zip(output_tables, expected_output_rows):
                result = list(select_rows(f"* from [{table}]"))
                self._remove_system_columns(result)
                assert_items_equal(result, expected_rows)

    def _get_out_table_path(self, index):
        return self.OUTPUT_TABLE_PATH + str(index)

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
    NUM_TEST_PARTITIONS = 4

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_select(self, query_tracker, yql_agent):
        self._run_test(
            input_table_attrs=dict(
                schema=self._make_queue_schema([
                    {"name": "string_field", "type": "string"},
                    {"name": "int64_field", "type": "int64"},
                ]),
            ),
            input_rows=[
                {"string_field": "foo", "int64_field": 1},
                {"string_field": "bar", "int64_field": 10},
                {"string_field": "foobar", "int64_field": 100},
            ],
            query_text=f"""
insert into `{self._get_out_table_path(0)}`
select
    string_field || "_ytflow" as string_field,
    int64_field * 100 as int64_field,
    int64_field > 10 as bool_field
from `{self.INPUT_TABLE}`
where string_field = "foo" or int64_field >= 100;
""",
            output_tables_attrs=[
                dict(
                    schema=self._make_queue_schema([
                        {"name": "string_field", "type": "string"},
                        {"name": "int64_field", "type": "int64"},
                        {"name": "bool_field", "type": "boolean"},
                    ]),
                ),
            ],
            expected_output_rows=[
                [
                    {"string_field": "foo_ytflow", "int64_field": 100, "bool_field": False},
                    {"string_field": "foobar_ytflow", "int64_field": 10000, "bool_field": True},
                ],
            ]
        )

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_throwing_udf(self, query_tracker, yql_agent):
        self._run_test(
            input_table_attrs=dict(
                schema=self._make_queue_schema([
                    {"name": "value", "type": "string"},
                    {"name": "need_throw", "type": "boolean"},
                ]),
            ),
            input_rows=[
                {"value": "foo", "need_throw": False},
                {"value": "bar", "need_throw": True},
                {"value": "foobar", "need_throw": False},
            ],
            query_text=f"""
insert into `{self._get_out_table_path(0)}`
select
    ThrowingUdf::ParseWithThrow(value, need_throw) as parsed_value
from `{self.INPUT_TABLE}`
""",
            output_tables_attrs=[
                dict(
                    schema=self._make_queue_schema([
                        {"name": "parsed_value", "type": "string"},
                    ]),
                ),
            ],
            expected_output_rows=[
                [
                    {"parsed_value": "foo"},
                    {'parsed_value': '(yexception) yt/yql/tests/agent/throwing_udf/throwing_udf.cpp:14: expected exception'},
                    {"parsed_value": "foobar"},
                ],
            ],
        )

    @authors("ngc224")
    @pytest.mark.timeout(180)
    @pytest.mark.parametrize("vital", [False, True])
    def test_consumer_vitality(self, query_tracker, yql_agent, vital):
        self._run_test(
            input_table_attrs=dict(
                schema=self._make_queue_schema([
                    {"name": "value", "type": "string"},
                ]),
            ),
            input_rows=[
                {"value": "foo"},
                {"value": "bar"},
            ],
            query_text=f"""
pragma Ytflow.YtConsumerVital = "{vital}";

insert into `{self._get_out_table_path(0)}`
select value
from `{self.INPUT_TABLE}`
""",
            output_tables_attrs=[
                dict(
                    schema=self._make_queue_schema([
                        {"name": "value", "type": "string"},
                    ]),
                ),
            ],
            expected_output_rows=[
                [
                    {"value": "foo"},
                    {"value": "bar"},
                ],
            ],
        )

        registrations = list_queue_consumer_registrations(
            queue_path=self.INPUT_TABLE,
            consumer_path=self.CONSUMER_PATH,
        )

        assert len(registrations) == 1
        assert registrations[0]["vital"] == vital

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_multiple_outputs_in_lambda(self, query_tracker, yql_agent):
        FIELD_GOOD = "int64_field"
        FIELD_BAD = "string_field"

        self._run_test(
            input_table_attrs=dict(
                schema=self._make_queue_schema([
                    {"name": FIELD_GOOD, "type": "int64"},
                ]),
            ),
            input_rows=[
                {FIELD_GOOD: 1},
                {FIELD_GOOD: 10},
                {FIELD_GOOD: 100},
            ],
            query_text=f"""
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

$good_stream, $bad_stream = process `{self.INPUT_TABLE}` using $lambda(TableRow());

insert into `{self._get_out_table_path(0)}` with truncate
select * from $good_stream;

insert into `{self._get_out_table_path(1)}` with truncate
select * from $bad_stream;
""",
            output_tables_attrs=[
                dict(
                    schema=self._make_queue_schema([
                        {"name": FIELD_GOOD, "type": "int64"},
                    ]),
                ),
                dict(
                    schema=self._make_queue_schema([
                        {"name": FIELD_BAD, "type": "string"},
                    ]),
                ),
            ],
            expected_output_rows=[
                [
                    {FIELD_GOOD: 10},
                ],
                [
                    {FIELD_BAD: "1"},
                    {FIELD_BAD: "100"},
                ]
            ]
        )
