from yt_queries import start_query

from yt.environment.helpers import assert_items_equal
from yt.wrapper.flow_commands import wait_pipeline_state, PipelineState

from yt_commands import authors, create, mount_table, insert_rows, select_rows

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
    OUTPUT_TABLE = '//tmp/Output'
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

    def _run_test(
        self, input_table_attrs, input_rows,
        query_text,
        output_table_attrs, expected_output_rows
    ):
        pipeline_path = self.PIPELINE_PATH

        query_text_header = f"""
use primary;

pragma Engine = "ytflow";

pragma Ytflow.Cluster = "primary";
pragma Ytflow.PipelinePath = "{pipeline_path}";

pragma Ytflow.YtConsumerPath = "{self.CONSUMER_PATH}";
pragma Ytflow.YtProducerPath = "{self.PRODUCER_PATH}";
"""

        input_table = self.INPUT_TABLE
        input_table_attrs.update(dynamic=True)
        create("table", input_table, attributes=input_table_attrs)

        output_table = self.OUTPUT_TABLE
        output_table_attrs.update(dynamic=True)
        create("table", output_table, attributes=output_table_attrs)

        mount_table(input_table, sync=True)
        mount_table(output_table, sync=True)

        insert_rows(input_table, input_rows)

        query_text = '\n'.join([query_text_header, query_text])

        query = start_query("yql", query_text)
        query.track()

        wait_pipeline_state(
            PipelineState.Completed, pipeline_path,
            client=self.Env.create_client(),
            timeout=600)

        result = list(select_rows(f"* from [{output_table}]"))
        system_columns = ["$tablet_index", "$row_index", "$timestamp", "$cumulative_data_weight"]

        for item in result:
            for system_column in system_columns:
                item.pop(system_column, None)

        assert_items_equal(result, expected_output_rows)


class TestYtflow(TestYtflowBase):
    NUM_TEST_PARTITIONS = 4

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_select(self, query_tracker, yql_agent):
        self._run_test(
            input_table_attrs=dict(
                schema=[
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                    {"name": "string_field", "type": "string"},
                    {"name": "int64_field", "type": "int64"},
                ],
            ),
            input_rows=[
                {"string_field": "foo", "int64_field": 1},
                {"string_field": "bar", "int64_field": 10},
                {"string_field": "foobar", "int64_field": 100},
            ],
            query_text=f"""
insert into `{self.OUTPUT_TABLE}`
select
    string_field || "_ytflow" as string_field,
    int64_field * 100 as int64_field,
    int64_field > 10 as bool_field
from `{self.INPUT_TABLE}`
where string_field = "foo" or int64_field >= 100;
""",
            output_table_attrs=dict(
                schema=[
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                    {"name": "string_field", "type": "string"},
                    {"name": "int64_field", "type": "int64"},
                    {"name": "bool_field", "type": "boolean"},
                ],
            ),
            expected_output_rows=[
                {"string_field": "foo_ytflow", "int64_field": 100, "bool_field": False},
                {"string_field": "foobar_ytflow", "int64_field": 10000, "bool_field": True},
            ],
        )
