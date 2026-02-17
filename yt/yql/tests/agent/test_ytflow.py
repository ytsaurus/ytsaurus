import itertools
import json
import os

import pytest

import yatest.common

import library.python.ydb.federated_topic_client as fedydb

import logbroker.tools.lib.recipe_helpers.cm_requests as cm_requests

from yt.environment.helpers import assert_items_equal
from yt.wrapper.flow_commands import PipelineState
from yt.yql.tests.common.test_framework.test_utils import wait_pipeline_state_or_failed_jobs

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


def load_env():
    with open(ENV_FILE, "r") as env_file:
        for line in env_file:
            for key, value in json.loads(line.strip()).items():
                os.environ[key] = value


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

    MAX_YQL_VERSION = '2025.04'
    DEFAULT_YQL_UI_VERSION = '2025.01'

    PIPELINE_PATH = '//tmp/pipeline'

    YT_TABLE_PATH = '//tmp/Table'
    YT_CONSUMER_PATH = '//tmp/main_consumer'
    YT_PRODUCER_PATH = '//tmp/main_producer'

    # By default logbroker federation recipe creates two clusters named 'cluster_a' and 'cluster_b'
    # and two accounts named `prod` and `test`.
    # Each account also already contains 1 topic and 1 consumer named `topic` and `consumer` correspondingly.
    LOGBROKER_TOPIC = "topic"
    LOGBROKER_CONSUMER = "consumer"
    LOGBROKER_ACCOUNT = "test"
    LOGBROKER_CLUSTER = "cluster_a"
    LOGBROKER_DATABASE = "/Root/logbroker-federation/" + LOGBROKER_ACCOUNT
    LOGBROKER_COMPRESSION_CODEC = "raw"
    LOGBROKER_COMPRESSION_LEVEL = "0"

    has_logbroker_federation = False

    def setup_method(self, method):
        super(TestYtflowBase, self).setup_method(method)

        # NOTE: initialization is in setup_method as session scoped fixture
        # may be used in not first test
        cls = type(self)
        if not cls.has_logbroker_federation:
            cls.has_logbroker_federation = os.getenv("CM_PORT") is not None

            if cls.has_logbroker_federation:
                cls.logbroker_endpoint = f"localhost:{os.getenv(f"{cls.LOGBROKER_CLUSTER}_port")}"
                cls.fed_driver = fedydb.FederationDriver(
                    cls.logbroker_endpoint,
                    cls.LOGBROKER_DATABASE
                )
                cls.fed_driver.wait_init()
                cls.cm = cm_requests.CMApiHelper(f'localhost:{os.getenv("CM_PORT")}')

    @classmethod
    def teardown_class(cls):
        if cls.has_logbroker_federation:
            cls.fed_driver.close()

        super(TestYtflowBase, cls).teardown_class()

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

        if cls.has_logbroker_federation:
            config['yql_agent']['pq_gateway_config'] = dict(
                cluster_mapping=[dict(
                    name='logbroker',
                    endpoint=cls.logbroker_endpoint,
                    database=cls.LOGBROKER_DATABASE
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

    @pytest.fixture
    def create_logbroker_topic(self, request, logbroker_federation):
        assert self.has_logbroker_federation

        logbroker_topic_index_generator = itertools.count()

        topic_prefix = [
            request.cls.__name__.lower(),
            request.function.__name__.lower()
        ]

        if hasattr(request.node, "callspec"):
            indices = request.node.callspec.indices
            for param in request.node.callspec.params.keys():
                topic_prefix.append(str(indices[param]))

        logbroker_topic_prefix = ".".join(topic_prefix)
        logbroker_created_topics = []

        def create_topic():
            topic_idx = next(logbroker_topic_index_generator)
            topic_path = logbroker_topic_prefix + "." + self.LOGBROKER_TOPIC + str(topic_idx)
            topic_path_with_account = self.LOGBROKER_ACCOUNT + "/" + topic_path
            self.cm.exec_request((
                cm_requests.request_create_topic(topic_path_with_account),
            ))
            logbroker_created_topics.append(topic_path_with_account)
            return topic_path

        yield create_topic

        if not logbroker_created_topics:
            return

        self.cm.exec_request(tuple(
            cm_requests.request_remove_topic(topic) for topic in logbroker_created_topics
        ))

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

    def _write_logbroker_topic(self, topic_path, data):
        fed_writer = self.fed_driver.topic_writer(
            topic=topic_path,
        )
        fed_writer.write(data)
        fed_writer.close()

    def _read_logbroker_topic(self, topic_path):
        assert self.has_logbroker_federation

        fed_reader = self.fed_driver.topic_reader(
            topic=topic_path,
            consumer=self.LOGBROKER_CONSUMER,
        )
        batch = fed_reader.receive_batch()
        fed_reader.commit_with_ack(batch)
        fed_reader.close()
        return batch.messages

    def _assert_logbroker_topic_content(self, topic_path, expected_data):
        actual_data = [lb_data.message.data.decode() for lb_data in self._read_logbroker_topic(topic_path)]
        assert actual_data == expected_data

    def _run_query(self, query_text):
        with yatest.common.network.PortManager() as port_manager:
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

            query = start_query("yql", query_text)
            query.track()

            wait_pipeline_state_or_failed_jobs(
                PipelineState.Completed, pipeline_path,
                client=self.Env.create_client(),
                timeout=600)

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
    def test_select(self, query_tracker, yql_agent):
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

        self._run_query(f"""
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
    def test_throwing_udf(self, query_tracker, yql_agent):
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

        self._run_query(f"""
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
    def test_udf_terminate(self, query_tracker, yql_agent):
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
            self._run_query(query)

    @authors("ngc224")
    @pytest.mark.timeout(180)
    @pytest.mark.parametrize("vital", [False, True])
    def test_consumer_vitality(self, query_tracker, yql_agent, vital):
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

        self._run_query(f"""
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
    def test_multiple_outputs_in_lambda(self, query_tracker, yql_agent):
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

        self._run_query(f"""
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

insert into `{out_table_good_path}` with truncate
select * from $good_stream;

insert into `{out_table_bad_path}` with truncate
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
    def test_logbroker_read(self, query_tracker, yql_agent, create_logbroker_topic):
        input_topic_path = create_logbroker_topic()
        self._write_logbroker_topic(input_topic_path, ["a", "b", "c"])

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Data", "type": "string"},
            ]),
        ))

        self._run_query(f"""
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
    def test_logbroker_write(self, query_tracker, yql_agent, create_logbroker_topic):
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

        out_topic_path = create_logbroker_topic()

        self._run_query(f"""
$stream = select coalesce(Data, "Empty!") as Data from `{input_table_path}`;

insert into logbroker.`{out_topic_path}`
select * from $stream;
""")

        self._assert_logbroker_topic_content(out_topic_path, ["AB", "CD", "EF"])

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_read_yt_write_yt_logbroker(self, query_tracker, yql_agent, create_logbroker_topic):
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
        out_topic_path = create_logbroker_topic()

        self._run_query(f"""
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

insert into `{out_table_path}` with truncate
select * from $good_stream;

insert into logbroker.`{out_topic_path}` with truncate
select * from $bad_stream;
""")

        self._assert_yt_table_content(out_table_path, [
            {"Data": "yt"}
        ])
        self._assert_logbroker_topic_content(out_topic_path, ["logbroker", "logbroker"])

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_read_logbroker_write_yt_logbroker(self, query_tracker, yql_agent, create_logbroker_topic):
        input_topic_path = create_logbroker_topic()
        self._write_logbroker_topic(input_topic_path, ["yt", "logbroker", "logbroker"])

        out_table_path = self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Data", "type": "string"},
            ]),
        ))
        out_topic_path = create_logbroker_topic()

        self._run_query(f"""
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

insert into `{out_table_path}` with truncate
select * from $good_stream;

insert into logbroker.`{out_topic_path}` with truncate
select * from $bad_stream;
""")

        self._assert_yt_table_content(out_table_path, [
            {"Data": "yt"}
        ])
        self._assert_logbroker_topic_content(out_topic_path, ["logbroker", "logbroker"])

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_many_logbroker_outputs(self, query_tracker, yql_agent, create_logbroker_topic):
        input_topic_path = create_logbroker_topic()
        self._write_logbroker_topic(input_topic_path, [str(i) for i in range(5)])

        out_topics = [create_logbroker_topic() for _ in range(5)]

        self._run_query(f"""
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

{"\n".join([f"""insert into logbroker.`{out_topic_path}` with truncate select * from $stream{idx};""" for idx, out_topic_path in enumerate(out_topics)])}

""")
        for idx, out_topic in enumerate(out_topics):
            self._assert_logbroker_topic_content(out_topic, [f"{idx}"])

    @authors("artemmashin")
    @pytest.mark.timeout(180)
    def test_yt_yt_logbroker_output(self, query_tracker, yql_agent, create_logbroker_topic):
        input_topic_path = create_logbroker_topic()
        self._write_logbroker_topic(input_topic_path, [str(i) for i in range(3)])

        out_tables = [self._create_yt_table(dict(
            schema=self._make_queue_schema([
                {"name": "Data", "type": "string"},
            ]),
        )) for _ in range(2)]

        out_topic_path = create_logbroker_topic()

        self._run_query(f"""
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

insert into `{out_tables[0]}` with truncate
select * from $stream0;

insert into `{out_tables[1]}` with truncate
select * from $stream1;

insert into logbroker.`{out_topic_path}` with truncate
select * from $stream2;
""")

        for idx, out_table in enumerate(out_tables):
            self._assert_yt_table_content(out_table, [
                {"Data": f"{idx}"}
            ])

        self._assert_logbroker_topic_content(out_topic_path, ["2"])
