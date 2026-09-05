import itertools
import json
import os
from contextlib import closing

import pytest

import yatest.common

from library.python.ydb import federated_topic_client
from logbroker.public.api.admin import config_manager_admin_pb2
from logbroker.tools.lib.recipe_helpers import cm_requests
from yt.yt.flow.yandex.extensions.monium.python.mock import _MoniumServerMock

from yt.environment.helpers import assert_items_equal

from yt_commands import authors

import test_ytflow as ytflow_common


ENV_FILE = yatest.common.work_path("env.json.txt")


def has_logbroker_federation():
    return os.getenv("CM_PORT") is not None


def get_logbroker_endpoint(cluster):
    return f"localhost:{os.getenv(f'{cluster}_port')}"


def get_logbroker_cm_endpoint():
    return f"localhost:{os.getenv('CM_PORT')}"


class LogbrokerClient:
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
        request.create_topic.parent_template = "default"
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


@pytest.fixture(scope="session")
def logbroker_federation():
    recipe_binary = yatest.common.binary_path(
        "kikimr/public/tools/federation_recipe/federation_recipe")

    common_args = [
        "--build-root", yatest.common.build_path(),
        "--source-root", yatest.common.source_path(),
        "--output-dir", yatest.common.output_path(),
        "--env-file", ENV_FILE,
    ]

    yatest.common.process.execute(
        command=[recipe_binary, "start", "--legacy-pq"] + common_args
    )

    load_env()

    yield

    yatest.common.process.execute(
        command=[recipe_binary, "stop"] + common_args
    )


@pytest.fixture
def logbroker_client(request, logbroker_federation):
    if not has_logbroker_federation():
        pytest.skip("Logbroker federation is not available")

    cluster = LogbrokerClient.LOGBROKER_CLUSTER
    cm = cm_requests.CMApiHelper(f"localhost:{os.getenv('CM_PORT')}")

    with LogbrokerClient(
        endpoint=get_logbroker_endpoint(cluster),
        database=LogbrokerClient.LOGBROKER_DATABASE,
        cm=cm,
        test_id=ytflow_common.get_test_id(request),
    ) as client:
        yield client


_monium_mock_singleton = None


@pytest.fixture(scope="session")
def solomon_emulator():
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
            mock.cleanup_solomon(*shard_name.split("/", 2))

    client = _Client()
    try:
        yield client
    finally:
        for shard_path in created_shards:
            client.cleanup(shard_path)


class TestYtflowExtraProvidersBase(ytflow_common.TestYtflowBase):
    LOGBROKER_CONSUMER = "consumer"
    LOGBROKER_COMPRESSION_CODEC = "raw"
    LOGBROKER_COMPRESSION_LEVEL = "0"

    SOLOMON_PROJECT = "project"
    SOLOMON_SERVICE = "service"
    SOLOMON_CLUSTER = "cluster"

    @classmethod
    def extend_yql_agent_config(cls, config):
        config["yql_agent"]["ytflow_gateway_config"]["default_settings"].extend([
            dict(name="LogbrokerSubject", value="authenticated@well-known"),
            dict(name="LogbrokerTopicPartitionCount", value="3"),
            dict(name="_LogbrokerMirrorToCluster", value="all_original"),
            dict(name="_LogbrokerConfigManagerPollingPeriod", value="100ms"),
            dict(name="_MoniumDriverSecure", value="false"),
        ])
        if has_logbroker_federation():
            config["yql_agent"]["pq_gateway_config"] = dict(cluster_mapping=[dict(
                name="logbroker",
                endpoint=get_logbroker_endpoint(LogbrokerClient.LOGBROKER_CLUSTER),
                token="dummy_token",
                database=LogbrokerClient.LOGBROKER_DATABASE,
                config_manager_endpoint=get_logbroker_cm_endpoint(),
                add_bearer_to_token=False,
            )])

        config["yql_agent"]["solomon_gateway_config"] = dict(cluster_mapping=[dict(
            name="solomon",
            cluster=os.getenv("SOLOMON_MOCK_ENDPOINT", "localhost:0"),
            token="dummy_token",
        )])

    @classmethod
    def extend_debug_gateways_config(cls, gateways_config, yql_agent_config):
        gateways_config["solomon"] = yql_agent_config["yql_agent"]["solomon_gateway_config"]
        if has_logbroker_federation():
            gateways_config["pq"] = yql_agent_config["yql_agent"]["pq_gateway_config"]

    def get_additional_query_pragmas(self):
        return f"""
pragma Ytflow.LogbrokerConsumerPath = "{self.LOGBROKER_CONSUMER}";
pragma Ytflow.LogbrokerWriteCompressionCodec = "{self.LOGBROKER_COMPRESSION_CODEC}";
pragma Ytflow.LogbrokerWriteCompressionLevel = "{self.LOGBROKER_COMPRESSION_LEVEL}";
"""

    def _write_logbroker_topic(self, topic_path, data, logbroker_client):
        with closing(logbroker_client.create_topic_writer(topic_path)) as topic_writer:
            topic_writer.write(data)

    def _read_logbroker_topic(self, topic_path, logbroker_client):
        with closing(logbroker_client.create_topic_reader(
            topic_path, self.LOGBROKER_CONSUMER
        )) as topic_reader:
            batch = topic_reader.receive_batch()
            topic_reader.commit_with_ack(batch)
            return batch.messages

    def _assert_logbroker_topic_content(self, topic_path, expected_data, logbroker_client):
        actual_data = [
            lb_data.message.data.decode()
            for lb_data in self._read_logbroker_topic(topic_path, logbroker_client)
        ]

        assert_items_equal(actual_data, expected_data)

    def _assert_solomon_shard_content(
        self, shard_name, expected_data, solomon_client, sensors={"counter"}
    ):
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

        assert_items_equal(
            result_metrics,
            sorted(expected_metrics, key=lambda metric: (metric["ts"], metric["labels"])),
        )

    def _convert_solomon_metrics_to_yt_format(
        self, metrics, timestamp_column="metric_timestamp"
    ):
        def convert_timestamp(metric):
            converted_metric = metric.copy()
            if converted_metric[timestamp_column] is not None:
                converted_metric[timestamp_column] *= 1000000
            return converted_metric

        return list(map(convert_timestamp, metrics))


class TestYtflowLogbroker(TestYtflowExtraProvidersBase):
    NUM_TEST_PARTITIONS = 8

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

        # TODO (artemmashin): revert to 'select *' after fix in pq provider
        run_query(f"""
insert into logbroker.`{out_topic_path}`
select Data from logbroker.`{input_topic_path}`;
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
$stream = select Data from logbroker.`{input_topic_path}`;

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
$stream = select Data from logbroker.`{input_topic_path}`;

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
$stream = select Data from logbroker.`{input_topic_path}`;

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


class TestYtflowSolomon(TestYtflowExtraProvidersBase):
    NUM_TEST_PARTITIONS = 8

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
