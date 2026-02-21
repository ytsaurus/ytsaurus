from common import TestQueriesYqlBase

from yt.environment.helpers import assert_items_equal, wait_for_dynamic_config_update

from yt_commands import (authors, create, create_user, sync_mount_table,
                         write_table, insert_rows, alter_table, raises_yt_error,
                         write_file, create_pool, wait, get, set, ls, list_operations,
                         get_operation, issue_token, create_group)

from yt_helpers import profiler_factory

from yt_queries import get_query_tracker_info, get_query_declared_parameters_info

from yt.wrapper import yson, YtError

import yt_error_codes

import pytest

import re


class TestQueriesYqlSimpleBase(TestQueriesYqlBase):
    def _run_simple_query(self, query, **kwargs):
        result_format_kwargs = {}
        if "result_output_format" in kwargs:
            result_format_kwargs["output_format"] = kwargs["result_output_format"]

        query = self.start_query("yql", query, **kwargs)
        query.track()
        query_info = query.get()
        if query_info["result_count"] == 0:
            return None
        elif query_info["result_count"] == 1:
            return query.read_result(0, **result_format_kwargs)
        else:
            return [query.read_result(i, **result_format_kwargs) for i in range(query_info["result_count"])]

    def _test_simple_query(self, query, expected, **kwargs):
        result = self._run_simple_query(query, **kwargs)
        if expected is None:
            assert result is None
            return

        assert_items_equal(result, expected)

    def _test_simple_query_error(self, query, expected, **kwargs):
        try:
            self._run_simple_query(query, **kwargs)
        except YtError as err:
            assert err.contains_text(expected)
            return
        assert False

    def _exists_pending_stage_in_progress(self, query):
        queryInfo = query.get()
        if "progress" in queryInfo and "yql_progress" in queryInfo["progress"]:
            for stage in queryInfo["progress"]["yql_progress"].values():
                if "pending" in stage and stage["pending"] > 0 :
                    return True
        return False


class TestStackOverflow(TestQueriesYqlSimpleBase):
    @authors("mpereskokova")
    def test_stack_overflow(self, query_tracker, yql_agent):

        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "x", "type": "string"}]
        })
        create("table", "//tmp/t2", attributes={
            "schema": [{"name": "x", "type": "string"}]
        })
        rows1 = [{"x": str(i)} for i in range(1024)]
        rows2 = [{"x": str(i)} for i in range(256)]

        write_table("//tmp/t1", rows1)
        write_table("//tmp/t2", rows2)

        query = self.start_query("yql", """
            $src = "//tmp/t1";
            $small_table = "//tmp/t2";

            DEFINE ACTION $split_by_dataset($ds) AS
                $out = "//tmp" || $ds;

                INSERT INTO $out WITH TRUNCATE
                SELECT *
                FROM $src
                WHERE x = $ds;

                COMMIT;
            END DEFINE;

            $datasets = (
                SELECT AGGREGATE_LIST(x)
                FROM $small_table
            );

            EVALUATE FOR $row IN $datasets
                DO $split_by_dataset($row);
        """, settings={"execution_mode": "validate"})
        query.track()


class TestNotTableResult(TestQueriesYqlSimpleBase):
    @authors("mpereskokova")
    def test_not_table_result(self, query_tracker, yql_agent):
        error = "Non-table results are not supported in Query Tracker. Expected List<Struct<…>> but found \"List<List<Struct<'a':Int32,'id':Uint32>>>"
        with raises_yt_error(error):
            self._run_simple_query("""
                $data = select * FROM AS_TABLE([
                    <|id: 1u, val: "a"|>,
                    <|id: 2u, val: "b"|>
                ]);

                $udf = ($key, $values) -> (
                    Just([<|id: $key, a: 1|>])
                );

                REDUCE $data
                on id
                using $udf(TableRow())
            """)


class TestGetOperationLink(TestQueriesYqlSimpleBase):
    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_operation_link(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)
        query = self.start_query("yql", 'select a+1 as result from primary.`//tmp/t`')
        query.track()

        op = list_operations()["operations"][0]["id"]
        op_cluster = get_operation(op)["runtime_parameters"]["annotations"]["description"]["yql_op_url"]
        assert str(op_cluster) == "primary"
        op_runner = get_operation(op)["runtime_parameters"]["annotations"]["description"]["yql_runner"]
        assert str(op_runner) == "yql-agent"


class TestMetrics(TestQueriesYqlSimpleBase):
    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_metrics(self, query_tracker, yql_agent):
        yqla = ls("//sys/yql_agent/instances")[0]
        profiler = profiler_factory().at_yql_agent(yqla)

        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        create_pool("small", attributes={"resource_limits": {"user_slots": 0}})
        query = self.start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')

        wait(lambda: self._exists_pending_stage_in_progress(query))

        active_queries_metric = profiler.gauge("yql_agent/active_queries")
        wait(lambda: active_queries_metric.get() == 1)

        set("//sys/pools/small/@resource_limits/user_slots", 1)

        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"result": 43}])

        wait(lambda: active_queries_metric.get() == 0)


class TestSimpleQueriesYql(TestQueriesYqlSimpleBase):
    NUM_TEST_PARTITIONS = 4

    @authors("max42")
    def test_simple(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]
        })
        rows = [{"a": 42, "b": "foo"}, {"a": -17, "b": "bar"}]
        write_table("//tmp/t", rows)
        self._test_simple_query("select * from primary.`//tmp/t`", rows)

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_simple_insert(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]
        })
        rows = [{"a": 42, "b": "foo"}]

        self._test_simple_query("insert into primary.`//tmp/t`(a, b) values (42, 'foo')", None)
        self._test_simple_query("select * from primary.`//tmp/t`", rows)

    @authors("max42")
    def test_issues(self, query_tracker, yql_agent):
        with raises_yt_error(30000):
            self._run_simple_query("select * from primary.`//tmp/nonexistent`")

    @authors("max42")
    @pytest.mark.timeout(180)
    def test_schemaful_read(self, query_tracker, yql_agent):
        schema = [
            {"name": "a", "type": "int64"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "boolean"},
            {"name": "d", "type": "double"},
        ]
        schema_new = [
            {"name": "b", "type": "string"},
            {"name": "d", "type": "double"},
            {"name": "c", "type": "boolean"},
            {"name": "a", "type": "int64"},
        ]
        create("table", "//tmp/t", attributes={"schema": schema})
        rows = [
            {"a": 42, "b": "foo", "c": True, "d": 3.14},
        ]
        write_table("<append=%true>//tmp/t", rows)
        alter_table("//tmp/t", schema=schema_new)

        self._test_simple_query("""
            select * from primary.`//tmp/t`;
            select c, b from primary.`//tmp/t`
        """, [rows, [{"b": "foo", "c": True}]])

    @authors("mpereskokova")
    def test_calc(self, query_tracker, yql_agent):
        self._test_simple_query("""
            select 1;
            select 1 + 1;
            select 2 as b, 1 as a
        """, [[{"column0": 1}], [{"column0": 2}], [{"a": 1, "b": 2}]])


class TestLibs(TestQueriesYqlSimpleBase):
    YQL_TEST_LIBRARY = """
            $my_sqr = ($x)->($x * $x);
            export $my_sqr;
        """

    @authors("a-romanov")
    def test_libs(self, query_tracker, yql_agent):
        self._test_simple_query("""
            select core::IndexOf([3,7,1], 7) as idx, test::my_sqr(3) as sqr;
        """, [{"idx": 1, "sqr": 9}])


class TestTypes(TestQueriesYqlSimpleBase):
    NUM_TEST_PARTITIONS = 4

    @authors("a-romanov")
    def test_datetime_types(self, query_tracker, yql_agent):
        self._test_simple_query("""
            select
                Date("2024-11-24") as `Date`,
                Datetime("2024-11-24T11:20:59Z") as `Datetime`,
                Timestamp("2024-11-24T13:42:11.666Z") as `Timestamp`,
                -Interval("P2W") as `Interval`,
                Date32("1960-11-24") as `Date32`,
                Datetime64("1950-11-24T11:20:59Z") as `Datetime64`,
                Timestamp64("1940-11-24T13:42:11.666Z") as `Timestamp64`,
                -Interval64("PT42M") as `Interval64`,
        """, [{"Date": 20051,
               "Datetime": 1732447259,
               "Timestamp": 1732455731666000,
               "Interval": -1209600000000,
               "Date32": -3325,
               "Datetime64": -602858341,
               "Timestamp64": -918382668334000,
               "Interval64": -2520000000
               }])

    @authors("a-romanov")
    def test_datetime_tz_types(self, query_tracker, yql_agent):
        self._test_simple_query("""
            select
                TzDate("2024-11-25,CET") as `TzDate`,
                TzDatetime("2024-11-24T11:20:59,Australia/NSW") as `TzDatetime`,
                TzTimestamp("2024-11-24T13:42:11,Africa/Nairobi") as `TzTimestamp`,
                TzDate32("1960-11-24,America/Cayenne") as `TzDate32`,
                TzDatetime64("1950-11-24T11:20:59,Europe/Samara") as `TzDatetime64`,
                TzTimestamp64("1940-11-24T13:42:11,Iceland") as `TzTimestamp64`,
        """, [{"TzDate": "2024-11-25,CET",
               "TzDatetime": "2024-11-24T11:20:59,Australia/NSW",
               "TzTimestamp": "2024-11-24T13:42:11,Africa/Nairobi",
               "TzDate32": "1960-11-24,America/Cayenne",
               "TzDatetime64": "1950-11-24T11:20:59,Europe/Samara",
               "TzTimestamp64": "1940-11-24T13:42:11,Iceland"
               }])

    @authors("a-romanov")
    def test_exotic_types(self, query_tracker, yql_agent):
        self._test_simple_query("""
            select
                {} as `EmptyDict`,
                [] as `EmptyList`,
                Null as `Null`,
                <| Signed : -13, Unsigned : 42U, Y : '{"key" = 3.14}'y |> as Struct,
                ((-13, 42U, false), "foo", true, "bar"u) as Tuple,
                [-2.5f, 3.f] as `List`,
                {"one"u: 1, "two"u: 2} as `Dict`,
                {("One"u, 1UL), ("Two"u, 2UL)} as `Set`,
                AsVariant(88L, "var") as `Variant`,
                AsTagged(123, "tag") as `Tagged`,
                Decimal("123456.789", 13, 3) as `Decimal`,
                '[1, "text", 3.14]'j as `Json`,
                '[7u; "str"; -3.14]'y as `Yson`,
        """, [{"EmptyDict": None,
               "EmptyList": None,
               "Null": None,
               "Struct": {"Signed": -13, "Unsigned": 42, "Y": {"key": 3.14}},
               "Tuple": [[-13, 42, False], "foo", True, "bar"], "List": [-2.5, 3.],
               "Dict": [["two", 2], ["one", 1]],
               "Set": [[["Two", 2], None], [["One", 1], None]],
               "Variant": ["var", 88],
               "Tagged": 123,
               "Decimal": b"\x80\0\0\0\7[\xCD\x15",
               "Json": '[1, "text", 3.14]',
               "Yson": [7, 'str', -3.14]
               }])

    @authors("mpereskokova")
    def test_web_json_decimal(self, query_tracker, yql_agent):
        format = yson.loads(b'<value_format=yql>web_json')
        self._test_simple_query(
            """select Decimal("123456.789", 13, 3) as `Decimal`""",
            b'{"rows":[{"Decimal":["123456.789","8"]}],"incomplete_columns":"false","incomplete_all_column_names":"false","all_column_names":["Decimal"],"yql_type_registry":[["NullType"],'
            b'["DataType","Int64"],["DataType","Uint64"],["DataType","Double"],["DataType","Boolean"],["DataType","String"],["DataType","Yson"],["DataType","Yson"],["DataType","Decimal","13","3"]]}',
            result_output_format=format)

    @authors("mpereskokova")
    def test_optional_and_tagged(self, query_tracker, yql_agent):
        self._test_simple_query("""
            select
                Just(2) as `SimpleOptional`,
                Just(Just(2)) as `DoubleOptional`,
                AsTagged(AsTuple(AsTagged(1, "tag1"), Just(Just(2))), "tag2") as `TaggedTupple`,
                AsTagged(AsTagged(1, "tag1"), "tag2") as `NestedTagged`\
            """, [{"SimpleOptional": 2, "DoubleOptional": [2], "TaggedTupple": [1, [2]], "NestedTagged": 1}])

    @authors("a-romanov")
    def test_double_optional(self, query_tracker, yql_agent):
        self._test_simple_query("""
            $list = ["abc"u, null];
            select $list, $list[0], $list[1], $list[2],
            (just($list[0]), just($list[1]), just($list[2])),
            {$list[0], $list[1], $list[2]};
        """, [{'column0': ['abc', None], 'column1': ['abc'], 'column2': [None], 'column3': None,
               'column4': [[['abc']], [[None]], [None]],
               'column5': [[None, None], [[None], None], [['abc'], None]]}])


class TestYqlAgentBan(TestQueriesYqlSimpleBase):
    NUM_YQL_AGENTS = 1
    YQL_AGENT_DYNAMIC_CONFIG = {"state_check_period": 2000}

    def _test_query_fails(self):
        query = self.start_query("yql", 'select 1')
        query.track()
        return False

    def _test_query_completes(self):
        query = self.start_query("yql", 'select 1')
        try:
            query.track()
        except Exception:
            return False
        return True

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_yql_agent_ban_on_new_queries(self, query_tracker, yql_agent):
        address = yql_agent.yql_agent.addresses[0]
        set(f"//sys/yql_agent/instances/{address}/@banned", True)

        with raises_yt_error(yt_error_codes.Unavailable) as err:
            wait(self._test_query_fails)
        assert err[0].contains_text("No alive peers found")

        set(f"//sys/yql_agent/instances/{address}/@banned", False)

        wait(self._test_query_completes)

    @authors("mpereskokova")
    def test_yql_agent_ban_on_existing_queries(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        create_pool("small", attributes={"resource_limits": {"user_slots": 0}})
        long_query = self.start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')
        wait(lambda: long_query.get()["state"] == "running")

        address = yql_agent.yql_agent.addresses[0]
        set(f"//sys/yql_agent/instances/{address}/@banned", True)

        with raises_yt_error(yt_error_codes.Unavailable) as err:
            wait(self._test_query_fails)
        assert err[0].contains_text("No alive peers found")

        set("//sys/pools/small/@resource_limits/user_slots", 1)
        long_query.track()


class TestYqlAgentDynConfig(TestQueriesYqlSimpleBase):
    NUM_TEST_PARTITIONS = 16
    CLASS_TEST_LIMIT = 30 * 60
    NUM_YQL_AGENTS = 1

    def _update_dyn_config(self, yql_agent, dyn_config):
        config = get("//sys/yql_agent/config")
        config["yql_agent"] = dyn_config
        set("//sys/yql_agent/config", config)
        wait_for_dynamic_config_update(yql_agent.yql_agent.client, config, "//sys/yql_agent/instances")

    @authors("lucius")
    @pytest.mark.timeout(180)
    def test_yql_agent_dyn_config(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]
        })
        rows = [{"a": 42, "b": "foo"}, {"a": -17, "b": "bar"}]
        write_table("//tmp/t", rows)
        self._test_simple_query("select * from primary.`//tmp/t`", rows)

        self._update_dyn_config(yql_agent, {
            "gateways": {
                "yt": {
                    "cluster_mapping": [
                    ],
                },
            },
        })
        self._test_simple_query("select * from primary.`//tmp/t`", rows)

    @authors("lucius")
    @pytest.mark.timeout(300)
    def test_yql_agent_dyn_config_replace_cluster(self, query_tracker, yql_agent):
        instances = ls("//sys/yql_agent/instances")
        assert instances
        config_path = "//sys/yql_agent/instances/" + instances[0] + "/orchid/config"
        config = get(config_path)
        cluster_mapping = config["yql_agent"]["gateway_config"]["cluster_mapping"]
        # expected cluster_mapping =
        # [
        #     {
        #         'default': true,
        #         'cluster': 'localhost:29782',
        #         'name': 'primary',
        #         'settings': [
        #             {'name': 'QueryCacheChunkLimit', 'value': '100000'},
        #             {'name': '_UseKeyBoundApi', 'value': 'true'}
        #         ]
        #     }
        # ]
        assert len(cluster_mapping) == 1
        primary = cluster_mapping[0]
        assert primary["name"] == "primary"
        assert primary["settings"]
        settings = {s["name"]: s["value"] for s in primary["settings"]}
        assert "QueryCacheChunkLimit" in settings
        assert "_EnableDq" not in settings

        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]
        })
        rows = [{"a": 42, "b": "foo"}, {"a": -17, "b": "bar"}]
        write_table("//tmp/t", rows)
        self._test_simple_query("select * from primary.`//tmp/t`", rows)

        # add new attr and update existing one
        self._update_dyn_config(yql_agent, {
            "gateways": {
                "yt": {
                    "cluster_mapping": [
                        {
                            "name": "primary",
                            "settings": [
                                {"name": "_EnableDq", "value": "true"},
                                {"name": "QueryCacheChunkLimit", "value": "200000"},
                            ],
                        },
                    ],
                },
            },
        })
        self._test_simple_query("select * from primary.`//tmp/t`", rows)

    def _dyn_config_expect_error(self, yql_agent, dyn_config, expected_error):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        self._update_dyn_config(yql_agent, dyn_config)
        with raises_yt_error(expected_error):
            self._test_simple_query("select * from primary.`//tmp/t`", rows)

        # should work after fixing
        self._update_dyn_config(yql_agent, {
            "gateways": {
                "yt": {
                    "cluster_mapping": [
                    ],
                },
            },
        })
        self._test_simple_query("select * from primary.`//tmp/t`", rows)

    @authors("lucius")
    @pytest.mark.timeout(600)
    def test_yql_agent_broken_dyn_config_without_address(self, query_tracker, yql_agent):
        self._dyn_config_expect_error(
            yql_agent,
            {
                "gateways": {
                    "yt": {
                        "cluster_mapping": [
                            {
                                "name": "cluster_without_address",
                            },
                        ],
                    },
                },
            },
            "Cluster address must be specified",
        )

    @authors("lucius")
    @pytest.mark.timeout(600)
    def test_yql_agent_broken_dyn_config_wrong_address(self, query_tracker, yql_agent):
        self._dyn_config_expect_error(
            yql_agent,
            {
                "gateways": {
                    "yt": {
                        "cluster_mapping": [
                            {
                                "name": "primary",
                                "cluster": "host_does_not_exist:80",
                            },
                        ],
                    },
                },
            },
            "can not resolve host_does_not_exist:80",
        )

    @authors("lucius")
    @pytest.mark.timeout(600)
    def test_yql_agent_broken_dyn_config_wrong_settings(self, query_tracker, yql_agent):
        self._dyn_config_expect_error(
            yql_agent,
            {
                "gateways": {
                    "yt": {
                        "cluster_mapping": [
                            {
                                "name": "primary",
                                "settings": [
                                    {"name": "QueryCacheChunkLimit", "value": "-1.23"},
                                ],
                            },
                        ],
                    },
                },
            },
            """Bad "QueryCacheChunkLimit" setting for "primary" cluster""",
        )


class TestComplexQueriesYql(TestQueriesYqlSimpleBase):
    NUM_TEST_PARTITIONS = 4

    @authors("mpereskokova")
    def test_count(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": -17}]
        write_table("//tmp/t", rows)

        self._test_simple_query("select count(*) from primary.`//tmp/t`", [{"column0": 2}])

    @authors("mpereskokova")
    def test_union(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t1", rows)

        create("table", "//tmp/t2", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 43}]
        write_table("//tmp/t2", rows)

        self._test_simple_query(
            "select * from `//tmp/t1` union all select * from `//tmp/t2`",
            [{"a": 42}, {"a": 43}],
        )

    @authors("mpereskokova")
    @pytest.mark.timeout(300)
    def test_complex_query(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        self._test_simple_query("select sum(1) from (select * from `//tmp/t1`)", [{"column0": 2}])

    @authors("aleksandr.gaev")
    @pytest.mark.timeout(300)
    def test_sum(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        self._test_simple_query("select sum(a) from `//tmp/t1`", [{"column0": 85}])

    @authors("aleksandr.gaev")
    def test_zeros_in_settings(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 45}]
        write_table("//tmp/t1", rows)

        self._test_simple_query("select * from `//tmp/t1`", [{"a": 45}], settings={"random_attribute": 0})


class TestExecutionModesYql(TestQueriesYqlSimpleBase):
    NUM_TEST_PARTITIONS = 16

    @authors("aleksandr.gaev")
    def test_validate(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        for mode in ["validate", 0]:
            query = self.start_query("yql", "select * from `//tmp/t1`", settings={"execution_mode": mode})
            query.track()
            result = query.get()
            assert result["result_count"] == 0
            assert result["progress"]["yql_plan"]["Basic"] == {'nodes': [{'id': 1, 'level': 1, 'name': 'Commit! on primary #1', 'type': 'op'}], 'links': []}

    @authors("aleksandr.gaev")
    def test_optimize(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        for mode in ["optimize", 1]:
            query = self.start_query("yql", "select * from `//tmp/t1`", settings={"execution_mode": mode})
            query.track()
            result = query.get()
            assert result["result_count"] == 0
            assert len(result["progress"]["yql_plan"]["Basic"]["nodes"]) > 2

    @authors("aleksandr.gaev")
    def test_run(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        for mode in ["run", 2]:
            self._test_simple_query("select * from `//tmp/t1`", rows, settings={"execution_mode": mode})

    @authors("aleksandr.gaev")
    def test_unknown_execution_modes(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        with raises_yt_error('Error parsing'):
            self._run_simple_query("select * from `//tmp/t1`", settings={"execution_mode": "unknown"})

        with raises_yt_error("Error casting"):
            self._run_simple_query("select * from `//tmp/t1`", settings={"execution_mode": 42})


class TestYqlPlugin(TestQueriesYqlSimpleBase):
    NUM_TEST_PARTITIONS = 4

    @authors("mpereskokova")
    def test_default_cluster_read(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        self._test_simple_query("select * from `//tmp/t`", rows)

    @authors("mpereskokova")
    def test_dyntable_read(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "int64"}, {"name": "c", "type": "string"}],
            "dynamic": True,
            "enable_dynamic_store_read": True,
        })
        sync_mount_table("//tmp/t")

        rows = [{"a": 42, "b": 43, "c": "test"}]
        insert_rows("//tmp/t", rows)

        self._test_simple_query("""
            select * from `//tmp/t`;
            select c, a from `//tmp/t`
        """, [rows, [{"a": 42, "c": "test"}]])

    @authors("mpereskokova")
    def test_pragma_refselect(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "int64"}, {"name": "c", "type": "string"}],
            "dynamic": True,
            "enable_dynamic_store_read": True,
        })
        sync_mount_table("//tmp/t")

        rows = [{"a": 42, "b": 43, "c": "test"}]
        insert_rows("//tmp/t", rows)

        self._test_simple_query("""
            pragma RefSelect;

            select * from `//tmp/t`;
            select c, a from `//tmp/t`
        """, [rows, [{"a": 42, "c": "test"}]])


class TestDefaultCluster(TestQueriesYqlSimpleBase):
    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_exists_cluster(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        self._test_simple_query("select a + 1 as b from primary.`//tmp/t`;", [{"b": 43}], settings={"cluster": "primary"})

    @authors("mpereskokova")
    def test_unknown_cluster(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        with raises_yt_error(1):  # Generic error
            self._run_simple_query("select a + 1 from primary.`//tmp/t`;", settings={"cluster": "unknown_cluster"})


class TestAllYqlAgentsOverload(TestQueriesYqlSimpleBase):
    YQL_AGENT_DYNAMIC_CONFIG = {"max_simultaneous_queries": 1}
    NUM_YQL_AGENTS = 1

    @authors("mpereskokova")
    @pytest.mark.timeout(300)
    def test_yql_agent_overload(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        create_pool("small", attributes={"resource_limits": {"user_slots": 0}})

        q1 = self.start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')
        wait(lambda: q1.get()["state"] == "running")

        q2 = self.start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')
        wait(lambda: q2.get()["state"] == "running")
        wait(lambda: q2.get()["state"] == "pending")

        set("//sys/pools/small/@resource_limits/user_slots", 1)

        q1.track()
        q2.track()


class TestPartialYqlAgentsOverload(TestQueriesYqlSimpleBase):
    YQL_AGENT_DYNAMIC_CONFIG = {"max_simultaneous_queries": 1}
    NUM_YQL_AGENTS = 2

    @authors("mpereskokova")
    @pytest.mark.timeout(300)
    def test_yql_agent_overload(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        create_pool("small", attributes={"resource_limits": {"user_slots": 0}})

        q1 = self.start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')
        q2 = self.start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')

        wait(lambda: q1.get()["state"] == "running")
        wait(lambda: q2.get()["state"] == "running")

        set("//sys/pools/small/@resource_limits/user_slots", 1)

        q1.track()
        q2.track()


class TestYqlAgent(TestQueriesYqlSimpleBase):
    NUM_TEST_PARTITIONS = 16

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_progress(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        create_pool("small", attributes={"resource_limits": {"user_slots": 0}})
        query = self.start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')

        wait(lambda: self._exists_pending_stage_in_progress(query))

        set("//sys/pools/small/@resource_limits/user_slots", 1)

        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"result": 43}])

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_files(self, query_tracker, yql_agent):
        self._test_simple_query(
            "select FileContent(\"test_file_raw\") as column",
            [{"column": "test_content"}],
            files=[{"name": "test_file_raw", "content": "test_content", "type": "raw_inline_data"}],
        )

        # check downloading files by links
        create("file", "//tmp/test_file")
        write_file("//tmp/test_file", b"test_file_content")
        file_link = "http://" + self.Env.get_http_proxy_address() + "/api/v3/read_file?path=//tmp/test_file"

        self._test_simple_query(
            "select FileContent(\"long_link\"); select FileContent(\"short_link\")",
            [[{"column0": "test_file_content"}], [{"column0": "test_file_content"}]],
            files=[
                {"name": "long_link", "content": file_link, "type": "url"},
                {"name": "short_link", "content": "yt://primary/tmp/test_file", "type": "url"},
            ],
        )

    @authors("apollo1321")
    def test_config_defaults(self, query_tracker, yql_agent):
        instances = ls("//sys/yql_agent/instances")
        for instance in instances:
            config = get("//sys/yql_agent/instances/" + instance + "/orchid/config")

            gateway_config = config["yql_agent"]["gateway_config"]
            assert len(gateway_config["remote_file_patterns"]) == 1
            assert gateway_config["remote_file_patterns"][0]["pattern"] == "yt://([a-zA-Z0-9\\-_]+)/([^&@?]+)$"
            assert gateway_config["yt_log_level"] == "YL_DEBUG"
            assert not gateway_config["execute_udf_locally_if_possible"]
            assert len(gateway_config["cluster_mapping"]) == 1
            assert len(gateway_config["cluster_mapping"][0]["settings"]) == 2
            assert len(gateway_config["default_settings"]) == 61

            setting_found = False
            for setting in gateway_config["default_settings"]:
                if setting["name"] == "DefaultCalcMemoryLimit":
                    assert setting["value"] == "2G"
                    setting_found = True
            assert setting_found

            file_storage_config = config["yql_agent"]["file_storage_config"]
            assert file_storage_config["retry_count"] == 3
            assert file_storage_config["max_files"] == 1 << 13
            # The default should be overwritten from 1 << 14.
            assert file_storage_config["max_size_mb"] == 1 << 13


def assert_full_result(query_result):
    assert "full_result" in query_result
    full_result = query_result["full_result"]
    assert isinstance(full_result, yson.YsonMap)
    assert "cluster" in full_result
    assert isinstance(full_result["cluster"], str)
    assert "table_path" in full_result
    assert isinstance(full_result["table_path"], str)


class TestQueriesYqlLimitedResult(TestQueriesYqlSimpleBase):
    QUERY_TRACKER_DYNAMIC_CONFIG = {"yql_engine": {"row_count_limit": 1}}

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_rows_limit(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "sort_order": "ascending", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}, {"a": 44}]
        write_table("//tmp/t1", rows)

        query = self.start_query("yql", "select * from `//tmp/t1`")
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": 42}])
        assert query.get_result(0)["is_truncated"]

        query = self.start_query("yql", "select * from `//tmp/t1` limit 1")
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": 42}])
        assert not query.get_result(0)["is_truncated"]


class TestQueriesYqlResultTruncation(TestQueriesYqlSimpleBase):
    NUM_TEST_PARTITIONS = 4
    CLASS_TEST_LIMIT = 1800
    QUERY_TRACKER_DYNAMIC_CONFIG = {"yql_engine": {"resulting_rowset_value_length_limit": 20 * 1024**2}}

    def _assert_select_result(self, path, rows, is_truncated, has_full_result):
        q = self.start_query("yql", f"select * from `{path}`")
        q.track()
        assert q.get()["result_count"] == 1
        assert_items_equal(q.read_result(0), rows)
        assert q.get_result(0)["is_truncated"] == yson.YsonBoolean(is_truncated)
        if has_full_result:
            assert_full_result(q.get_result(0))
        else:
            assert "full_result" not in q.get_result(0)

    @authors("aleksandr.gaev")
    @pytest.mark.timeout(600)
    def test_big_result(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "value", "type": "string"}]
        })

        value_size = 1024**2

        # 14 MB
        rows = [{"value": str(i) + ''.join(['a' for _ in range(value_size)])} for i in range(14)]
        write_table("//tmp/t", rows)
        self._assert_select_result("//tmp/t", rows, False, True)

        # 15 MB
        new_rows = [{"value": str(i) + ''.join(['b' for _ in range(value_size)])} for i in range(14, 15)]
        rows += new_rows
        write_table("//tmp/t", rows)
        self._assert_select_result("//tmp/t", rows, False, True)

        # 16 MB
        new_rows = [{"value": str(i) + ''.join(['c' for _ in range(value_size)])} for i in range(15, 16)]
        rows += new_rows
        write_table("//tmp/t", rows)
        self._assert_select_result("//tmp/t", rows[:15], True, True)

        # 17 MB
        new_rows = [{"value": str(i) + ''.join(['d' for _ in range(value_size)])} for i in range(16, 17)]
        rows += new_rows
        write_table("//tmp/t", rows)
        self._assert_select_result("//tmp/t", rows[:15], True, True)

        # 22 MB
        new_rows = [{"value": str(i) + ''.join(['d' for _ in range(value_size)])} for i in range(17, 22)]
        rows += new_rows
        write_table("//tmp/t", rows)
        self._assert_select_result("//tmp/t", rows[:15], True, True)

    @authors("aleksandr.gaev")
    @pytest.mark.timeout(360)
    def test_big_line(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "value", "type": "string"}]
        })

        # 12 MB line
        rows = [{"value": "a"}, {"value": ''.join(['a' for _ in range(12 * 1024**2)])}]
        write_table("//tmp/t", rows, table_writer={"max_row_weight": 64 * 1024**2})
        self._assert_select_result("//tmp/t", rows, False, False)

        # 16 MB line
        rows = [{"value": "a"}, {"value": ''.join(['a' for _ in range(16 * 1024**2)])}]
        write_table("//tmp/t", rows, table_writer={"max_row_weight": 64 * 1024**2})
        self._assert_select_result("//tmp/t", rows[:1], True, False)

        # 19 MB line
        rows = [{"value": "a"}, {"value": ''.join(['a' for _ in range(19 * 1024**2)])}]
        write_table("//tmp/t", rows, table_writer={"max_row_weight": 64 * 1024**2})
        self._assert_select_result("//tmp/t", rows[:1], True, False)

    @authors("aleksandr.gaev")
    @pytest.mark.timeout(180)
    def test_line_above_limit(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "value", "type": "string"}]
        })

        # 22 MB line
        rows = [{"value": "a"}, {"value": ''.join(['a' for _ in range(22 * 1024**2)])}]
        write_table("//tmp/t", rows, table_writer={"max_row_weight": 64 * 1024**2})
        q = self.start_query("yql", "select * from `//tmp/t`")
        q.track()
        assert q.get()["result_count"] == 1
        with raises_yt_error("Failed to save rowset"):
            q.read_result(0)
        result = q.get_result(0)
        assert result["is_truncated"] == yson.YsonBoolean(True)
        assert "error" in result
        assert "Failed to save rowset" in str(result["error"])
        assert "Failed to read resulting rowset. Try using INSERT INTO to save result" in str(result["error"])
        assert "full_result" not in result

    @authors("a-romanov")
    @pytest.mark.timeout(180)
    def test_big_result_with_empty_string_in_yson(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "value", "type": "string"}]
        })

        value_size = 1024**2

        # 22 MB
        rows = [{"value": ''.join(['d' for _ in range(value_size)])} for i in range(22)]
        write_table("//tmp/t", rows)

        expected = [{"value": ''.join(['d' for _ in range(value_size)]), "check": ""}]
        q = self.start_query("yql", "select value, ('[\"\"]'y).0 as `check` from `//tmp/t`")
        q.track()
        assert q.get()["result_count"] == 1
        assert q.get_result(0)["is_truncated"]
        assert_items_equal(q.read_result(0)[:1], expected)


class TestQueriesYqlAuthBase(TestQueriesYqlSimpleBase):
    DELTA_HTTP_PROXY_CONFIG = {
        "auth" : {"enable_authentication": True}
    }

    def setup_method(self, method):
        super().setup_method(method)
        create_user("denied_user")
        create_user("allowed_user")

        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}],
            "acl": [
                {"action": "deny", "subjects": ["denied_user"], "permissions": ["read"]},
                {"action": "allow", "subjects": ["allowed_user"], "permissions": ["read"]},
            ]
        })
        write_table("//tmp/t", [{"a": 42}])


class TestQueriesYqlAuth(TestQueriesYqlAuthBase):
    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_yql_agent_impersonation_deny(self, query_tracker, yql_agent):
        with raises_yt_error("failed"):
            self._run_simple_query("select a + 1 as b from primary.`//tmp/t`;", authenticated_user="denied_user")

    @authors("mpereskokova")
    @pytest.mark.timeout(180)
    def test_yql_agent_impersonation_allow(self, query_tracker, yql_agent):
        self._test_simple_query("select a + 1 as b from primary.`//tmp/t`;", [{"b": 43}], authenticated_user="allowed_user")


class TestQueriesYqlWithSecrets(TestQueriesYqlAuthBase):
    NUM_TEST_PARTITIONS = 8

    @authors("ngc224")
    @pytest.mark.timeout(180)
    @pytest.mark.parametrize(
        "secret_node_type", ["document", "string_node", "file"]
    )
    def test_secret_with_provided_category(self, query_tracker, yql_agent, secret_node_type):
        def run_query(username):
            token, token_hash = issue_token(username)

            vault_token_path = f"//tmp/vault/{username}_token"
            create(
                secret_node_type, vault_token_path,
                recursive=True,
            )

            if secret_node_type == "file":
                write_file(vault_token_path, token.encode('utf8'))
            else:
                set(vault_token_path, token)

            self._test_simple_query(
                "pragma yt.auth = 'custom_secret'; select a + 1 as b from primary.`//tmp/t`;",
                [{"b": 43}],
                secrets=[{"id": "custom_secret", "category": "yt", "ypath": vault_token_path}],
            )

        run_query("allowed_user")

        with raises_yt_error('Access denied for user "denied_user"'):
            run_query("denied_user")

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_secret_with_trailing_newline(self, query_tracker, yql_agent):
        def run_query(username):
            token, token_hash = issue_token(username)

            vault_token_path = f"//tmp/vault/{username}_token"
            create(
                "file", vault_token_path,
                recursive=True,
            )

            write_file(vault_token_path, token.encode('utf8') + b'\n')

            self._test_simple_query(
                "pragma yt.auth = 'custom_secret'; select a + 1 as b from primary.`//tmp/t`;",
                [{"b": 43}],
                secrets=[{"id": "custom_secret", "category": "yt", "ypath": vault_token_path}],
            )

        run_query("allowed_user")

        with raises_yt_error('Access denied for user "denied_user"'):
            run_query("denied_user")

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_secret_with_discovered_category_from_attribute(self, query_tracker, yql_agent):
        def run_query(username):
            token, token_hash = issue_token(username)

            vault_token_path = f"//tmp/vault/{username}_token"
            create(
                "file", vault_token_path,
                recursive=True,
            )

            write_file(vault_token_path, token.encode('utf8'))

            set(f"{vault_token_path}/@_yqla_secret_category", b"yt")

            self._test_simple_query(
                "pragma yt.auth = 'custom_secret'; select a + 1 as b from primary.`//tmp/t`;",
                [{"b": 43}],
                secrets=[{"id": "custom_secret", "ypath": vault_token_path}],
            )

        run_query("allowed_user")

        with raises_yt_error('Access denied for user "denied_user"'):
            run_query("denied_user")

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_secret_with_discovered_category_from_name(self, query_tracker, yql_agent):
        def run_query(username):
            token, token_hash = issue_token(username)

            vault_token_path = f"//tmp/vault/{username}_token"
            create(
                "file", vault_token_path,
                recursive=True,
            )

            write_file(vault_token_path, token.encode('utf8'))

            self._test_simple_query(
                "pragma yt.auth = 'default_yt'; select a + 1 as b from primary.`//tmp/t`;",
                [{"b": 43}],
                secrets=[{"id": "default_yt", "ypath": vault_token_path}],
            )

        run_query("allowed_user")

        with raises_yt_error('Access denied for user "denied_user"'):
            run_query("denied_user")

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_bad_secret(self, query_tracker, yql_agent):
        def run_query(username):
            vault_token_path = f"//tmp/vault/{username}_token"
            create(
                "document", vault_token_path,
                recursive=True,
            )

            set(vault_token_path, 42)

            self._test_simple_query(
                "pragma yt.auth = 'custom_secret'; select a + 1 as b from primary.`//tmp/t`;",
                [{"b": 43}],
                secrets=[{"id": "custom_secret", "category": "yt", "ypath": vault_token_path}],
            )

        with raises_yt_error('Cannot convert secret value to string'):
            run_query("allowed_user")

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_secret_with_conflicting_discovered_category(self, query_tracker, yql_agent):
        def run_query(username):
            token, token_hash = issue_token(username)

            vault_token_path = f"//tmp/vault/{username}_token"
            create(
                "file", vault_token_path,
                recursive=True,
            )

            write_file(vault_token_path, token.encode('utf8'))

            set(f"{vault_token_path}/@_yqla_secret_category", b"yt")

            self._test_simple_query(
                "pragma yt.auth = 'custom_secret'; select a + 1 as b from primary.`//tmp/t`;",
                [{"b": 43}],
                secrets=[{"id": "custom_secret", "category": "non-yt", "ypath": vault_token_path}],
            )

        with raises_yt_error('Found mismatch between provided and discovered secret categories'):
            run_query("allowed_user")

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_secret_with_bad_discovered_category(self, query_tracker, yql_agent):
        def run_query(username):
            token, token_hash = issue_token(username)

            vault_token_path = f"//tmp/vault/{username}_token"
            create(
                "file", vault_token_path,
                recursive=True,
            )

            write_file(vault_token_path, token.encode('utf8'))

            set(f"{vault_token_path}/@_yqla_secret_category", 42)

            self._test_simple_query(
                "pragma yt.auth = 'custom_secret'; select a + 1 as b from primary.`//tmp/t`;",
                [{"b": 43}],
                secrets=[{"id": "custom_secret", "ypath": vault_token_path}],
            )

        with raises_yt_error('Cannot convert secret category to string'):
            run_query("allowed_user")

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_secret_with_unsupported_discovered_category(self, query_tracker, yql_agent):
        def run_query(username):
            token, token_hash = issue_token(username)

            vault_token_path = f"//tmp/vault/{username}_token"
            create(
                "file", vault_token_path,
                recursive=True,
            )

            write_file(vault_token_path, token.encode('utf8'))

            self._test_simple_query(
                "pragma yt.auth = 'default_non_supported_category'; select a + 1 as b from primary.`//tmp/t`;",
                [{"b": 43}],
                secrets=[{"id": "default_non_supported_category", "ypath": vault_token_path}],
            )

        with raises_yt_error('Mismatch credential category, expected: yt, but found: non_supported_category'):
            run_query("allowed_user")

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_secret_with_not_discovered_category(self, query_tracker, yql_agent):
        def run_query(username):
            token, token_hash = issue_token(username)

            vault_token_path = f"//tmp/vault/{username}_token"
            create(
                "file", vault_token_path,
                recursive=True,
            )

            write_file(vault_token_path, token.encode('utf8'))

            self._test_simple_query(
                "pragma yt.auth = 'custom_secret'; select a + 1 as b from primary.`//tmp/t`;",
                [{"b": 43}],
                secrets=[{"id": "custom_secret", "ypath": vault_token_path}],
            )

        with raises_yt_error('Mismatch credential category, expected: yt, but found: '):
            run_query("allowed_user")


class TestQueriesYqlWithSecretProtection(TestQueriesYqlAuthBase):
    INSECURE_GROUPS = ["everyone", "large_group"]

    def setup_method(self, method):
        super().setup_method(method)
        create_group("large_group")
        create_group("small_group")

    @classmethod
    def modify_yql_agent_config(cls, config):
        config['yql_agent']['insecure_secret_path_subjects'] = cls.INSECURE_GROUPS

    @authors("ngc224")
    @pytest.mark.timeout(180)
    def test_secret_protection(self, query_tracker, yql_agent):
        def run_query(username, vault, subjects):
            token, token_hash = issue_token(username)

            vault_path = f"//tmp/{vault}"

            create(
                "map_node", vault_path,
                attributes={
                    "acl": [
                        {
                            "action": "allow",
                            "subjects": subjects,
                            "permissions": ["read"],
                        },
                        {
                            "action": "allow",
                            "subjects": self.INSECURE_GROUPS,
                            "permissions": ["mount"],
                        },
                    ],
                    "inherit_acl": False,
                }
            )

            vault_token_path = f"{vault_path}/{username}_token"
            create(
                "document", vault_token_path,
                attributes={"value": token},
            )

            self._test_simple_query(
                "pragma yt.auth = 'custom_secret'; select a + 1 as b from primary.`//tmp/t`;",
                [{"b": 43}],
                secrets=[{"id": "custom_secret", "category": "yt", "ypath": vault_token_path}],
            )

        run_query("allowed_user", "secure_vault_allowed", ["allowed_user", "small_group"])

        with raises_yt_error("Found insecure subjects for provided secret"):
            run_query("allowed_user", "insecure_vault_allowed", ["everyone"])

        with raises_yt_error('Access denied for user "denied_user"'):
            run_query("denied_user", "secure_vault_denied", ["denied_user"])

        with raises_yt_error("Found insecure subjects for provided secret"):
            run_query("denied_user", "insecure_vault_denied", ["denied_user", "large_group"])


class TestYqlColumnOrderAggregateWithAs(TestQueriesYqlSimpleBase):
    @authors("gritukan", "mpereskokova")
    @pytest.mark.timeout(600)
    def test_aggregate_with_as(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        write_table("//tmp/t", [{"a": 42}])

        self._test_simple_query("""
            select
                sum(a) as c_a,
                sum(a) + 1 as c_b,
                sum(a) + 2 as c_c,
                sum(a) + 3 as c_d,
            from primary.`//tmp/t`
        """, [{"c_a": 42, "c_b": 43, "c_c": 44, "c_d": 45}])

        self._test_simple_query("""
            select
                sum(a) as c_d,
                sum(a) + 1 as c_c,
                sum(a) + 2 as c_b,
                sum(a) + 3 as c_a,
            from primary.`//tmp/t`
        """, [{"c_a": 45, "c_b": 44, "c_c": 43, "c_d": 42}])

        self._test_simple_query("""
            select
                sum(a) as c_d,
                sum(a) + 1 as c_b,
                sum(a) + 2 as c_c,
                sum(a) + 3 as c_a,
            from primary.`//tmp/t`
        """, [{"c_a": 45, "c_b": 43, "c_c": 44, "c_d": 42}])

        self._test_simple_query("""
            select
                sum(a) as c_c,
                sum(a) + 1 as c_a,
                sum(a) + 2 as c_d,
                sum(a) + 3 as c_b,
            from primary.`//tmp/t`
        """, [{"c_a": 43, "c_b": 45, "c_c": 42, "c_d": 44}])


class TestYqlColumnOrderIssue707(TestQueriesYqlSimpleBase):
    @authors("gritukan", "mpereskokova")
    @pytest.mark.timeout(600)
    def test_issue_707(self, query_tracker, yql_agent):
        # https://github.com/ytsaurus/ytsaurus/issues/707
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}, {"name": "c", "type": "float"}]
        })
        write_table("//tmp/t", [{"a": 42, "b": "foo", "c": 2.0}])

        self._test_simple_query("""
            select
                sum(a) as x,
                count(*) as y,
                sum(c) as z
            from primary.`//tmp/t`
        """, [{"x": 42, "y": 1, "z": 2.0}])
        self._test_simple_query("""
            select
                sum(a) as z,
                count(*) as x,
                sum(c) as y
            from primary.`//tmp/t`
        """, [{"x": 1, "y": 2.0, "z": 42}])


class TestYqlColumnOrderParametrize(TestQueriesYqlSimpleBase):
    @authors("gritukan", "mpereskokova")
    @pytest.mark.parametrize("dynamic", [False, True])
    @pytest.mark.timeout(300)
    def test_select_table(self, query_tracker, yql_agent, dynamic):
        create("table", "//tmp/t", attributes={
            "schema": [
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "float"},
            ],
            "dynamic": dynamic,
            "enable_dynamic_store_read": True,
        })
        if dynamic:
            sync_mount_table("//tmp/t")
            insert_rows("//tmp/t", [{"a": 42, "b": "foo", "c": 2.0}])
        else:
            write_table("//tmp/t", [{"a": 42, "b": "foo", "c": 2.0}])

        self._test_simple_query("""
            select *,
            from primary.`//tmp/t`
        """, [{"a": 42, "b": "foo", "c": 2.0}])
        self._test_simple_query("""
            select a, b, c,
            from primary.`//tmp/t`
        """, [{"a": 42, "b": "foo", "c": 2.0}])
        self._test_simple_query("""
            select c, a, b
            from primary.`//tmp/t`
        """, [{"a": 42, "b": "foo", "c": 2.0}])
        self._test_simple_query("""
            select a as b, b as c, c as a,
            from primary.`//tmp/t`
        """, [{"a": 2.0, "b": 42, "c": "foo"}])
        self._test_simple_query("""
            select c as b, b as a, a as c,
            from primary.`//tmp/t`
        """, [{"a": "foo", "b": 2.0, "c": 42}])

        # Multiple outputs
        self._test_simple_query("""
            select a, b, c
            from primary.`//tmp/t`;
            select a, b, c
            from primary.`//tmp/t`;
        """, [[{"a": 42, "b": "foo", "c": 2.0}], [{"a": 42, "b": "foo", "c": 2.0}]])
        self._test_simple_query("""
            select a, b, c
            from primary.`//tmp/t`;
            select c, a, b
            from primary.`//tmp/t`;
        """, [[{"a": 42, "b": "foo", "c": 2.0}], [{"a": 42, "b": "foo", "c": 2.0}]])


class TestYqlColumnOrderSelectScalars(TestQueriesYqlSimpleBase):
    @authors("gritukan", "mpereskokova")
    @pytest.mark.timeout(600)
    def test_select_scalars(self, query_tracker, yql_agent):
        self._test_simple_query("""
            select 42 as a, "foo" as b, 2.0 as c
        """, [{"a": 42, "b": "foo", "c": 2.0}])
        self._test_simple_query("""
            select 42 as c, 43 as a, 44 as b
        """, [{"a": 43, "b": 44, "c": 42}])
        self._test_simple_query("""
            select 42 as c, "foo" as a, 2.0 as b
        """, [{"a": "foo", "b": 2.0, "c": 42}])

        # Error cases
        with raises_yt_error("duplicate column names"):
            self._run_simple_query("select 42 as a, 42 as a")
        with raises_yt_error("duplicate column names"):
            self._run_simple_query("select 42 as a, 43 as a")
        with raises_yt_error("duplicate column names"):
            self._run_simple_query("select 42 as a, \"foo\" as a")

        # Multiple outputs
        self._test_simple_query("""
            select 42 as a, "foo" as b, 2.0 as c;
            select 42 as c, "foo" as a, 2.0 as b;
        """, [[{"a": 42, "b": "foo", "c": 2.0}], [{"a": "foo", "b": 2.0, "c": 42}]])


class TestYqlColumnOrderDifferentSources(TestQueriesYqlSimpleBase):
    @authors("gritukan", "mpereskokova")
    @pytest.mark.timeout(300)
    def test_different_sources(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}, {"name": "c", "type": "float"}],
            "dynamic": True,
            "enable_dynamic_store_read": True,
        })
        sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", [{"a": 42, "b": "foo", "c": 2.0}])

        create("table", "//tmp/t2", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}, {"name": "c", "type": "float"}],
            "enable_dynamic_store_read": True,
        })
        write_table("//tmp/t2", [{"a": 43, "b": "bar", "c": 3.0}])

        self._test_simple_query("""
            select a, b, c from primary.`//tmp/t1`
            union all
            select a, b, c from primary.`//tmp/t2`
        """, [{"a": 43, "b": "bar", "c": 3.0}, {"a": 42, "b": "foo", "c": 2.0}])
        self._test_simple_query("""
            select a, b, c from primary.`//tmp/t2`
            union all
            select a, b, c from primary.`//tmp/t1`
        """, [{"a": 42, "b": "foo", "c": 2.0}, {"a": 43, "b": "bar", "c": 3.0}])
        self._test_simple_query("""
            select c, a, b from primary.`//tmp/t2`
            union all
            select c, a, b from primary.`//tmp/t1`
        """, [{"a": 42, "b": "foo", "c": 2.0}, {"a": 43, "b": "bar", "c": 3.0}])

    @authors("a-romanov")
    @pytest.mark.timeout(300)
    def test_different_sources_with_limit(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}, {"name": "c", "type": "float"}],
            "dynamic": True,
            "enable_dynamic_store_read": True,
        })
        sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", [{"a": 42, "b": "foo", "c": 2.0}, {"a": 43, "b": "xyz", "c": 3.0}, {"a": 44, "b": "uvw", "c": 4.0}])

        create("table", "//tmp/t2", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}, {"name": "c", "type": "float"}],
            "enable_dynamic_store_read": True,
        })
        write_table("//tmp/t2", [{"a": 45, "b": "bar", "c": -3.0}, {"a": 46, "b": "abc", "c": -4.0}, {"a": 47, "b": "def", "c": -5.0}])

        self._test_simple_query("""
            select * from primary.`//tmp/t1`
            union all
            select * from primary.`//tmp/t2`
            limit 4
       """, [{"a": 42, "b": "foo", "c": 2.0}, {"a": 43, "b": "xyz", "c": 3.0}, {"a": 44, "b": "uvw", "c": 4.0}, {"a": 45, "b": "bar", "c": -3.0}])
        self._test_simple_query("""
            select * from primary.`//tmp/t2`
            union all
            select * from primary.`//tmp/t1`
            limit 4
        """, [{"a": 45, "b": "bar", "c": -3.0}, {"a": 46, "b": "abc", "c": -4.0}, {"a": 47, "b": "def", "c": -5.0}, {"a": 42, "b": "foo", "c": 2.0}])


class TestAssignedEngine(TestQueriesYqlSimpleBase):
    NUM_YQL_AGENTS = 2

    @authors("kirsiv40")
    def test_assigned_engine(self, query_tracker, yql_agent):
        yqla_instances = get("//sys/yql_agent/instances")

        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        create_pool("small", attributes={"resource_limits": {"user_slots": 0}})
        query = self.start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')

        wait(lambda: query.get_state() == "running")

        query_running_info = query.get()
        assert "annotations" in query_running_info
        assert "assigned_engine" in query_running_info["annotations"]

        set("//sys/pools/small/@resource_limits/user_slots", 1)
        query.track()

        query_finished_info = query.get()
        assert "annotations" in query_finished_info
        assert "assigned_engine" in query_finished_info["annotations"]

        assert query_running_info["annotations"]["assigned_engine"] == query_finished_info["annotations"]["assigned_engine"]
        assert query_finished_info["annotations"]["assigned_engine"] in yqla_instances


class TestAstReturns(TestQueriesYqlSimpleBase):
    @authors("kirsiv40")
    @pytest.mark.timeout(90)
    def test_ast_attribute_in_progress(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        create_pool("small", attributes={"resource_limits": {"user_slots": 0}})
        query = self.start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')

        wait(lambda: ("progress" in query.get() and "yql_ast" in query.get()["progress"]))

        query_running_info = query.get()

        runnug_query_ast_lines = query_running_info["progress"]["yql_ast"].split('\n')
        assert len(runnug_query_ast_lines) > 2
        for line in runnug_query_ast_lines:
            assert (re.fullmatch(r"([()]*)|(^\(.*\)$)|(^\(.*\(block '\($)", line.strip()))

        set("//sys/pools/small/@resource_limits/user_slots", 1)
        query.track()

        query_finished_info = query.get()
        assert "yql_ast" in query_finished_info["progress"]

        finished_query_ast_lines = query_finished_info["progress"]["yql_ast"].split('\n')
        assert len(finished_query_ast_lines) > 2
        for line in finished_query_ast_lines:
            assert (re.fullmatch(r"([()]*)|(^\(.*\)$)|(^\(.*\(block '\($)", line.strip()))


class TestYqlVersionChanges(TestQueriesYqlSimpleBase):
    NUM_YQL_AGENTS = 2

    @authors("kirsiv40")
    def test_yql_version_changes(self, query_tracker, yql_agent):
        settings = {"yql_version": "2025.01"}

        query_old_version = self.start_query("yql", "select CurrentLanguageVersion() as result;", settings=settings)
        query_old_version.track()
        assert_items_equal(query_old_version.read_result(0), [{"result": "2025.01"}])

        query_old_udfs = self.start_query("yql", "select String::Reverse(\"abc\") as result;", settings=settings)
        query_old_udfs.track()
        assert_items_equal(query_old_udfs.read_result(0), [{"result": "cba"}])

        settings["yql_version"] = "2025.02"
        query_new_version = self.start_query("yql", "select CurrentLanguageVersion() as result;", settings=settings)
        query_new_version.track()
        assert_items_equal(query_new_version.read_result(0), [{"result": "2025.02"}])

        with raises_yt_error() as err:
            query_new_udfs = self.start_query("yql", "select String::Reverse(\"abc\");", settings=settings)
            query_new_udfs.track()
        assert err[0].contains_text("'String.Reverse' is not available")

    @authors("kirsiv40")
    def test_yql_versions_throws(self, query_tracker, yql_agent):
        with raises_yt_error() as err:
            query = self.start_query("yql", "select CurrentLanguageVersion() as result;", settings={"yql_version": "2025.00"})
            query.track()
        assert err[0].contains_text("Invalid YQL language version")

        with raises_yt_error() as err:
            query = self.start_query("yql", "select CurrentLanguageVersion() as result;", settings={"yql_version": "not a valid version"})
            query.track()
        assert err[0].contains_text("Invalid YQL language version")

    @authors("kirsiv40")
    def test_default_yql_version(self, query_tracker, yql_agent):
        query = self.start_query("yql", "select CurrentLanguageVersion() as result;")
        query.track()
        result_version = query.read_result(0)[0]["result"]
        assert re.fullmatch(r'\d\d\d\d\.\d\d', result_version)
        assert result_version >= "2025.01"
        assert result_version <= "3000.00"


class TestMaxYqlVersionConfigAttr(TestQueriesYqlSimpleBase):
    MAX_YQL_VERSION = "2025.01"

    @authors("kirsiv40")
    def test_max_yql_version(self, query_tracker, yql_agent):
        query = self.start_query("yql", "select CurrentLanguageVersion() as result;", settings={"yql_version": "2025.01"})
        query.track()
        assert_items_equal(query.read_result(0), [{"result": "2025.01"}])

        with raises_yt_error() as err:
            query = self.start_query("yql", "select CurrentLanguageVersion() as result;", settings={"yql_version": "2025.02"})
            query.track()
        assert err[0].contains_text("is not available, maximum version is")

    @authors("kirsiv40")
    def test_yql_versions_throws(self, query_tracker, yql_agent):
        with raises_yt_error() as err:
            query = self.start_query("yql", "select CurrentLanguageVersion() as result;", settings={"yql_version": "2025.00"})
            query.track()
        assert err[0].contains_text("Invalid YQL language version")

        with raises_yt_error() as err:
            query = self.start_query("yql", "select CurrentLanguageVersion() as result;", settings={"yql_version": "not a valid version"})
            query.track()
        assert err[0].contains_text("Invalid YQL language version")

    @authors("kirsiv40")
    def test_default_yql_version(self, query_tracker, yql_agent):
        query = self.start_query("yql", "select CurrentLanguageVersion() as result;")
        query.track()
        assert_items_equal(query.read_result(0), [{"result": "2025.01"}])


class TestSimpleQueriesBase(TestQueriesYqlSimpleBase):
    def _test_simple_yql_query_versions(self, query_tracker, yql_agent):
        query = self.start_query("yql", "select CurrentLanguageVersion() as result;")
        query.track()
        result_version = query.read_result(0)[0]["result"]
        assert re.fullmatch(r'\d\d\d\d\.\d\d', result_version)
        assert result_version >= "2025.01"
        assert result_version <= "3000.00"

        query = self.start_query("yql", "select CurrentLanguageVersion() as result;", settings={"yql_version": "2025.03"})
        query.track()
        assert_items_equal(query.read_result(0), [{"result": "2025.03"}])


class TestAgentWithInvalidMaxYqlVersion(TestSimpleQueriesBase):
    MAX_YQL_VERSION = "some invalid version name. should be set to maximum availible in facade"

    @authors("kirsiv40")
    def test_default_yql_query(self, query_tracker, yql_agent):
        self._test_simple_yql_query_versions(query_tracker, yql_agent)


class TestAgentWithUndefinedMaxYqlVersion(TestSimpleQueriesBase):
    MAX_YQL_VERSION = "9999.99"

    @authors("kirsiv40")
    def test_default_yql_query(self, query_tracker, yql_agent):
        self._test_simple_yql_query_versions(query_tracker, yql_agent)


class TestGetQueryTrackerInfoBase(TestQueriesYqlSimpleBase):
    NUM_YQL_AGENTS = 2

    def _check_qt_info(self, qt_info):
        assert "engines_info" in qt_info
        assert "yql" in qt_info["engines_info"]
        yql_info = qt_info["engines_info"]["yql"]

        assert "available_yql_versions" in yql_info
        assert "default_yql_ui_version" in yql_info
        assert "supported_features" in yql_info

        yql_versions = yql_info["available_yql_versions"]
        default_version = yql_info["default_yql_ui_version"]

        assert len(yql_versions) != 0
        for version in yql_versions:
            assert re.fullmatch(r'\d\d\d\d\.\d\d', version)

        assert re.fullmatch(r'\d\d\d\d\.\d\d', default_version)

        assert default_version in yql_versions

    def _test_qt_info_with_incorrect_yqla_stage(self):
        qt_info = get_query_tracker_info(attributes=["engines_info"], settings={"yql_agent_stage": "unavailable yqla stage name"})
        assert qt_info["engines_info"] == {}

        qt_info = get_query_tracker_info(settings={"yql_agent_stage": "unavailable yqla stage name"})
        assert qt_info["engines_info"] == {}


class TestGetQueryTrackerInfoWithMaxYqlVersion(TestGetQueryTrackerInfoBase):
    MAX_YQL_VERSION = "2025.01"

    def _check_specific_qt_info(self, qt_info):
        self._check_qt_info(qt_info)
        assert qt_info["engines_info"]["yql"] == \
            {
                "available_yql_versions": ["2025.01",],
                "default_yql_ui_version": "2025.01",
                "supported_features": {"declare_params": True, "yql_runner": True},
            }

    @authors("kirsiv40")
    def test_qt_info(self, query_tracker, yql_agent):
        self._check_specific_qt_info(get_query_tracker_info())
        self._check_specific_qt_info(get_query_tracker_info(settings={"yql_agent_stage": "production"}))
        self._check_specific_qt_info(get_query_tracker_info(settings={"some_unused_settings": "some_unused_settings"}))
        self._check_specific_qt_info(get_query_tracker_info(attributes=["engines_info"]))
        self._check_specific_qt_info(get_query_tracker_info(attributes=["engines_info"], settings={"yql_agent_stage": "production"}))
        self._check_specific_qt_info(get_query_tracker_info(attributes=["engines_info"], settings={"some_unused_settings": "some_unused_settings"}))
        self._test_qt_info_with_incorrect_yqla_stage()


class TestGetQueryTrackerInfoWithoutMaxYqlVersion(TestGetQueryTrackerInfoBase):
    @authors("kirsiv40")
    def test_qt_info(self, query_tracker, yql_agent):
        self._check_qt_info(get_query_tracker_info())
        self._check_qt_info(get_query_tracker_info(settings={"yql_agent_stage": "production"}))
        self._check_qt_info(get_query_tracker_info(settings={"some_unused_settings": "some_unused_settings"}))
        self._check_qt_info(get_query_tracker_info(attributes=["engines_info"]))
        self._check_qt_info(get_query_tracker_info(attributes=["engines_info"], settings={"yql_agent_stage": "production"}))
        self._check_qt_info(get_query_tracker_info(attributes=["engines_info"], settings={"some_unused_settings": "some_unused_settings"}))
        self._test_qt_info_with_incorrect_yqla_stage()


class TestGetQueryTrackerInfoWithInvalidMaxYqlVersion(TestGetQueryTrackerInfoBase):
    MAX_YQL_VERSION = "some invalid version name. should be set to maximum availible in facade"

    @authors("kirsiv40")
    def test_qt_info(self, query_tracker, yql_agent):
        self._check_qt_info(get_query_tracker_info())
        self._check_qt_info(get_query_tracker_info(settings={"yql_agent_stage": "production"}))
        self._check_qt_info(get_query_tracker_info(settings={"some_unused_settings": "some_unused_settings"}))
        self._check_qt_info(get_query_tracker_info(attributes=["engines_info"]))
        self._check_qt_info(get_query_tracker_info(attributes=["engines_info"], settings={"yql_agent_stage": "production"}))
        self._check_qt_info(get_query_tracker_info(attributes=["engines_info"], settings={"some_unused_settings": "some_unused_settings"}))
        self._test_qt_info_with_incorrect_yqla_stage()


class TestGetQueryTrackerInfoWithVisibleYqlVersionBase(TestGetQueryTrackerInfoBase):
    _ALL_YQL_VERSIONS = ["2025.01", "2025.02", "2025.03", "2025.04", "2025.05"]
    _RELEASED_YQL_VERSIONS = ["2025.01", "2025.02", "2025.03", "2025.04"]

    def _check_specific_qt_info(self, qt_info, all_versions):
        self._check_qt_info(qt_info)
        assert qt_info["engines_info"]["yql"] == \
            {
                "available_yql_versions": self._ALL_YQL_VERSIONS if all_versions else self._RELEASED_YQL_VERSIONS,
                "default_yql_ui_version": "2025.03",
                "supported_features": {"declare_params": True, "yql_runner": True},
            }

    def _test_visible_versions(self, all_versions):
        self._check_specific_qt_info(get_query_tracker_info(), all_versions)
        self._check_specific_qt_info(get_query_tracker_info(settings={"yql_agent_stage": "production"}), all_versions)
        self._check_specific_qt_info(get_query_tracker_info(settings={"some_unused_settings": "some_unused_settings"}), all_versions)
        self._check_specific_qt_info(get_query_tracker_info(attributes=["engines_info"]), all_versions)
        self._check_specific_qt_info(get_query_tracker_info(attributes=["engines_info"], settings={"yql_agent_stage": "production"}), all_versions)
        self._check_specific_qt_info(get_query_tracker_info(attributes=["engines_info"], settings={"some_unused_settings": "some_unused_settings"}), all_versions)
        self._test_qt_info_with_incorrect_yqla_stage()


class TestGetQueryTrackerInfoWithVisibleYqlVersionStatic(TestGetQueryTrackerInfoWithVisibleYqlVersionBase):
    DEFAULT_YQL_UI_VERSION = "2025.03"

    @authors("lucius")
    def test_visible_versions(self, query_tracker, yql_agent):
        self._test_visible_versions(all_versions=True)


class TestGetQueryTrackerInfoWithVisibleYqlVersionDynamic(TestGetQueryTrackerInfoWithVisibleYqlVersionBase):
    YQL_AGENT_DYNAMIC_CONFIG = {
        "default_yql_ui_version": "2025.03",
        "allow_not_released_yql_versions": False,
    }

    @authors("lucius")
    def test_visible_versions(self, query_tracker, yql_agent):
        self._test_visible_versions(all_versions=False)


class TestGetQueryTrackerInfoWithVisibleYqlVersionBoth(TestGetQueryTrackerInfoWithVisibleYqlVersionBase):
    DEFAULT_YQL_UI_VERSION = "2025.01"
    ALLOW_NOT_RELEASED_YQL_VERSIONS = True
    YQL_AGENT_DYNAMIC_CONFIG = {
        "default_yql_ui_version": "2025.03",
        "allow_not_released_yql_versions": False,
    }

    @authors("lucius")
    def test_visible_versions(self, query_tracker, yql_agent):
        self._test_visible_versions(all_versions=False)


class TestGetQueryTrackerInfoWithVisibleYqlVersionBothNotReleased(TestGetQueryTrackerInfoWithVisibleYqlVersionBase):
    DEFAULT_YQL_UI_VERSION = "2025.01"
    ALLOW_NOT_RELEASED_YQL_VERSIONS = False
    YQL_AGENT_DYNAMIC_CONFIG = {
        "default_yql_ui_version": "2025.03",
        "allow_not_released_yql_versions": True,
    }

    @authors("lucius")
    def test_visible_versions(self, query_tracker, yql_agent):
        self._test_visible_versions(all_versions=True)


class TestDeclare(TestQueriesYqlBase):
    @authors("kirsiv40")
    def test_declare(self, query_tracker, yql_agent):
        assert get_query_declared_parameters_info("select 1;", "yql") == {}

        query_text = "DECLARE $VAR as string;\nselect 1;\nselect $VAR as result"
        assert get_query_declared_parameters_info(query_text, "yql") == {'$VAR': 'String'}

        query = self.start_query("yql", query_text, settings={"declared_parameters": "{\"$VAR\"={\"Data\"=\"test-string\"}}"})
        query.track()
        assert query.read_result(1)[0]["result"] == "test-string"

        query = self.start_query("yql", query_text, settings={"declared_parameters": "{\"$VAR\"={\"Data\"=\"test-string\"};\"$some_unused_var\"={\"Data\"=\"111\"}}"})
        query.track()
        assert query.read_result(1)[0]["result"] == "test-string"

        # a syntactially correct query can still be parsed even if it can't be executed
        query_text = "DECLARE $VAR as Json;\nselect 1;\nselect $VAR as result;select FileContent(\"some-file\");"
        assert get_query_declared_parameters_info(query_text, "yql") == {'$VAR': 'Json'}


##################################################################


@authors("kirsiv40")
@pytest.mark.enabled_multidaemon
class TestGetQueryTrackerInfoWithMaxYqlVersionRpcProxy(TestGetQueryTrackerInfoWithMaxYqlVersion):
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1

    DRIVER_BACKEND = "rpc"
    ENABLE_MULTIDAEMON = True


@authors("kirsiv40")
@pytest.mark.enabled_multidaemon
class TestGetQueryTrackerInfoWithoutMaxYqlVersionRpcProxy(TestGetQueryTrackerInfoWithoutMaxYqlVersion):
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1

    DRIVER_BACKEND = "rpc"
    ENABLE_MULTIDAEMON = True


@authors("kirsiv40")
@pytest.mark.enabled_multidaemon
class TestGetQueryTrackerInfoWithInvalidMaxYqlVersionRpcProxy(TestGetQueryTrackerInfoWithInvalidMaxYqlVersion):
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1

    DRIVER_BACKEND = "rpc"
    ENABLE_MULTIDAEMON = True


@authors("lucius")
@pytest.mark.enabled_multidaemon
class TestGetQueryTrackerInfoWithVisibleYqlVersionStaticRpcProxy(TestGetQueryTrackerInfoWithVisibleYqlVersionStatic):
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1

    DRIVER_BACKEND = "rpc"
    ENABLE_MULTIDAEMON = True


@authors("lucius")
@pytest.mark.enabled_multidaemon
class TestGetQueryTrackerInfoWithVisibleYqlVersionDynamicRpcProxy(TestGetQueryTrackerInfoWithVisibleYqlVersionDynamic):
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1

    DRIVER_BACKEND = "rpc"
    ENABLE_MULTIDAEMON = True


@authors("lucius")
@pytest.mark.enabled_multidaemon
class TestGetQueryTrackerInfoWithVisibleYqlVersionBothRpcProxy(TestGetQueryTrackerInfoWithVisibleYqlVersionBoth):
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1

    DRIVER_BACKEND = "rpc"
    ENABLE_MULTIDAEMON = True


@authors("kirsiv40")
@pytest.mark.enabled_multidaemon
class TestDeclareRpcProxy(TestDeclare):
    DRIVER_BACKEND = "rpc"
    ENABLE_MULTIDAEMON = True


@authors("staketd")
class TestYqlAgentWithProcesses(TestYqlAgent):
    YQL_SUBPROCESSES_COUNT = 8


@authors("staketd")
class TestYqlAgentDynConfigWithProcesses(TestYqlAgentDynConfig):
    YQL_SUBPROCESSES_COUNT = 8


@authors("staketd")
class TestMaxYqlVersionConfigAttrWithProcesses(TestMaxYqlVersionConfigAttr):
    YQL_SUBPROCESSES_COUNT = 8


@authors("a-romanov")
class TestsDDL(TestQueriesYqlSimpleBase):
    NUM_TEST_PARTITIONS = 3

    def test_simple_create_table(self, query_tracker, yql_agent):
        self._test_simple_query("create table `//tmp/t1` (xyz Text, abc Int32 not null, uvw Date null);", None)
        self._test_simple_query("$p = process `//tmp/t1`; select FormatType(TypeOf($p)) as type;", [{'type': "List<Struct<'abc':Int32,'uvw':Date?,'xyz':Utf8?>>"}])

    def test_error_already_exists(self, query_tracker, yql_agent):
        self._test_simple_query("create table `//tmp/t2` (xyz Text);", None)
        self._test_simple_query_error("create table `//tmp/t2` (xyz Text);", "already exists.")

    def test_error_double_create(self, query_tracker, yql_agent):
        self._test_simple_query_error("""
            create table `//tmp/t0` (xyz Text);
            create table `//tmp/t0` (xyz Text);
        """, "already exists.")

    def test_error_write_and_create(self, query_tracker, yql_agent):
        self._test_simple_query_error("""
            insert into `//tmp/t0` (xyz) values ("one"u),("two"u);
            create table `//tmp/t0` (xyz Text);
        """, "already exists.")

    def test_error_create_and_drop(self, query_tracker, yql_agent):
        self._test_simple_query_error("""
            create table `//tmp/ttt` (xyz Text);
            drop table `//tmp/ttt`;
        """, "is modified and dropped in the same transaction")

    def test_create_commit_drop(self, query_tracker, yql_agent):
        self._test_simple_query("""
            create table `//tmp/t3` (xyz Text);
            commit;
            drop table `//tmp/t3`;
        """, None)

    def test_create_commit_read(self, query_tracker, yql_agent):
        self._test_simple_query("""
            create table `//tmp/t4` (xyz Text);
            commit;
            select * from `//tmp/t4`;
        """, [])

    def test_create_and_write(self, query_tracker, yql_agent):
        self._test_simple_query("""
            create table `//tmp/t7` (xyz Text);
            insert into `//tmp/t7` (xyz) values ("one"u),("two"u);
        """, None)
        self._test_simple_query("select * from `//tmp/t7`", [{'xyz': "one"}, {'xyz': "two"}])

    def test_create_table_with_order_by(self, query_tracker, yql_agent):
        self._test_simple_query("""
            create table `//tmp/t5` (
                key Text,
                subkey Int32,
                value Date,
                order by(key, subkey)
            );
        """, None)
        self._test_simple_query("""
            $p = process `//tmp/t5`;
            select FormatType(ListItemType(TypeOf($p))) as type, YQL::ConstraintsOf($p) as constraints;
        """, [{'type': "Struct<'key':Utf8?,'subkey':Int32?,'value':Date?>", 'constraints': """{
  "Empty":true,
  "Sorted":
    [
      [
        "key",
        true
      ],
      [
        "subkey",
        true
      ]
    ]
}"""
               }])

    def test_create_table_with_order_by_descending(self, query_tracker, yql_agent):
        self._test_simple_query_error("""
            create table `//tmp/t6` (
                key Text,
                subkey Int32,
                value Date,
                order by(key asc, subkey desc)
            );
        """, "is only supported when YT's native descending sort is enabled")
        self._test_simple_query("""
            pragma yt.UseNativeDescSort;
            create table `//tmp/t6` (
                key Text,
                subkey Int32,
                value Date,
                order by(key asc, subkey desc)
            );
            commit;
            $p = process `//tmp/t6`;
            select FormatType(ListItemType(TypeOf($p))) as type, YQL::ConstraintsOf($p) as constraints;
        """, [{'type': "Struct<'key':Utf8?,'subkey':Int32?,'value':Date?>", 'constraints': """{
  "Empty":true,
  "Sorted":
    [
      [
        "key",
        true
      ],
      [
        "subkey",
        false
      ]
    ]
}"""
               }])

    def test_simple_create_view(self, query_tracker, yql_agent):
        settings = {"yql_version": "2025.05"}
        self._test_simple_query("create table `//tmp/t` (xyz Text not null);", None, settings=settings)
        self._test_simple_query("create view `//tmp/v` as do begin select cast(xyz as Float) as num from `//tmp/t` end do;", None, settings=settings)
        self._test_simple_query("$p = process `//tmp/v`; select FormatType(ListItemType(TypeOf($p))) as type;", [{'type': "Struct<'num':Float?>"}], settings=settings)
        self._test_simple_query_error("create view `//tmp/v` as do begin select cast(xyz as Float) as num from `//tmp/t` end do;", "already exists.", settings=settings)

    def test_drop_view(self, query_tracker, yql_agent):
        settings = {"yql_version": "2025.05"}
        self._test_simple_query("create view `//tmp/v0` as do begin select * from as_table([<|uvw:31.14|>]) end do;", None, settings=settings)
        self._test_simple_query("drop view `//tmp/v0`;", None, settings=settings)
        self._test_simple_query_error("drop view `//tmp/v0`;", "does not exists.", settings=settings)

    def test_wrong_drop(self, query_tracker, yql_agent):
        settings = {"yql_version": "2025.05"}

        self._test_simple_query("create view `//tmp/v1` as do begin select * from as_table([<|uvw:31.14|>]) end do;", None, settings=settings)
        self._test_simple_query_error("drop table `//tmp/v1`;", 'Drop of "tmp/v1" view can not be done via DROP TABLE statement.', settings=settings)

        self._test_simple_query("create table `//tmp/t1` (xyz Text not null);", None, settings=settings)
        self._test_simple_query_error("drop view `//tmp/t1`;", 'Drop of "tmp/t1" table can not be done via DROP VIEW statement.', settings=settings)

    def test_create_drop_table_by_exists(self, query_tracker, yql_agent):
        self._test_simple_query("drop table if exists `//tmp/tt00`;", None)
        self._test_simple_query("create table if not exists `//tmp/tt00` (xyz Text, abc Int16 not null, uvw Date null);", None)
        self._test_simple_query("create table if not exists `//tmp/tt00` (abc Text, def Float not null, klm Date null);", None)
        self._test_simple_query("$p = process `//tmp/tt00`; select FormatType(ListItemType(TypeOf($p))) as type;", [{'type': "Struct<'abc':Int16,'uvw':Date?,'xyz':Utf8?>"}])
        self._test_simple_query("drop table if exists `//tmp/tt00`;", None)
        self._test_simple_query("drop table if exists `//tmp/tt00`;", None)
        self._test_simple_query_error("select * from `//tmp/tt00`;", "does not exist")

    def test_alter_add_columns(self, query_tracker, yql_agent):
        settings = {"yql_version": "2025.05"}
        self._test_simple_query_error("alter table `//tmp/t7` add column one Datetime, add column two Json null;", "does not exists.", settings=settings)
        self._test_simple_query("insert into `//tmp/t7` select 'text'u as xyz;", None)
        self._test_simple_query("alter table `//tmp/t7` add column one Datetime, add column two Json null;", None, settings=settings)
        self._test_simple_query("$p = process `//tmp/t7`; select FormatType(ListItemType(TypeOf($p))) as type;", [{'type': "Struct<'one':Datetime?,'two':Json?,'xyz':Utf8>"}])
        self._test_simple_query_error("alter table `//tmp/t7` add column bad Float not null;", "Additional contrains for columns isn't supported.", settings=settings)

    def test_alter_add_column_and_insert(self, query_tracker, yql_agent):
        settings = {"yql_version": "2025.05"}
        self._test_simple_query("insert into `//tmp/t8` select 'text'u as xyz;", None)
        self._test_simple_query("""
            alter table `//tmp/t8` add column extra Float;
            insert into `//tmp/t8` select 'plus'u as xyz, 3.f as extra;
        """, None, settings=settings)
        self._test_simple_query("select * from `//tmp/t8`", [{'extra': None, 'xyz': 'text'}, {'extra': 3.0, 'xyz': 'plus'}])

    def test_insert_and_alter_wrong_sequence(self, query_tracker, yql_agent):
        settings = {"yql_version": "2025.05"}
        self._test_simple_query("insert into `//tmp/t9` select 'jump'u as xyz;", None)
        self._test_simple_query_error("""
            insert into `//tmp/t9` select 'plus'u as xyz;
            alter table `//tmp/t9` add column extra Float;
        """, "table content after another table modifications in the same transaction", settings=settings)


@authors("mpereskokova")
class TestStackOverflowWithProcesses(TestStackOverflow):
    YQL_SUBPROCESSES_COUNT = 8
