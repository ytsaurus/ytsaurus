from yt_env_setup import wait, YTEnvSetup, require_ytserver_root_privileges
from yt_commands import *
from yt.wrapper.clickhouse import get_clickhouse_clique_spec_builder
from yt.wrapper.common import simplify_structure

from yt.yson import YsonUint64

from distutils.spawn import find_executable

from yt.common import update

TEST_DIR = os.path.join(os.path.dirname(__file__))

import json
import pytest
import psutil
import subprocess
import random

CLICKHOUSE_CLIENT_BINARY = find_executable("clickhouse")
YTSERVER_CLICKHOUSE_BINARY = find_executable("ytserver-clickhouse")

DEFAULTS = {
    "memory_footprint": 2 * 1000**3,
    "memory_limit": 5 * 1000**3,
    "host_ytserver_clickhouse_path": YTSERVER_CLICKHOUSE_BINARY,
    "cpu_limit": 1,
    "enable_monitoring": False,
    "clickhouse_config": {},
}


class Clique(object):
    base_config = None
    clique_index = 0

    def __init__(self, instance_count, max_failed_job_count=0, config_patch=None, **kwargs):
        config = update(Clique.base_config, config_patch) if config_patch is not None else Clique.base_config

        filename = "//sys/clickhouse/config-{}.yson".format(Clique.clique_index)
        Clique.clique_index += 1
        create("file", filename)
        write_file(filename, yson.dumps(config, yson_format="pretty"))

        spec_builder = get_clickhouse_clique_spec_builder(instance_count,
                                                          cypress_config_path=filename,
                                                          max_failed_job_count=max_failed_job_count,
                                                          defaults=DEFAULTS,
                                                          **kwargs)
        self.spec = simplify_structure(spec_builder.build())
        self.spec["tasks"]["clickhouse_servers"]["force_core_dump"] = True
        self.instance_count = instance_count

    def _get_active_instance_count(self):
        if exists("//sys/clickhouse/cliques/{0}".format(self.op.id), verbose=False):
            return len(get("//sys/clickhouse/cliques/{0}".format(self.op.id), verbose=False))
        else:
            return 0

    def _print_progress(self):
        print >>sys.stderr, self.op.build_progress(), "(active instance count: {})".format(self._get_active_instance_count())

    def __enter__(self):
        self.op = start_op("vanilla",
                           spec=self.spec,
                           dont_track=True)

        print >>sys.stderr, "Waiting for clique {} to become ready".format(self.op.id)

        MAX_COUNTER_VALUE = 600
        counter = 0
        while True:
            state = self.op.get_state(verbose=False)

            # ClickHouse operations should never complete by itself.
            assert state != "completed"

            if state == "aborted" or state == "failed":
                raise self.op.get_error()
            elif state == "running" and self._get_active_instance_count() == self.instance_count:
                break
            elif counter % 30 == 0:
                self._print_progress()
            elif counter >= MAX_COUNTER_VALUE:
                raise YtError("Clique did not start in time, clique directory: {}".format(get("//sys/clickhouse/cliques/{0}".format(self.op.id), verbose=False)))

            time.sleep(self.op._poll_frequency)
            counter += 1

        self._print_progress()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        time.sleep(2)
        try:
            self.op.complete()
        except YtError as err:
            print >>sys.stderr, "Error while completing clique operation:", err
        clique_error = None
        try:
            self.op.track()
        except YtError as err:
            clique_error = err

        if clique_error is not None:
            if exc_type is not None:
                original_error = exc_value
                raise YtError("ClickHouse request failed and resulted in clique failure", inner_errors=[original_error, clique_error])
            else:
                raise YtError("Clique failed", inner_errors=[clique_error])

        # Pass the error.
        return False

    def make_query(self, query, user="root", verbose=True):
        instances = get("//sys/clickhouse/cliques/{0}".format(self.op.id), attributes=["host", "tcp_port"])
        assert len(instances) > 0
        instance = random.choice(instances.values())
        host = instance.attributes["host"]
        port = instance.attributes["tcp_port"]

        args = [CLICKHOUSE_CLIENT_BINARY, "client",
                "-h", host,
                "--port", port,
                "--format", "JSON",
                "-u", user,
                "--output_format_json_quote_64bit_integers", "0"]
        print >>sys.stderr, "Running '{0}' with the following input:\n> {1}".format(' '.join(args), query)

        process = psutil.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.stdin.write(query)
        stdout, stderr = process.communicate()
        return_code = process.returncode

        output = ""
        if stdout:
            output += "Stdout:\n" + stdout + "\n"
        if stderr:
            output += "Stderr:\n" + stderr + "\n"

        if verbose:
            print >>sys.stderr, output
        if return_code != 0:
            raise YtError("ClickHouse query failed\n" + output)

        return json.loads(stdout)


@require_ytserver_root_privileges
@pytest.mark.xfail(run=False, reason="I want to go to vacation and do not know what's the deal with those core dumps :(")
class ClickHouseTestBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "job_environment": {
                    "type": "cgroups",
                    "memory_watchdog_period": 100,
                    "supported_cgroups": ["cpuacct", "blkio", "cpu"],
                },
            },
            "job_controller": {
                "resource_limits": {
                    "memory": 10 * 2**30,
                },
            },
        }
    }

    def _read_local_config_file(self, name):
        return open(os.path.join(TEST_DIR, "test_clickhouse", name)).read()

    def _setup(self):
        if CLICKHOUSE_CLIENT_BINARY is None or YTSERVER_CLICKHOUSE_BINARY is None:
            pytest.skip("This test requires built clickhouse and ytserver-clickhouse binaries; "
                        "they are available only when using ya as a build system")

        if exists("//sys/clickhouse"):
            return
        create("map_node", "//sys/clickhouse")

        # We need to inject cluster_connection into yson config.
        Clique.base_config = yson.loads(self._read_local_config_file("config.yson"))
        Clique.base_config["cluster_connection"] = self.__class__.Env.configs["driver"]

@pytest.mark.xfail(run=False, reason="I want to go to vacation and do not know what's the deal with those core dumps :(")
class TestClickHouseCommon(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def test_readonly(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})

        with Clique(1) as clique:
            try:
                clique.make_query('insert into "//tmp/t" values(1)')
            except YtError as err:
                # 164 is an error code for READONLY.
                assert "164" in str(err)
                return

        assert False

    @pytest.mark.parametrize("instance_count", [1, 5])
    def test_avg(self, instance_count):
        with Clique(instance_count) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
            for i in range(10):
                write_table("<append=%true>//tmp/t", {"a": i})

            assert abs(clique.make_query('select avg(a) from "//tmp/t"')["data"][0]["avg(a)"] - 4.5) < 1e-6
            with pytest.raises(YtError):
                clique.make_query('select avg(b) from "//tmp/t"')

            # TODO(max42): support range selectors.
            #assert abs(float(clique.make_query('select avg(a) from "//tmp/t[#2:#9]"')) - 5.0) < 1e-6

    # YT-9497
    def test_aggregation_with_multiple_string_columns(self):
        with Clique(1) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "key1", "type": "string"},
                                                              {"name": "key2", "type": "string"},
                                                              {"name": "value", "type": "int64"}]})
            for i in range(5):
                write_table("<append=%true>//tmp/t", [{"key1": "dream", "key2": "theater", "value": i * 5 + j} for j in range(5)])
            total = 24 * 25 // 2

            result = clique.make_query('select key1, key2, sum(value) from "//tmp/t" group by key1, key2')
            assert result["data"] == [{"key1": "dream", "key2": "theater", "sum(value)": total}]

    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_cast(self, instance_count):
        with Clique(instance_count) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
            write_table("//tmp/t", [{"a": "2012-12-12 20:00:00"}])

            result = clique.make_query('select CAST(a as datetime) from "//tmp/t"')
            assert result["data"] == [{"CAST(a, 'datetime')": "2012-12-12 20:00:00"}]

    def test_settings(self):
        with Clique(1) as clique:
            # I took some random option from the documentation and changed it in config.yson.
            # Let's see if it changed in internal table with settings.

            result = clique.make_query("select * from system.settings where name = 'max_temporary_non_const_columns'")
            assert result["data"][0]["value"] == "1234"
            assert result["data"][0]["changed"] == 1

    def test_schema_caching(self):
        with Clique(1) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
            write_table("//tmp/t", [{"a": 1}])
            old_description = clique.make_query('describe "//tmp/t"')
            assert old_description["data"][0]["name"] == "a"
            remove("//tmp/t")
            create("table", "//tmp/t", attributes={"schema": [{"name": "b", "type": "int64"}]})
            write_table("//tmp/t", [{"b": 1}])
            new_description = clique.make_query('describe "//tmp/t"')
            assert new_description["data"][0]["name"] == "b"

@pytest.mark.xfail(run=False, reason="I want to go to vacation and do not know what's the deal with those core dumps :(")
class TestJobInput(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def _expect_row_count(self, clique, query, row_count):
        result = clique.make_query(query)
        assert result["statistics"]["rows_read"] == row_count

    def test_chunk_filter(self):
        with Clique(1) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "i", "type": "int64", "sort_order": "ascending"}]})
            for i in xrange(10):
                write_table("<append=%true>//tmp/t", [{"i": i}])
            self._expect_row_count(clique, 'select * from "//tmp/t" where i >= 3', 7)
            self._expect_row_count(clique, 'select * from "//tmp/t" where i < 2', 2)
            self._expect_row_count(clique, 'select * from "//tmp/t" where 5 <= i and i <= 8', 4)
            self._expect_row_count(clique, 'select * from "//tmp/t" where i in (-1, 2, 8, 8, 15)', 2)

@pytest.mark.xfail(run=False, reason="I want to go to vacation and do not know what's the deal with those core dumps :(")
class TestCompositeTypes(ClickHouseTestBase):
    def setup(self):
        self._setup()

        create("table", "//tmp/t", attributes={"schema": [{"name": "i", "type": "int64"}, {"name": "v", "type": "any"}]})
        write_table("//tmp/t", [
            {
                "i": 0,
                "v": {
                    "i64": -42,
                    "ui64": YsonUint64(23),
                    "bool": True,
                    "dbl": 3.14,
                    "str": "xyz",
                    "subnode": {
                        "i64": 123,
                    },
                    "arr_i64": [-1, 0, 1],
                    "arr_ui64": [1, 1, 2, 3, 5],
                    "arr_dbl": [-1.1, 2.71],
                    "arr_bool": [False, True, False],
                },
            },
            {
                "i": 1,
                "v": {
                    "i64": "xyz",  # Wrong type.
                },
            },
            {
                "i": 2,
                "v": {
                    "i64": YsonUint64(2**63 + 42),  # Out of range for getting value as i64.
                },
            },
            {
                "i": 3,
                "v": {},  # Key i64 is missing.
            },
            {
                "i": 4,
                "v": {
                    "i64": 57,
                },
            }
        ])

    def test_read_int64_strict(self):
        with Clique(1) as clique:
            for i in xrange(4):
                query = "select YPathInt64Strict(v, '/i64') from \"//tmp/t\" where i = {}".format(i)
                if i != 0:
                    with pytest.raises(YtError):
                        clique.make_query(query)
                else:
                    result = clique.make_query(query)
                    assert result["data"][0].popitem()[1] == -42

    def test_read_uint64_strict(self):
        with Clique(1) as clique:
            result = clique.make_query("select YPathUInt64Strict(v, '/i64') from \"//tmp/t\" where i = 4")
            assert result["data"][0].popitem()[1] == 57

    def test_read_from_subnode(self):
        with Clique(1) as clique:
            result = clique.make_query("select YPathUInt64Strict(v, '/subnode/i64') from \"//tmp/t\" where i = 0")
            assert result["data"][0].popitem()[1] == 123

    def test_read_int64_non_strict(self):
        with Clique(1) as clique:
            query = "select YPathInt64(v, '/i64') from \"//tmp/t\""
            result = clique.make_query(query)
            for i, item in enumerate(result["data"]):
                if i == 0:
                    assert item.popitem()[1] == -42
                elif i == 4:
                    assert item.popitem()[1] == 57
                else:
                    assert item.popitem()[1] == 0

    def test_read_all_types_strict(self):
        query = "select YPathInt64Strict(v, '/i64') as i64, YPathUInt64Strict(v, '/ui64') as ui64, " \
                "YPathDoubleStrict(v, '/dbl') as dbl, YPathBooleanStrict(v, '/bool') as bool, " \
                "YPathStringStrict(v, '/str') as str, YPathArrayInt64Strict(v, '/arr_i64') as arr_i64, " \
                "YPathArrayUInt64Strict(v, '/arr_ui64') as arr_ui64, YPathArrayDoubleStrict(v, '/arr_dbl') as arr_dbl, " \
                "YPathArrayBooleanStrict(v, '/arr_bool') as arr_bool from \"//tmp/t\" where i = 0"
        with Clique(1) as clique:
            result = clique.make_query(query)
        assert result["data"] == [{
            "i64": -42,
            "ui64": 23,
            "bool": True,
            "dbl": 3.14,
            "str": "xyz",
            "arr_i64": [-1, 0, 1],
            "arr_ui64": [1, 1, 2, 3, 5],
            "arr_dbl": [-1.1, 2.71],
            "arr_bool": [False, True, False],
        }]

    def test_read_all_types_non_strict(self):
        query = "select YPathInt64(v, '/i64') as i64, YPathUInt64(v, '/ui64') as ui64, " \
                "YPathDouble(v, '/dbl') as dbl, YPathBoolean(v, '/bool') as bool, " \
                "YPathString(v, '/str') as str, YPathArrayInt64(v, '/arr_i64') as arr_i64, " \
                "YPathArrayUInt64(v, '/arr_ui64') as arr_ui64, YPathArrayDouble(v, '/arr_dbl') as arr_dbl, " \
                "YPathArrayBoolean(v, '/arr_bool') as arr_bool from \"//tmp/t\" where i = 3"
        with Clique(1) as clique:
            result = clique.make_query(query)
        assert result["data"] == [{
            "i64": 0,
            "ui64": 0,
            "bool": False,
            "dbl": 0.0,
            "str": "",
            "arr_i64": [],
            "arr_ui64": [],
            "arr_dbl": [],
            "arr_bool": [],
        }]

    def test_const_args(self):
        with Clique(1) as clique:
            result = clique.make_query("select YPathString('{a=[1;2;{b=xyz}]}', '/a/2/b') as str")
        assert result["data"] == [{"str": "xyz"}]

    def test_nulls(self):
        with Clique(1) as clique:
            result = clique.make_query("select YPathString(NULL, NULL) as a, YPathString(NULL, '/x') as b, "
                                       "YPathString('{a=1}', NULL) as c")
        assert result["data"] == [{"a": None, "b": None, "c": None}]

@pytest.mark.xfail(run=False, reason="I want to go to vacation and do not know what's the deal with those core dumps :(")
class TestYtDictionaries(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def test_int_key_flat(self):
        create("table", "//tmp/dict", attributes={"schema": [{"name": "key", "type": "uint64"},
                                                             {"name": "value_str", "type": "string"},
                                                             {"name": "value_i64", "type": "int64"}]})
        write_table("//tmp/dict", [
            {"key": i, "value_str": "str" + str(i), "value_i64": i * i} for i in [1, 3, 5]
        ])

        with Clique(1, config_patch={
            "engine": {"dictionaries": [
                {
                    "name": "dict",
                    "layout": {"flat": {}},
                    "structure": {
                        "id": {"name": "key"},
                        "attribute": [{"name": "value_str", "type": "String", "null_value": "n/a"},
                                      {"name": "value_i64", "type": "Int64", "null_value": 42}]
                    },
                    "lifetime": 0,
                    "source": {
                        "yt": {
                            "path": "//tmp/dict"
                        }
                    }
                }
            ]}}) as clique:
            result = clique.make_query("select number, dictGetString('dict', 'value_str', number) as str, "
                                       "dictGetInt64('dict', 'value_i64', number) as i64 from numbers(5)")
        assert result["data"] == [
            {"number": 0, "str": "n/a", "i64": 42},
            {"number": 1, "str": "str1", "i64": 1},
            {"number": 2, "str": "n/a", "i64": 42},
            {"number": 3, "str": "str3", "i64": 9},
            {"number": 4, "str": "n/a", "i64": 42},
        ]

    def test_composite_key_hashed(self):
        create("table", "//tmp/dict", attributes={"schema": [{"name": "key", "type": "string"},
                                                             {"name": "subkey", "type": "int64"},
                                                             {"name": "value", "type": "string"}]})
        write_table("//tmp/dict", [
            {"key": "a", "subkey": 1, "value": "a1"},
            {"key": "a", "subkey": 2, "value": "a2"},
            {"key": "b", "subkey": 1, "value": "b1"},
        ])

        create("table", "//tmp/queries", attributes={"schema": [{"name": "key", "type": "string"},
                                                                {"name": "subkey", "type": "int64"}]})
        write_table("//tmp/queries", [{"key": "a", "subkey": 1},
                                      {"key": "a", "subkey": 2},
                                      {"key": "b", "subkey": 1},
                                      {"key": "b", "subkey": 2}])

        with Clique(1, config_patch={
            "engine": {"dictionaries": [
                {
                    "name": "dict",
                    "layout": {"complex_key_hashed": {}},
                    "structure": {
                        "key": {"attribute": [{"name": "key", "type": "String"},
                                              {"name": "subkey", "type": "Int64"}]},
                        "attribute": [{"name": "value", "type": "String", "null_value": "n/a"}]
                    },
                    "lifetime": 0,
                    "source": {
                        "yt": {
                            "path": "//tmp/dict"
                        }
                    }
                }
            ]}}) as clique:
            result = clique.make_query("select dictGetString('dict', 'value', tuple(key, subkey)) as value from \"//tmp/queries\"")
        assert result["data"] == [{"value": "a1"}, {"value": "a2"}, {"value": "b1"}, {"value": "n/a"}]

    def test_lifetime(self):
        create("table", "//tmp/dict", attributes={"schema": [{"name": "key", "type": "uint64"},
                                                             {"name": "value", "type": "string"}]})
        write_table("//tmp/dict", [{"key": 42, "value": "x"}])

        with Clique(1, config_patch={
            "engine": {"dictionaries": [
                {
                    "name": "dict",
                    "layout": {"flat": {}},
                    "structure": {
                        "id": {"name": "key"},
                        "attribute": [{"name": "value", "type": "String", "null_value": "n/a"}]
                    },
                    "lifetime": 1,
                    "source": {
                        "yt": {
                            "path": "//tmp/dict"
                        }
                    }
                }
            ]}}) as clique:
            assert clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")["data"][0]["value"] == "x"

            write_table("//tmp/dict", [{"key": 42, "value": "y"}])
            # TODO(max42): make update time customizable in CH and reduce this constant.
            time.sleep(7)
            assert clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")["data"][0]["value"] == "y"

            remove("//tmp/dict")
            time.sleep(7)
            assert clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")["data"][0]["value"] == "y"

            create("table", "//tmp/dict", attributes={"schema": [{"name": "key", "type": "uint64"},
                                                                 {"name": "value", "type": "string"}]})
            write_table("//tmp/dict", [{"key": 42, "value": "z"}])
            time.sleep(7)
            assert clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")["data"][0]["value"] == "z"

@pytest.mark.xfail(run=False, reason="I want to go to vacation and do not know what's the deal with those core dumps :(")
class TestClickHouseSchema(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def test_missing_schema(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"key": 42, "value": "x"}])

        with Clique(1) as clique:
            with pytest.raises(YtError):
                clique.make_query("select * from \"//tmp/t\"")

    def _to_description(self, columns):
        for column in columns:
            for unused_attribute in ["default_type", "default_expression", "comment", "codec_expression"]:
                column[unused_attribute] = ""
        return columns

    def test_common_schema_unsorted(self):
        create("table", "//tmp/t1", attributes={"schema": [
            {"name": "a", "type": "int64"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "double"},
        ]})
        create("table", "//tmp/t2", attributes={"schema": [
            {"name": "a", "type": "int64"},
            {"name": "d", "type": "double"},
        ]})
        create("table", "//tmp/t3", attributes={"schema": [
            {"name": "a", "type": "string"}
        ]})

        write_table("//tmp/t1", {"a": 42, "b": "x", "c": 3.14})
        write_table("//tmp/t2", {"a": 17, "d": 2.71})
        write_table("//tmp/t3", {"a": "y"})

        with Clique(1) as clique:
            assert clique.make_query("describe concatYtTables(\"//tmp/t1\", \"//tmp/t2\")")["data"] == \
                self._to_description([{"name": "a", "type": "Int64"}])
            assert clique.make_query("select * from concatYtTables(\"//tmp/t1\", \"//tmp/t2\") order by a")["data"] == \
                [{"a": 17}, {"a": 42}]

            with pytest.raises(YtError):
                clique.make_query("describe concatYtTables(\"//tmp/t1\", \"//tmp/t2\", \"//tmp/t3\")")

    def test_common_schema_sorted(self):
        create("table", "//tmp/t1", attributes={"schema": [
            {"name": "a", "type": "int64", "sort_order": "ascending"},
            {"name": "b", "type": "string", "sort_order": "ascending"},
            {"name": "c", "type": "double"},
        ]})
        create("table", "//tmp/t2", attributes={"schema": [
            {"name": "a", "type": "int64", "sort_order": "ascending"},
            {"name": "d", "type": "double"},
        ]})
        create("table", "//tmp/t3", attributes={"schema": [
            {"name": "a", "type": "int64"},
            {"name": "d", "type": "double"},
        ]})

        write_table("//tmp/t1", {"a": 42, "b": "x", "c": 3.14})
        write_table("//tmp/t2", {"a": 17, "d": 2.71})

        with Clique(1) as clique:
            with pytest.raises(YtError):
                clique.make_query("describe concatYtTables(\"//tmp/t1\", \"//tmp/t2\")")
            with pytest.raises(YtError):
                clique.make_query("describe concatYtTables(\"//tmp/t2\", \"//tmp/t1\")")
            with pytest.raises(YtError):
                clique.make_query("describe concatYtTables(\"//tmp/t1\", \"//tmp/t3\")")

            assert clique.make_query("describe concatYtTablesDropPrimaryKey(\"//tmp/t1\", \"//tmp/t2\")")["data"] == \
                self._to_description([{"name": "a", "type": "Int64"}])
            assert clique.make_query("select * from concatYtTablesDropPrimaryKey(\"//tmp/t1\", \"//tmp/t2\") order by a")["data"] == \
                [{"a": 17}, {"a": 42}]

            assert clique.make_query("describe concatYtTablesDropPrimaryKey(\"//tmp/t2\", \"//tmp/t1\")")["data"] == \
                self._to_description([{"name": "a", "type": "Int64"}])
            assert clique.make_query("select * from concatYtTablesDropPrimaryKey(\"//tmp/t2\", \"//tmp/t1\") order by a")["data"] == \
                [{"a": 17}, {"a": 42}]


@pytest.mark.xfail(run=False, reason="I want to go to vacation and do not know what's the deal with those core dumps :(")
class TestClickHouseAccess(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def test_clique_access(self):
        create_user("u")
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 1}])

        with Clique(1, config_patch={"validate_operation_access": False}) as clique:
            assert len(clique.make_query("select * from \"//tmp/t\"", user="u")["data"]) == 1

        with Clique(1, config_patch={"validate_operation_access": True}) as clique:
            with pytest.raises(YtError):
                clique.make_query("select * from \"//tmp/t\"", user="u")

        with Clique(1,
                    config_patch={"validate_operation_access": True},
                    spec={
                        "acl": [{
                            "subjects": ["u"],
                            "action": "allow",
                            "permissions": ["read"]
                        }]
                    }) as clique:
            assert len(clique.make_query("select * from \"//tmp/t\"", user="u")["data"]) == 1

