from yt_commands import *

from yt_env_setup import wait, YTEnvSetup, require_ytserver_root_privileges, is_asan_build
from yt.wrapper.clickhouse import get_clickhouse_clique_spec_builder
from yt.wrapper.common import simplify_structure

import yt.yson as yson

from yt.common import update

from distutils.spawn import find_executable

import json
import pytest
import psutil
import subprocess
import random
import os
import copy

TEST_DIR = os.path.join(os.path.dirname(__file__))

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
    path_to_run = None

    def __init__(self, instance_count, max_failed_job_count=0, config_patch=None, **kwargs):
        config = update(Clique.base_config, config_patch) if config_patch is not None else Clique.base_config
        spec = {"pool": None}
        if "spec" in kwargs:
            spec = update(spec, kwargs.pop("spec"))

        self.log_root = os.path.join(self.path_to_run, "logs", "clickhouse-{}".format(Clique.clique_index))
        for writer_key, writer in config["logging"]["writers"].iteritems():
            if writer["type"] == "file":
                writer["file_name"] = os.path.join(self.log_root, writer["file_name"])
        os.mkdir(self.log_root)
        os.chmod(self.log_root, 0777)

        filename = "//sys/clickhouse/config-{}.yson".format(Clique.clique_index)
        Clique.clique_index += 1
        create("file", filename)
        write_file(filename, yson.dumps(config, yson_format="pretty"))

        spec_builder = get_clickhouse_clique_spec_builder(instance_count,
                                                          cypress_config_path=filename,
                                                          max_failed_job_count=max_failed_job_count,
                                                          defaults=DEFAULTS,
                                                          spec=spec,
                                                          **kwargs)
        self.spec = simplify_structure(spec_builder.build())
        if not is_asan_build():
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

        self.log_root_alternative = os.path.realpath(os.path.join(self.log_root, "..",
                                                                  "clickhouse-{}".format(self.op.id)))
        os.symlink(self.log_root, self.log_root_alternative)

        print >>sys.stderr, "Waiting for clique {} to become ready".format(self.op.id)
        print >>sys.stderr, "Logging roots:\n- {}\n- {}".format(self.log_root, self.log_root_alternative)

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
            raise YtError("ClickHouse query failed\n" + output, attributes={"query": query})

        if stdout != "":
            result = json.loads(stdout)
            print >>sys.stderr, "Got", len(result["data"]), "rows"
            return result
        else:
            return None


@require_ytserver_root_privileges
class ClickHouseTestBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    NODE_PORT_SET_SIZE = 5

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
        Clique.path_to_run = self.path_to_run
        if CLICKHOUSE_CLIENT_BINARY is None or YTSERVER_CLICKHOUSE_BINARY is None:
            pytest.skip("This test requires built clickhouse and ytserver-clickhouse binaries; "
                        "they are available only when using ya as a build system")

        if exists("//sys/clickhouse"):
            return
        create("map_node", "//sys/clickhouse")

        # We need to inject cluster_connection into yson config.
        Clique.base_config = yson.loads(self._read_local_config_file("config.yson"))
        Clique.base_config["cluster_connection"] = self.__class__.Env.configs["driver"]


class TestClickHouseCommon(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @pytest.mark.parametrize("instance_count", [1, 5])
    def test_avg(self, instance_count):
        with Clique(instance_count) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
            for i in range(5):
                write_table("<append=%true>//tmp/t", [{"a": 2 * i}, {"a": 2 * i + 1}])

            assert abs(clique.make_query('select avg(a) from "//tmp/t"')["data"][0]["avg(a)"] - 4.5) < 1e-6
            with pytest.raises(YtError):
                clique.make_query('select avg(b) from "//tmp/t"')

            # TODO(max42): support range selectors.
            assert abs(clique.make_query('select avg(a) from "//tmp/t[#2:#9]"')["data"][0]["avg(a)"] - 5.0) < 1e-6

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

    def test_concat_inside_link(self):
        with Clique(1) as clique:
            create("map_node", "//tmp/dir")
            create("link", "//tmp/link", attributes={"target_path": "//tmp/dir"})
            create("table", "//tmp/link/t1", attributes={"schema": [{"name": "i", "type": "int64"}]})
            create("table", "//tmp/link/t2", attributes={"schema": [{"name": "i", "type": "int64"}]})
            write_table("//tmp/link/t1", [{"i": 0}])
            write_table("//tmp/link/t2", [{"i": 1}])
            assert len(clique.make_query("select * from concatYtTablesRange('//tmp/link')")["data"]) == 2

class TestJobInput(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def _expect_row_count(self, clique, query, exact=None, min=None, max=None, verbose=True):
        result = clique.make_query(query, verbose=verbose)
        assert (exact is not None) ^ (min is not None and max is not None)
        if exact is not None:
            assert result["statistics"]["rows_read"] == exact
        else:
            assert min <= result["statistics"]["rows_read"] <= max

    def test_chunk_filter(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "i", "type": "int64", "sort_order": "ascending"}]})
        for i in xrange(10):
            write_table("<append=%true>//tmp/t", [{"i": i}])
        with Clique(1) as clique:
            self._expect_row_count(clique, 'select * from "//tmp/t" where i >= 3', exact=7)
            self._expect_row_count(clique, 'select * from "//tmp/t" where i < 2', exact=2)
            self._expect_row_count(clique, 'select * from "//tmp/t" where 5 <= i and i <= 8', exact=4)
            self._expect_row_count(clique, 'select * from "//tmp/t" where i in (-1, 2, 8, 8, 15)', exact=2)

    def test_chunk_slicing(self):
        create("table",
               "//tmp/t",
               attributes={
                   "chunk_writer": {"block_size": 1024},
                   "compression_codec": "none",
                   # TODO(max42): investigate what happens when both columns are sorted.
                   "schema": [{"name": "i", "type": "int64", "sort_order": "ascending"},
                              {"name": "s", "type": "string"}]
               })

        write_table("//tmp/t", [{"i": i, "s": str(i) * (10 * 1024)} for i in range(10)], verbose=False)
        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#" + chunk_id + "/@compressed_data_size") > 100 * 1024
        assert get("#" + chunk_id + "/@max_block_size") < 20 * 1024

        with Clique(1) as clique:
            # Due to inclusiveness issues each of the row counts should be correct with some error.
            self._expect_row_count(clique, 'select i from "//tmp/t" where i >= 3', min=7, max=8)
            self._expect_row_count(clique, 'select i from "//tmp/t" where i < 2', min=3, max=4)
            self._expect_row_count(clique, 'select i from "//tmp/t" where 5 <= i and i <= 8', min=4, max=6)
            self._expect_row_count(clique, 'select i from "//tmp/t" where i in (-1, 2, 8, 8, 15)', min=2, max=4)

        # Forcefully disable chunk slicing.
        with Clique(1, config_patch={"engine": {"subquery": {"max_sliced_chunk_count": 0}}}) as clique:
            # Due to inclusiveness issues each of the row counts should be correct with some error.
            self._expect_row_count(clique, 'select i from "//tmp/t" where i >= 3', exact=10)
            self._expect_row_count(clique, 'select i from "//tmp/t" where i < 2', exact=10)
            self._expect_row_count(clique, 'select i from "//tmp/t" where 5 <= i and i <= 8', exact=10)
            self._expect_row_count(clique, 'select i from "//tmp/t" where i in (-1, 2, 8, 8, 15)', exact=10)

    def test_sampling(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": i} for i in range(1000)], verbose=False)
        with Clique(1) as clique:
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 0.1', min=85, max=115, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 100', min=85, max=115, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 2/20', min=85, max=115, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 0.1 offset 42', min=85, max=115, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 10000', exact=1000, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 10000', exact=1000, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 0', exact=0, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 0.000000000001', exact=0, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 1/100000000000', exact=0, verbose=False)


class TestMutations(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def test_insert_values(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "i64", "type": "int64"},
                                                          {"name": "ui64", "type": "uint64"},
                                                          {"name": "str", "type": "string"},
                                                          {"name": "dbl", "type": "double"},
                                                          {"name": "bool", "type": "boolean"}]})
        with Clique(1) as clique:
            clique.make_query('insert into "//tmp/t"(i64) values (1), (-2)')
            clique.make_query('insert into "//tmp/t"(ui64) values (7), (8)')
            with pytest.raises(YtError):
                clique.make_query('insert into "//tmp/t"(str) values (2)')
            clique.make_query('insert into "//tmp/t"(i64, ui64, str, dbl, bool) values (-1, 1, \'abc\', 3.14, 1)')
            clique.make_query('insert into "//tmp/t"(i64, ui64, str, dbl, bool) values (NULL, NULL, NULL, NULL, NULL)')
            with pytest.raises(YtError):
                clique.make_query('insert into "//tmp/t"(bool) values (3)')
            # This insert leads to NULL insertion.
            clique.make_query('insert into "//tmp/t"(bool) values (2.4)')
            assert read_table("//tmp/t") == [
                {"i64": 1, "ui64": None, "str": None, "dbl": None, "bool": None},
                {"i64": -2, "ui64": None, "str": None, "dbl": None, "bool": None},
                {"i64": None, "ui64": 7, "str": None, "dbl": None, "bool": None},
                {"i64": None, "ui64": 8, "str": None, "dbl": None, "bool": None},
                {"i64": -1, "ui64": 1, "str": "abc", "dbl": 3.14, "bool": True},
                {"i64": None, "ui64": None, "str": None, "dbl": None, "bool": None},
                {"i64": None, "ui64": None, "str": None, "dbl": None, "bool": None},
            ]
            assert get("//tmp/t/@chunk_count") == 5
            clique.make_query('insert into "<append=%false>//tmp/t" values (-2, 2, \'xyz\', 2.71, 0)')
            assert read_table("//tmp/t") == [
                {"i64": -2, "ui64": 2, "str": "xyz", "dbl": 2.71, "bool": False},
            ]
            assert get("//tmp/t/@chunk_count") == 1

    def test_insert_select(self):
        create("table", "//tmp/s1", attributes={"schema": [{"name": "i64", "type": "int64"},
                                                           {"name": "ui64", "type": "uint64"},
                                                           {"name": "str", "type": "string"},
                                                           {"name": "dbl", "type": "double"},
                                                           {"name": "bool", "type": "boolean"}]})
        write_table("//tmp/s1", [
            {"i64": 2, "ui64": 3, "str": "abc", "dbl": 3.14, "bool": True},
            {"i64": -1, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": False},
        ])

        # Table with different order of columns.
        create("table", "//tmp/s2", attributes={"schema": [{"name": "i64", "type": "int64"},
                                                           {"name": "str", "type": "string"},
                                                           {"name": "dbl", "type": "double"},
                                                           {"name": "ui64", "type": "uint64"},
                                                           {"name": "bool", "type": "boolean"}]})
        write_table("//tmp/s2", [
            {"i64": 4, "ui64": 9, "str": "def", "dbl": 12.3, "bool": False},
            {"i64": -5, "ui64": 5, "str": "ijk", "dbl": -3.1, "bool": True},
        ])

        create("table", "//tmp/t", attributes={"schema": [{"name": "i64", "type": "int64"},
                                                          {"name": "ui64", "type": "uint64"},
                                                          {"name": "str", "type": "string"},
                                                          {"name": "dbl", "type": "double"},
                                                          {"name": "bool", "type": "boolean"}]})
        with Clique(1) as clique:
            clique.make_query('insert into "//tmp/t" select * from "//tmp/s1"')
            assert read_table("//tmp/t") == [
                {"i64": 2, "ui64": 3, "str": "abc", "dbl": 3.14, "bool": True},
                {"i64": -1, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": False},
            ]

            # Number of columns does not match.
            with pytest.raises(YtError):
                clique.make_query('insert into "//tmp/t" select i64, ui64 from "//tmp/s1"')

            # Columns are matched according to positions. Values are best-effort casted due to CH logic.
            clique.make_query('insert into "<append=%false>//tmp/t" select * from "//tmp/s2"')
            assert read_table("//tmp/t") == [
                {"i64": 4, "ui64": None, "str": "12.3", "dbl": 9.0, "bool": False},
                {"i64": -5, "ui64": None, "str": "-3.1", "dbl": 5.0, "bool": True},
            ]

            clique.make_query('insert into "<append=%false>//tmp/t" select i64, ui64, str, dbl, bool from "//tmp/s2"')
            assert read_table("//tmp/t") == [
                {"i64": 4, "ui64": 9, "str": "def", "dbl": 12.3, "bool": False},
                {"i64": -5, "ui64": 5, "str": "ijk", "dbl": -3.1, "bool": True},
            ]

            clique.make_query('insert into "//tmp/t"(i64, ui64) select max(i64), min(ui64) from "//tmp/s2"')
            assert read_table("//tmp/t") == [
                {"i64": 4, "ui64": 9, "str": "def", "dbl": 12.3, "bool": False},
                {"i64": -5, "ui64": 5, "str": "ijk", "dbl": -3.1, "bool": True},
                {"i64": 4, "ui64": 5, "str": None, "dbl": None, "bool": None},
            ]

    def test_create_table_simple(self):
        with Clique(1, config_patch={"engine": {"create_table_default_attributes": {"foo": 42}}}) as clique:
            clique.make_query('create table "//tmp/t"(i64 Int64, ui64 UInt64, str String, dbl Float64, i32 Int32) '
                              'engine YtTable() order by (str, i64)')
            assert normalize_schema(get("//tmp/t/@schema")) == make_schema([
                {"name": "str", "type": "string", "sort_order": "ascending", "required": False},
                {"name": "i64", "type": "int64", "sort_order": "ascending", "required": False},
                {"name": "ui64", "type": "uint64", "required": False},
                {"name": "dbl", "type": "double", "required": False},
                {"name": "i32", "type": "int64", "required": False},
            ], strict=True, unique_keys=False)

            # Table already exists.
            with pytest.raises(YtError):
                clique.make_query('create table "//tmp/t"(i64 Int64, ui64 UInt64, str String, dbl Float64, i32 Int32) '
                                  'engine YtTable() order by (str, i64)')

            # No non-trivial expressions.
            with pytest.raises(YtError):
                clique.make_query('create table "//tmp/t2"(i64 Int64) engine YtTable() order by (i64 * i64)')

            # No fancy types.
            with pytest.raises(YtError):
                clique.make_query('create table "//tmp/t2"(d DateTime) engine YtTable()')

            # Missing key column.
            with pytest.raises(YtError):
                clique.make_query('create table "//tmp/t2"(i Int64) engine YtTable() order by j')

            clique.make_query('create table "//tmp/t_snappy"(i Int64) engine YtTable(\'{compression_codec=snappy}\')')
            assert get("//tmp/t_snappy/@compression_codec") == "snappy"

            # Default optimize_for should be scan.
            assert get("//tmp/t_snappy/@optimize_for") == "scan"

            assert get("//tmp/t_snappy/@foo") == 42

            # Empty schema.
            with pytest.raises(YtError):
                clique.make_query('create table "//tmp/t2" engine YtTable()')

            # Underscore indicates that the columns should be ignored and schema from attributes should
            # be taken.
            clique.make_query('create table "//tmp/t2"(_ UInt8) engine YtTable(\'{schema=[{name=a;type=int64}]}\')')
            assert get("//tmp/t2/@schema/0/name") == "a"

            # Column list has higher priority.
            clique.make_query('create table "//tmp/t3"(b String) engine YtTable(\'{schema=[{name=a;type=int64}]}\')')
            assert get("//tmp/t3/@schema/0/name") == "b"

    def test_create_table_as_select(self):
        create("table", "//tmp/s1", attributes={"schema": [{"name": "i64", "type": "int64"},
                                                           {"name": "ui64", "type": "uint64"},
                                                           {"name": "str", "type": "string"},
                                                           {"name": "dbl", "type": "double"},
                                                           {"name": "bool", "type": "boolean"}]})
        write_table("//tmp/s1", [
            {"i64": -1, "ui64": 3, "str": "def", "dbl": 3.14, "bool": True},
            {"i64": 2, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": False},
        ])

        with Clique(1) as clique:
            clique.make_query('create table "//tmp/t1" engine YtTable() order by i64 as select * from "//tmp/s1"')

            assert read_table("//tmp/t1") == [
                {"i64": -1, "ui64": 3, "str": "def", "dbl": 3.14, "bool": 1},
                {"i64": 2, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": 0},
            ]

    def test_create_table_as_table(self):
        schema = [{"name": "i64", "type": "int64", "required": False, "sort_order": "ascending"},
                  {"name": "ui64", "type": "uint64", "required": False},
                  {"name": "str", "type": "string", "required": False},
                  {"name": "dbl", "type": "double", "required": False},
                  {"name": "bool", "type": "boolean", "required": False}]
        schema_copied = copy.deepcopy(schema)
        schema_copied[4]["type"] = "uint64"
        create("table", "//tmp/s1", attributes={"schema": schema,
                                                "compression_codec": "snappy"})

        with Clique(1) as clique:
            clique.make_query('show create table "//tmp/s1"')
            clique.make_query('create table "//tmp/s2" as "//tmp/s1" engine YtTable() order by i64')
            assert normalize_schema(get("//tmp/s2/@schema")) == make_schema(schema_copied, strict=True, unique_keys=False)

            # This is wrong.
            # assert get("//tmp/s2/@compression_codec") == "snappy"


class TestCompositeTypes(ClickHouseTestBase):
    def setup(self):
        self._setup()

        create("table", "//tmp/t", attributes={"schema": [{"name": "i", "type": "int64"}, {"name": "v", "type": "any"}]})
        write_table("//tmp/t", [
            {
                "i": 0,
                "v": {
                    "i64": -42,
                    "ui64": yson.YsonUint64(23),
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
                    "i64": yson.YsonUint64(2**63 + 42),  # Out of range for getting value as i64.
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


class TestYtDictionaries(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def test_int_key_flat(self):
        create("table", "//tmp/dict", attributes={"schema": [{"name": "key", "type": "uint64", "required": True},
                                                             {"name": "value_str", "type": "string", "required": True},
                                                             {"name": "value_i64", "type": "int64", "required": True}]})
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
        create("table", "//tmp/dict", attributes={"schema": [{"name": "key", "type": "string", "required": True},
                                                             {"name": "subkey", "type": "int64", "required": True},
                                                             {"name": "value", "type": "string", "required": True}]})
        write_table("//tmp/dict", [
            {"key": "a", "subkey": 1, "value": "a1"},
            {"key": "a", "subkey": 2, "value": "a2"},
            {"key": "b", "subkey": 1, "value": "b1"},
        ])

        create("table", "//tmp/queries", attributes={"schema": [{"name": "key", "type": "string", "required": True},
                                                                {"name": "subkey", "type": "int64", "required": True}]})
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
        create("table", "//tmp/dict", attributes={"schema": [{"name": "key", "type": "uint64", "required": True},
                                                             {"name": "value", "type": "string", "required": True}]})
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


class TestClickHouseSchema(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def test_missing_schema(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"key": 42, "value": "x"}])

        with Clique(1) as clique:
            with pytest.raises(YtError):
                clique.make_query("select * from \"//tmp/t\"")

    def _strip_description(self, rows):
        return [{key: value for key, value in row.iteritems() if key in ("name", "type")} for row in rows]

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
            assert self._strip_description(clique.make_query("describe concatYtTables(\"//tmp/t1\", \"//tmp/t2\")")["data"]) == \
                [{"name": "a", "type": "Nullable(Int64)"}]
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
            assert clique.make_query("describe concatYtTables(\"//tmp/t1\", \"//tmp/t2\")")["data"] == self._to_description(
                [{"name": "a", "type": "Nullable(Int64)"}])
            assert clique.make_query("describe concatYtTables(\"//tmp/t2\", \"//tmp/t1\")")["data"] == self._to_description(
                [{"name": "a", "type": "Nullable(Int64)"}])
            with pytest.raises(YtError):
                clique.make_query("describe concatYtTables(\"//tmp/t1\", \"//tmp/t3\")")

    def test_nulls_in_primary_key(self):
        create("table", "//tmp/t", attributes={"schema": [
            {"name": "a", "type": "int64", "sort_order": "ascending"}
        ]})

        content = [{"a": None}, {"a": -1}, {"a": 42}]
        write_table("//tmp/t", content)

        with Clique(1) as clique:
            for source in ["\"//tmp/t\"", "concatYtTables('//tmp/t')"]:
                assert clique.make_query("select * from {}".format(source))["data"] == content
                assert clique.make_query("select * from {} where isNull(a)".format(source))["data"] == [{"a": None}]
                assert clique.make_query("select * from {} where isNotNull(a)".format(source))["data"] == [{"a": -1}, {"a": 42}]

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


class TestQueryLog(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def test_query_log(self):
        with Clique(1, config_patch={"engine": {"settings": {"log_queries": 1}}}) as clique:
            clique.make_query("select 1")
            wait(lambda: len(clique.make_query("select * from system.tables where database = 'system' and "
                                               "name = 'query_log';")["data"]) >= 1)
            wait(lambda: len(clique.make_query("select * from system.query_log")["data"]) >= 1)
