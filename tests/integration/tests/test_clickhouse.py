from yt_env_setup import wait, YTEnvSetup, require_ytserver_root_privileges
from yt_commands import *
from yt.wrapper.clickhouse import get_clickhouse_clique_spec_builder
from yt.wrapper.common import simplify_structure

from yt.yson import YsonUint64

from distutils.spawn import find_executable

TEST_DIR = os.path.join(os.path.dirname(__file__))

import json
import pytest
import psutil
import subprocess
import random

@require_ytserver_root_privileges
class ClickhouseTestBase(YTEnvSetup):
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

    def _start_clique(self, instance_count, max_failed_job_count=1, **kwargs):
        spec_builder = get_clickhouse_clique_spec_builder(instance_count,
                                                          host_ytserver_clickhouse_path=self._ytserver_clickhouse_binary,
                                                          cypress_config_path="//sys/clickhouse/config.yson",
                                                          max_failed_job_count=max_failed_job_count,
                                                          cpu_limit=1,
                                                          memory_limit=5*2**30,
                                                          **kwargs)
        spec = simplify_structure(spec_builder.build())
        op = start_op("vanilla",
                      spec=spec,
                      dont_track=True)

        counter = 0
        while True:
            state = op.get_state(verbose=False)

            # Clickhouse operations should never complete by itself.
            assert state != "completed"

            if state == "aborted" or state == "failed":
                raise op.get_error()
            elif state == "running" and \
                    exists("//sys/clickhouse/cliques/{0}".format(op.id)) and \
                    len(get("//sys/clickhouse/cliques/{0}".format(op.id))) == instance_count:
                return op
            elif counter % 30 == 0:
                print >>sys.stderr, op.build_progress()
            time.sleep(op._poll_frequency)
            counter += 1

    def _read_local_config_file(self, name):
        return open(os.path.join(TEST_DIR, "test_clickhouse", name)).read()

    def _setup(self):
        self._clickhouse_client_binary = find_executable("clickhouse")
        self._ytserver_clickhouse_binary = find_executable("ytserver-clickhouse")

        if self._clickhouse_client_binary is None or self._ytserver_clickhouse_binary is None:
            pytest.skip("This test requires built clickhouse and ytserver-clickhouse binaries; "
                        "they are available only when using ya as a build system")

        if exists("//sys/clickhouse"):
            return
        create("map_node", "//sys/clickhouse")

        # We need to inject cluster_connection into yson config.
        config = yson.loads(self._read_local_config_file("config.yson"))
        config["cluster_connection"] = self.__class__.Env.configs["driver"]
        create("file", "//sys/clickhouse/config.yson")
        write_file("//sys/clickhouse/config.yson", yson.dumps(config, yson_format="pretty"))

    def _make_query(self, clique, query, verbose=True):
        instances = get("//sys/clickhouse/cliques/{0}".format(clique.id), attributes=["host", "tcp_port"])
        assert len(instances) > 0
        instance = random.choice(instances.values())
        host = instance.attributes["host"]
        port = instance.attributes["tcp_port"]

        args = [self._clickhouse_client_binary, "client",
                "-h", host,
                "--port", port,
                "-q", query,
                "--format", "JSON",
                "--output_format_json_quote_64bit_integers", "0"]
        print >>sys.stderr, "Running '{0}'...".format(' '.join(args))

        process = psutil.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
            raise YtError("Clickhouse query failed\n" + output)

        return json.loads(stdout)


class TestClickhouseCommon(ClickhouseTestBase):
    def setup(self):
        self._setup()

    def test_readonly(self):
        clique = self._start_clique(1)

        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})

        try:
            self._make_query(clique, 'insert into "//tmp/t" values(1)')
        except YtError as err:
            # 164 is an error code for READONLY.
            assert "164" in str(err)

    @pytest.mark.parametrize("instance_count", [1, 5])
    def test_avg(self, instance_count):
        if instance_count == 5:
            # TODO(max42).
            return
        clique = self._start_clique(instance_count)

        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        for i in range(10):
            write_table("<append=%true>//tmp/t", {"a": i})

        assert abs(self._make_query(clique, 'select avg(a) from "//tmp/t"')["data"][0]["avg(a)"] - 4.5) < 1e-6
        with pytest.raises(YtError):
            self._make_query(clique, 'select avg(b) from "//tmp/t"')
        # TODO(max42): support range selectors.
        #assert abs(float(self._make_query(clique, 'select avg(a) from "//tmp/t[#2:#9]"')) - 5.0) < 1e-6

    # YT-9497
    def test_aggregation_with_multiple_string_columns(self):
        clique = self._start_clique(1)

        create("table", "//tmp/t", attributes={"schema": [{"name": "key1", "type": "string"},
                                                          {"name": "key2", "type": "string"},
                                                          {"name": "value", "type": "int64"}]})
        for i in range(5):
            write_table("<append=%true>//tmp/t", [{"key1": "dream", "key2": "theater", "value": i * 5 + j} for j in range(5)])
        total = 24 * 25 // 2

        result = self._make_query(clique, 'select key1, key2, sum(value) from "//tmp/t" group by key1, key2')
        assert result["data"] == [{"key1": "dream", "key2": "theater", "sum(value)": total}]

    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_cast(self, instance_count):
        clique = self._start_clique(instance_count)

        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
        write_table("//tmp/t", [{"a": "2012-12-12 20:00:00"}])

        result = self._make_query(clique, 'select CAST(a as datetime) from "//tmp/t"')
        assert result["data"] == [{"CAST(a, 'datetime')": "2012-12-12 20:00:00"}]

    def test_settings(self):
        clique = self._start_clique(1)

        # I took some random option from the documentation and changed it in config.yson.
        # Let's see if it changed in internal table with settings.

        result = self._make_query(clique, "select * from system.settings where name = 'max_temporary_non_const_columns'")
        assert result["data"][0]["value"] == "1234"
        assert result["data"][0]["changed"] == 1

    def test_schema_caching(self):
        clique = self._start_clique(1)

        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 1}])
        old_description = self._make_query(clique, 'describe "//tmp/t"')
        assert old_description["data"][0]["name"] == "a"
        remove("//tmp/t")
        create("table", "//tmp/t", attributes={"schema": [{"name": "b", "type": "int64"}]})
        write_table("//tmp/t", [{"b": 1}])
        new_description = self._make_query(clique, 'describe "//tmp/t"')
        assert new_description["data"][0]["name"] == "b"


class TestCompositeTypes(ClickhouseTestBase):
    def setup(self):
        self._setup()
        self.clique = self._start_clique(1)

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

    def teardown(self):
        if self.clique.get_state() == "running":
            self.clique.abort()
        else:
            self.clique.track()

    def test_read_int64_strict(self):
        for i in xrange(4):
            query = "select YPathInt64Strict(v, '/i64') from \"//tmp/t\" where i = {}".format(i)
            if i != 0:
                with pytest.raises(YtError):
                    self._make_query(self.clique, query)
            else:
                result = self._make_query(self.clique, query)
                assert result["data"][0].popitem()[1] == -42

    def test_read_uint64_strict(self):
        result = self._make_query(self.clique, "select YPathUInt64Strict(v, '/i64') from \"//tmp/t\" where i = 4")
        assert result["data"][0].popitem()[1] == 57

    def test_read_from_subnode(self):
        result = self._make_query(self.clique, "select YPathUInt64Strict(v, '/subnode/i64') from \"//tmp/t\" where i = 0")
        assert result["data"][0].popitem()[1] == 123

    def test_read_int64_non_strict(self):
        query = "select YPathInt64(v, '/i64') from \"//tmp/t\""
        result = self._make_query(self.clique, query)
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
        result = self._make_query(self.clique, query)
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
        result = self._make_query(self.clique, query)
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
        result = self._make_query(self.clique, "select YPathString('{a=[1;2;{b=xyz}]}', '/a/2/b') as str")
        assert result["data"] == [{"str": "xyz"}]

    def test_nulls(self):
        result = self._make_query(self.clique, "select YPathString(NULL, NULL) as a, YPathString(NULL, '/x') as b, "
                                               "YPathString('{a=1}', NULL) as c")
        assert result["data"] == [{"a": None, "b": None, "c": None}]
