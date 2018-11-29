from yt_env_setup import wait, YTEnvSetup, require_ytserver_root_privileges
from yt_commands import *
from yt.wrapper.clickhouse import get_clickhouse_clique_spec_builder
from yt.wrapper.common import simplify_structure

from distutils.spawn import find_executable

TEST_DIR = os.path.join(os.path.dirname(__file__))

import json
import pytest
import psutil
import subprocess
import random

@require_ytserver_root_privileges
class TestClickhouse(YTEnvSetup):
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
                                                          max_failed_job_count=max_failed_job_count,
                                                          *kwargs)
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

    def setup(self):
        self._clickhouse_client_binary = find_executable("clickhouse")
        self._ytserver_clickhouse_binary = find_executable("ytserver-clickhouse")

        if self._clickhouse_client_binary is None or self._ytserver_clickhouse_binary is None:
            pytest.skip("This test requires built clickhouse and ytserver-clickhouse binaries; "
                        "they are available only when using ya as a build system")

        if exists("//sys/clickhouse"):
            return
        create("map_node", "//sys/clickhouse")
        create("map_node", "//sys/clickhouse/config_files")
        create("map_node", "//sys/clickhouse/configuration")

        # Setup necessary config files and upload them to the Cypress as files.
        file_configs = dict()

        file_configs["config_files/config.xml"] = self._read_local_config_file("config.xml")

        # We need to inject cluster_connection into yson config.
        yson_config = yson.loads(self._read_local_config_file("config.yson"))
        yson_config["cluster_connection"] = self.__class__.Env.configs["driver"]
        file_configs["config_files/config.yson"] = yson.dumps(yson_config, yson_format="pretty")

        file_configs["configuration/clusters"] = ""

        # Upload some of the configs as yson documents.
        document_configs = dict()

        document_configs["configuration/server"] = yson.loads(self._read_local_config_file("server.yson"))
        document_configs["configuration/users"] = yson.loads(self._read_local_config_file("users.yson"))

        for config_name, content in file_configs.iteritems():
            cypress_path = "//sys/clickhouse/{0}".format(config_name)
            create("file", cypress_path)
            write_file(cypress_path, content)

        for config_name, content in document_configs.iteritems():
            cypress_path = "//sys/clickhouse/{0}".format(config_name)
            create("document", cypress_path)
            set(cypress_path, content)

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

    @pytest.mark.parametrize("instance_count", [1, 5])
    def test_avg(self, instance_count):
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
        write_table("//tmp/t", [{"a": "2012-12-12"}])

        result = self._make_query(clique, 'select CAST(a as datetime) from "//tmp/t"')
        assert result["data"] == [{"CAST(a as datetime)": "2012-12-12"}]

