from yt_env_setup import wait, YTEnvSetup, require_ytserver_root_privileges
from yt_commands import *
from yt.wrapper.clickhouse import get_clickhouse_clique_spec_builder
from yt.wrapper.common import sanitize_structure

from distutils.spawn import find_executable

TEST_DIR = os.path.join(os.path.dirname(__file__))

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
            }
        }
    }

    def _start_clique(self, instance_count, max_failed_job_count=1, **kwargs):
        spec_builder = get_clickhouse_clique_spec_builder(instance_count,
                                                          host_ytserver_clickhouse_path=self._ytserver_clickhouse_binary,
                                                          max_failed_job_count=max_failed_job_count,
                                                          *kwargs)
        spec = sanitize_structure(spec_builder.build())
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

        args = [self._clickhouse_client_binary, "client", "-h", host, "--port", port, "-q", query]
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

        return stdout

    @pytest.mark.parametrize("instance_count", [1, 5])
    def test_avg(self, instance_count):
        clique = self._start_clique(instance_count)

        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        for i in range(10):
            write_table("<append=%true>//tmp/t", {"a": i})

        assert abs(float(self._make_query(clique, 'select avg(a) from "//tmp/t"')) - 4.5) < 1e-6
        with pytest.raises(YtError):
            self._make_query(clique, 'select avg(b) from "//tmp/t"')
        # TODO(max42): support range selectors.
        #assert abs(float(self._make_query(clique, 'select avg(a) from "//tmp/t[#2:#9]"')) - 5.0) < 1e-6
