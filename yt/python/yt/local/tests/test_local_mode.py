from yt.local import start, stop, delete
import yt.local as yt_local
from yt.common import remove_file, is_process_alive
from yt.wrapper import YtClient
from yt.wrapper.common import generate_uuid
from yt.environment.helpers import is_dead
from yt.environment.arcadia_interop import yatest_common, search_binary_path
from yt.test_helpers import get_tests_sandbox, get_build_root, wait, prepare_yt_environment
import yt.subprocess_wrapper as subprocess

import yt.yson as yson

import yt.wrapper as yt

import os
import logging
import pytest
import tempfile
import signal
import contextlib
import time
import string
import random
import sys

logger = logging.getLogger("YtLocal")

yt.http_helpers.RECEIVE_TOKEN_FROM_SSH_SESSION = False

TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))


def _get_tests_location():
    if yatest_common is not None:
        return yatest_common.source_path("yt/python/yt/local/tests")
    else:
        return TESTS_LOCATION


def _get_local_mode_tests_sandbox():
    return os.path.join(get_tests_sandbox(), "TestLocalMode")


def _get_yt_local_binary():
    if yatest_common is not None:
        return yatest_common.binary_path("yt/python/yt/local/bin/yt_local_native_make/yt_local")
    else:
        return os.path.join(TESTS_LOCATION, "../bin/yt_local")


def _get_instance_path(instance_id):
    return os.path.join(_get_local_mode_tests_sandbox(), instance_id)


def _read_pids_file(instance_id):
    pids_filename = os.path.join(_get_instance_path(instance_id), "pids.txt")
    if not os.path.exists(pids_filename):
        return []
    with open(pids_filename) as f:
        return list(map(int, f))


def _is_exists(environment):
    return os.path.exists(_get_instance_path(environment.id))


def _wait_instance_to_become_ready(process, instance_id):
    special_file = os.path.join(_get_instance_path(instance_id), "started")

    attempt_count = 10
    for _ in range(attempt_count):
        print("Waiting instance", instance_id, "to become ready...")
        if os.path.exists(special_file):
            return

        if process.poll() is not None:
            stderr = process.stderr.read()
            raise yt.YtError("Local YT instance process exited with error code {0}: {1}"
                             .format(process.returncode, stderr))

        time.sleep(1.0)

    raise yt.YtError("Local YT is not started")


@pytest.fixture(scope="session", autouse=True)
def prepare_yt():
    prepare_yt_environment()


@contextlib.contextmanager
def local_yt(*args, **kwargs):
    environment = None
    try:
        environment = start(*args, enable_debug_logging=True, fqdn="localhost", **kwargs)
        yield environment
    except:  # noqa
        logger.exception("Failed to start local yt")
        raise
    finally:
        if environment is not None:
            stop(environment.id)


class YtLocalBinary(object):
    def __init__(self, root_path):
        self.root_path = root_path

    def _prepare_binary_command_and_env(self, *args, **kwargs):
        command = [_get_yt_local_binary()]
        command += list(args)

        for key, value in kwargs.items():
            key = key.replace("_", "-")
            if value is True:
                command.extend(["--" + key])
            elif value is not False:
                command.extend(["--" + key, str(value)])

            command.extend(["--enable-debug-logging", "--fqdn", "localhost"])

        env = {
            "YT_LOCAL_ROOT_PATH": self.root_path,
            "PYTHONPATH": os.environ["PYTHONPATH"],
            "PATH": os.environ["PATH"],
        }
        return command, env

    def __call__(self, *args, **kwargs):
        command, env = self._prepare_binary_command_and_env(*args, **kwargs)
        return subprocess.check_output(command, env=env).strip()

    def run_async(self, *args, **kwargs):
        command, env = self._prepare_binary_command_and_env(*args, **kwargs)
        return subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def _get_id(test_name):
    collection = string.ascii_lowercase + string.ascii_uppercase + string.digits
    return test_name + "_" + "".join(random.choice(collection) for _ in range(5))


@pytest.mark.parametrize("enable_multidaemon", [False, True])
class TestLocalMode(object):
    @classmethod
    def setup_class(cls):
        cls.old_yt_local_root_path = os.environ.get("YT_LOCAL_ROOT_PATH", None)
        os.environ["YT_LOCAL_ROOT_PATH"] = _get_local_mode_tests_sandbox()
        cls.yt_local = YtLocalBinary(os.environ["YT_LOCAL_ROOT_PATH"])
        cls.env_path = os.environ.get("PATH")

    @classmethod
    def teardown_class(cls):
        if cls.old_yt_local_root_path is not None:
            os.environ["YT_LOCAL_ROOT_PATH"] = cls.old_yt_local_root_path
        if cls.env_path is None:
            if "PATH" in os.environ:
                del os.environ["PATH"]
        else:
            os.environ["PATH"] = cls.env_path

    def test_logging(self, enable_multidaemon):
        scheduler_count = 4
        node_count = 2
        master_count = 3

        with local_yt(id=_get_id("test_logging"), master_count=master_count, node_count=node_count,
                      scheduler_count=scheduler_count, http_proxy_count=1, enable_multidaemon=enable_multidaemon) as lyt:
            path = lyt.path
            logs_path = lyt.logs_path

        if enable_multidaemon:
            assert os.path.exists(os.path.join(logs_path, "multi.log"))
        else:
            for index in range(master_count):
                name = "master-0-" + str(index) + ".log"
                assert os.path.exists(os.path.join(logs_path, name))

            for index in range(node_count):
                name = "node-" + str(index) + ".log"
                assert os.path.exists(os.path.join(logs_path, name))

            for index in range(scheduler_count):
                name = "scheduler-" + str(index) + ".log"
                assert os.path.exists(os.path.join(logs_path, name))

            assert os.path.exists(os.path.join(logs_path, "http-proxy-0.log"))
        assert os.path.exists(os.path.join(path, "stderrs"))

    def test_user_configs_path(self, enable_multidaemon):
        master_count = 3
        node_count = 2
        scheduler_count = 4
        http_proxy_count = 5
        rpc_proxy_count = 6
        with local_yt(id=_get_id("test_user_configs_path"), master_count=master_count,
                      node_count=node_count, scheduler_count=scheduler_count,
                      http_proxy_count=http_proxy_count, rpc_proxy_count=rpc_proxy_count, enable_multidaemon=enable_multidaemon) as lyt:
            configs_path = lyt.configs_path

        assert os.path.exists(configs_path)
        assert os.path.exists(os.path.join(configs_path, "driver-0.yson"))
        assert os.path.exists(os.path.join(configs_path, "rpc-client.yson"))

        multiple_component_to_count = {
            "master-0": master_count,
            "node": node_count,
            "scheduler": scheduler_count,
            "http-proxy": http_proxy_count,
            "rpc-proxy": rpc_proxy_count,
        }

        for component, count in multiple_component_to_count.items():
            for index in range(count):
                name = "{}-{}.yson".format(component, index)
                assert os.path.exists(os.path.join(configs_path, name))

    def test_watcher(self, enable_multidaemon):
        # YT-19646: temporary disable this test in OS.
        # Wait for YT-19347 to investigate this problem.
        # if yatest_common is None and not which("logrotate"):
        if yatest_common is None:
            pytest.skip()

        watcher_config = {
            "logs_rotate_size": "1k",
            "logs_rotate_interval": 1,
            "logs_rotate_max_part_count": 5
        }
        with local_yt(id=_get_id("test_watcher"), watcher_config=watcher_config, enable_multidaemon=enable_multidaemon) as environment:
            proxy_port = environment.get_proxy_address().rsplit(":", 1)[1]
            client = YtClient(proxy="localhost:{0}".format(proxy_port))

            for _ in range(300):
                client.mkdir("//test")
                client.set("//test/node", "abc")
                client.get("//test/node")
                client.remove("//test/node")
                client.remove("//test")

            logs_path = environment.logs_path

        assert os.path.exists(logs_path)

        # Some log file may be missing if we exited during log rotation.
        presented = 0
        name = "http-proxy-0" if not enable_multidaemon else "multi"
        for file_index in range(1, 5):
            presented += os.path.exists(os.path.join(logs_path, f"{name}.debug.log.{file_index}.gz"))
        assert presented in (3, 4), "Log paths: {}".format(", ".join(os.listdir(logs_path)))

    def test_commands_sanity(self, enable_multidaemon):
        with local_yt(id=_get_id("test_commands_sanity"), enable_multidaemon=enable_multidaemon) as environment:
            pids = _read_pids_file(environment.id)
            expected_pid_count = 2 if enable_multidaemon else 6
            assert len(pids) == expected_pid_count
            # Should not delete running instance
            with pytest.raises(yt.YtError):
                delete(environment.id)
        # Should not stop already stopped instance
        with pytest.raises(yt.YtError):
            stop(environment.id)
        assert not os.path.exists(environment.pids_filename)
        delete(environment.id)
        assert not _is_exists(environment)

    def test_start(self, enable_multidaemon):
        with pytest.raises(yt.YtError):
            start(master_count=0, enable_multidaemon=enable_multidaemon)

        master_count = 3

        with local_yt(id=_get_id("test_start_masters_and_proxy"), master_count=master_count,
                      node_count=0, scheduler_count=0, controller_agent_count=0, enable_multidaemon=enable_multidaemon) as environment:
            expected_pid_count = 2 if enable_multidaemon else 5
            assert len(_read_pids_file(environment.id)) == expected_pid_count
            assert len(environment.configs["master"]) == master_count

        if enable_multidaemon:
            # FIXME(nadya73): It fails if node_count > 2.
            node_count = 2
        else:
            node_count = 5

        with local_yt(id=_get_id("test_start_no_proxy_many_schedulers"),
                      node_count=node_count, scheduler_count=2, http_proxy_count=0, enable_multidaemon=enable_multidaemon) as environment:
            assert len(environment.configs["node"]) == node_count
            assert len(environment.configs["scheduler"]) == 2
            assert len(environment.configs["master"]) == 1
            assert len(environment.configs["http_proxy"]) == 0
            assert len(environment.configs["controller_agent"]) == 1
            expected_pid_count = 2 if enable_multidaemon else 10
            assert len(_read_pids_file(environment.id)) == expected_pid_count
            with pytest.raises(yt.YtError):
                environment.get_proxy_address()

        with local_yt(id=_get_id("test_start_with_one_node"), node_count=1, enable_multidaemon=enable_multidaemon) as environment:
            expected_pid_count = 2 if enable_multidaemon else 6
            assert len(_read_pids_file(environment.id)) == expected_pid_count

        with local_yt(id=_get_id("test_start_masters_only"), node_count=0,
                      scheduler_count=0, controller_agent_count=0, http_proxy_count=0, enable_multidaemon=enable_multidaemon) as environment:
            assert len(_read_pids_file(environment.id)) == 2

    def test_use_local_yt(self, enable_multidaemon):
        with local_yt(id=_get_id("test_use_local_yt"), enable_multidaemon=enable_multidaemon) as environment:
            proxy_port = environment.get_proxy_address().rsplit(":", 1)[1]
            client = YtClient(proxy="localhost:{0}".format(proxy_port))
            client.config["tabular_data_format"] = yt.format.DsvFormat()
            client.mkdir("//test")

            client.set("//test/node", "abc")
            assert client.get("//test/node") == "abc"
            assert client.list("//test") == ["node"]

            client.remove("//test/node")
            assert not client.exists("//test/node")

            client.mkdir("//test/folder")
            assert client.get("//test/folder/@type") == "map_node"

            table = "//test/table"
            client.create("table", table)
            client.write_table(table, [{"a": "b"}])
            assert [{"a": "b"}] == list(client.read_table(table))

            assert set(client.search("//test")) == set(["//test", "//test/folder", table])

    def test_use_context_manager(self, enable_multidaemon):
        with yt_local.LocalYt(id=_get_id("test_use_context_manager"), fqdn="localhost", enable_multidaemon=enable_multidaemon) as client:
            client.config["tabular_data_format"] = yt.format.DsvFormat()
            client.mkdir("//test")

            client.set("//test/node", "abc")
            assert client.get("//test/node") == "abc"
            assert client.list("//test") == ["node"]

            client.remove("//test/node")
            assert not client.exists("//test/node")

            client.mkdir("//test/folder")
            assert client.get("//test/folder/@type") == "map_node"

            table = "//test/table"
            client.create("table", table)
            client.write_table(table, [{"a": "b"}])
            assert [{"a": "b"}] == list(client.read_table(table))

            assert set(client.search("//test")) == set(["//test", "//test/folder", table])

        with yt_local.LocalYt(path=os.path.join(get_tests_sandbox(), "test_path"), fqdn="localhost", enable_debug_logging=True):
            pass

    def test_local_cypress_synchronization(self, enable_multidaemon):
        local_cypress_path = os.path.join(_get_tests_location(), "local_cypress_tree")
        with local_yt(id=_get_id("test_local_cypress_synchronization"),
                      local_cypress_dir=local_cypress_path, enable_multidaemon=enable_multidaemon) as environment:
            proxy_port = environment.get_proxy_address().rsplit(":", 1)[1]
            client = YtClient(proxy="localhost:{0}".format(proxy_port))
            assert list(client.read_table("//table")) == [{"x": "1", "y": "1"}]
            assert client.get("//subdir/@type") == "map_node"
            assert client.get_attribute("//table", "myattr") == 4
            assert client.get_attribute("//subdir", "other_attr") == 42
            assert client.get_attribute("/", "root_attr") == "ok"

            assert list(client.read_table("//sorted_table")) == [{"x": "0", "y": "2"}, {"x": "1", "y": "1"},
                                                                 {"x": "3", "y": "3"}]
            assert client.get_attribute("//sorted_table", "sorted_by") == ["x"]

            schema = client.get_attribute("//table_with_schema", "schema")
            assert schema.attributes == {"strict": False, "unique_keys": True}

            assert len(schema) == 1
            assert schema[0]["sort_order"] == "ascending"
            assert schema[0]["type"] == "int32"
            assert schema[0]["name"] == "x"
            assert not schema[0]["required"]

            assert client.read_file("//file").read() == b"Test file.\n"
            assert client.get_attribute("//file", "myattr") == 4

    def test_preserve_state(self, enable_multidaemon):
        with local_yt(id=_get_id("test_preserve_state"), enable_multidaemon=enable_multidaemon) as environment:
            client = environment.create_client()
            client.write_table("//home/my_table", [{"x": 1, "y": 2, "z": 3}])

        with local_yt(id=environment.id, enable_multidaemon=enable_multidaemon) as environment:
            client = environment.create_client()
            assert list(client.read_table("//home/my_table")) == [{"x": 1, "y": 2, "z": 3}]

    def test_preserve_state_with_tmpfs(self, enable_multidaemon):
        kwargs = dict(tmpfs_path=os.path.join(get_tests_sandbox(), "tmpfs"))
        with local_yt(id=_get_id("test_preserve_state"), enable_multidaemon=enable_multidaemon, **kwargs) as environment:
            client = environment.create_client()
            client.write_table("//home/my_table", [{"x": 1, "y": 2, "z": 3}])

        with local_yt(id=environment.id, enable_multidaemon=enable_multidaemon, **kwargs) as environment:
            client = environment.create_client()
            assert list(client.read_table("//home/my_table")) == [{"x": 1, "y": 2, "z": 3}]

    def test_config_patches_path(self, enable_multidaemon):
        patch = {"test_key": "test_value"}
        try:
            with tempfile.NamedTemporaryFile(dir=get_tests_sandbox(), delete=False) as yson_file:
                yson.dump(patch, yson_file)

            with local_yt(id=_get_id("test_configs_patches"),
                          rpc_proxy_count=1,
                          master_config=yson_file.name,
                          node_config=yson_file.name,
                          scheduler_config=yson_file.name,
                          proxy_config=yson_file.name,
                          rpc_proxy_config=yson_file.name,
                          watcher_config=yson_file.name,
                          enable_multidaemon=enable_multidaemon) as environment:
                for service in ["master", "node", "scheduler", "http_proxy", "rpc_proxy", "watcher"]:
                    if service == "watcher":
                        configs = [environment.watcher_config]
                    else:
                        configs = environment.configs[service]
                    assert configs, "Not configs for service '{}'".format(service)
                    for config in configs:
                        assert config["test_key"] == "test_value"
        finally:
            remove_file(yson_file.name, force=True)

    def test_config_patches_value(self, enable_multidaemon):
        patch = {"test_key": "test_value"}
        with local_yt(id=_get_id("test_configs_patches"),
                      rpc_proxy_count=1,
                      master_config=patch,
                      node_config=patch,
                      scheduler_config=patch,
                      proxy_config=patch,
                      rpc_proxy_config=patch,
                      watcher_config=patch,
                      enable_multidaemon=enable_multidaemon) as environment:
            for service in ["master", "node", "scheduler", "http_proxy", "rpc_proxy", "watcher"]:
                if service == "watcher":
                    configs = [environment.watcher_config]
                else:
                    configs = environment.configs[service]
                assert configs, "Not configs for service '{}'".format(service)
                for config in configs:
                    assert config["test_key"] == "test_value"

    def test_yt_local_binary(self, enable_multidaemon):
        env_id = self.yt_local("start", id=_get_id("test_yt_local_binary_simple"))
        try:
            client = YtClient(proxy=self.yt_local("get_proxy", env_id))
            assert "sys" in client.list("/")
        finally:
            self.yt_local("stop", env_id)

        node_count = 5
        scheduler_count = 2
        master_count = 3

        env_id = self.yt_local(
            "start",
            master_count=master_count,
            node_count=node_count,
            scheduler_count=scheduler_count,
            enable_multidaemon=enable_multidaemon,
            id=_get_id("test_yt_local_binary_with_counts_specified"))

        try:
            client = YtClient(proxy=self.yt_local("get_proxy", env_id))
            assert len(client.list("//sys/cluster_nodes")) == node_count
            assert len(client.list("//sys/scheduler/instances")) == scheduler_count
            assert len(client.list("//sys/primary_masters")) == master_count
        finally:
            self.yt_local("stop", env_id, "--delete")

        patch = {"job_resource_manager": {"resource_limits": {"user_slots": 20}}}
        try:
            with tempfile.NamedTemporaryFile(dir=get_tests_sandbox(), delete=False) as node_config:
                yson.dump(patch, node_config)
            with tempfile.NamedTemporaryFile(dir=get_tests_sandbox(), delete=False) as config:
                yson.dump({"yt_local_test_key": "yt_local_test_value"}, config)

            env_id = self.yt_local(
                "start",
                node_count=1,
                node_config=node_config.name,
                scheduler_count=1,
                scheduler_config=config.name,
                master_count=1,
                master_config=config.name,
                enable_multidaemon=enable_multidaemon,
                id=_get_id("test_yt_local_binary_config_patches"))

            try:
                client = YtClient(proxy=self.yt_local("get_proxy", env_id))
                node_address = client.list("//sys/cluster_nodes")[0]
                wait(lambda: client.get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node_address)) == 20)
                for subpath in ["primary_masters", "scheduler/instances"]:
                    address = client.list("//sys/{0}".format(subpath))[0]
                    assert client.get("//sys/{0}/{1}/orchid/config/yt_local_test_key"
                                      .format(subpath, address)) == "yt_local_test_value"
            finally:
                self.yt_local("stop", env_id)
        finally:
            remove_file(node_config.name, force=True)
            remove_file(config.name, force=True)

    def test_yt_local_binary_config_patches(self, enable_multidaemon):
        patch = {"scheduler": {"cluster_info_logging_period": 1234}}
        try:
            with tempfile.NamedTemporaryFile(dir=get_tests_sandbox(), delete=False) as yson_file:
                yson.dump(patch, yson_file)

            env_id = self.yt_local("start", id=_get_id("test_yt_local_binary_with_configs"), scheduler_config=yson_file.name, enable_multidaemon=enable_multidaemon)
            try:
                client = YtClient(proxy=self.yt_local("get_proxy", env_id))
                assert client.get("//sys/scheduler/orchid/scheduler/config/cluster_info_logging_period") == 1234
            finally:
                self.yt_local("stop", env_id)

            env_id = self.yt_local("start", id=_get_id("test_yt_local_binary_with_configs"), scheduler_config_path=yson_file.name, enable_multidaemon=enable_multidaemon)
            try:
                client = YtClient(proxy=self.yt_local("get_proxy", env_id))
                assert client.get("//sys/scheduler/orchid/scheduler/config/cluster_info_logging_period") == 1234
            finally:
                self.yt_local("stop", env_id)

            env_id = self.yt_local("start", id=_get_id("test_yt_local_binary_with_configs"), scheduler_config=yson._dumps_to_native_str(patch), enable_multidaemon=enable_multidaemon)
            try:
                client = YtClient(proxy=self.yt_local("get_proxy", env_id))
                assert client.get("//sys/scheduler/orchid/scheduler/config/cluster_info_logging_period") == 1234
            finally:
                self.yt_local("stop", env_id)

        finally:
            remove_file(yson_file.name, force=True)

    def test_tablet_cell_initialization(self, enable_multidaemon):
        with local_yt(wait_tablet_cell_initialization=True,
                      id=_get_id("test_tablet_cell_initialization"),
                      enable_multidaemon=enable_multidaemon) as environment:
            client = environment.create_client()
            tablet_cells = client.list("//sys/tablet_cells")
            assert len(tablet_cells) == 1
            assert client.get("//sys/tablet_cells/{0}/@health".format(tablet_cells[0])) == "good"

    def test_rpc_proxy_is_starting(self, enable_multidaemon):
        with local_yt(id=_get_id("test_rpc_proxy_is_starting"), rpc_proxy_count=1, enable_multidaemon=enable_multidaemon) as environment:
            client = environment.create_client()

            for _ in range(6):
                time.sleep(5)

                if len(client.list("//sys/rpc_proxies")) == 1:
                    break
            else:
                assert False, "RPC proxy failed to start in 30 seconds"

    @pytest.mark.skipif(True, reason="st/YT-6227")
    def test_all_processes_are_killed(self, enable_multidaemon):
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
            env_id = generate_uuid()
            process = self.yt_local.run_async("start", sync=True, id=env_id,
                                              sync_mode_sleep_timeout=1, enable_multidaemon=enable_multidaemon)
            _wait_instance_to_become_ready(process, env_id)

            pids = _read_pids_file(env_id)
            assert all(is_process_alive(pid) for pid in pids)
            process.send_signal(sig)
            process.wait()

            start_time = time.time()
            WAIT_TIMEOUT = 10
            while True:
                print("Waiting all YT processes to exit...")

                all_processes_dead = all(is_dead(pid) for pid in pids)
                if all_processes_dead:
                    break

                if time.time() - start_time > WAIT_TIMEOUT:
                    assert False, "Not all processes were killed after {0} seconds".format(WAIT_TIMEOUT)

                time.sleep(1.0)

    def test_ytserver_all(self, enable_multidaemon):
        ytserver_all_path = search_binary_path("ytserver-all", binary_root=get_build_root())
        assert ytserver_all_path

        with local_yt(id=_get_id("ytserver_all"), ytserver_all_path=ytserver_all_path, enable_multidaemon=enable_multidaemon) as environment:
            client = environment.create_client()
            client.get("/")
            client.write_table("//tmp/test_table", [{"a": "b"}])
            client.run_map("cat", "//tmp/test_table", "//tmp/test_table", format=yt.JsonFormat())

    @pytest.mark.skipif("yatest_common is None")
    def test_ports(self, enable_multidaemon):
        from yatest.common.network import PortManager
        with PortManager() as port_manager:
            http_proxy_port = port_manager.get_port()
            rpc_proxy_port = port_manager.get_port()
            with local_yt(id=_get_id("test_ports"), rpc_proxy_count=1, http_proxy_ports=[http_proxy_port], rpc_proxy_ports=[rpc_proxy_port], enable_multidaemon=enable_multidaemon) as environment:
                assert environment.configs["http_proxy"][0]["port"] == http_proxy_port
                assert environment.configs["rpc_proxy"][0]["rpc_port"] == rpc_proxy_port

    def test_structured_logging(self, enable_multidaemon):
        with local_yt(id=_get_id("test_structured_logging"), enable_structured_logging=True, enable_multidaemon=enable_multidaemon) as environment:
            client = environment.create_client()
            client.get("/")
            filename = "http-proxy-0.json.log" if not enable_multidaemon else "multi.json.log"
            wait(lambda: os.path.exists(os.path.join(environment.logs_path, filename)))

    def test_one_node_configuration(self, enable_multidaemon):
        row_count = 100

        def mapper(row):
            yield {"key": row["key"] + 1}

        with local_yt(id=_get_id("one_node_configuration"), node_count=1, enable_multidaemon=enable_multidaemon) as environment:
            client = environment.create_client()
            if yatest_common is None:
                client.config["pickling"]["python_binary"] = sys.executable
            client.get("/")

            rows = [{"key": index} for index in range(row_count)]
            client.write_table("//tmp/test_table", rows)
            assert list(client.read_table("//tmp/test_table")) == rows

            client.run_map(mapper, "//tmp/test_table", "//tmp/output_table", job_count=5)
            client.run_sort("//tmp/output_table", sort_by=["key"])

            result_rows = [{"key": index + 1} for index in range(row_count)]
            assert list(client.read_table("//tmp/output_table")) == result_rows

    def test_components(self, enable_multidaemon):
        with local_yt(id=_get_id("test_components"), components=[{"name": "query_tracker"}], enable_multidaemon=enable_multidaemon) as environment:
            client = environment.create_client()
            assert client.list_queries()["queries"] == []

    def test_auth(self, enable_multidaemon):
        with local_yt(
                id=_get_id("test_auth"),
                enable_auth=True, create_admin_user=True, native_client_supported=True,
                enable_multidaemon=enable_multidaemon
        ) as environment:
            proxy_port = environment.get_proxy_address().rsplit(":", 1)[1]
            client = YtClient(proxy="localhost:{0}".format(proxy_port))
            with pytest.raises((yt.errors.YtTokenError, yt.errors.YtAuthenticationError)):
                client.list("/")

            client.config["token"] = "password"
            client.list("/")

    def test_start_all_components(self, enable_multidaemon):
        if enable_multidaemon:
            master_count = 3
            node_count = 2
            clock_count = 2
            scheduler_count = 2
            controller_agent_count = 2
            discovery_server_count = 2
            queue_agent_count = 2
            kafka_proxy_count = 2
            timestamp_provider_count = 2
            http_proxy_count = 2
            rpc_proxy_count = 2
            master_cache_count = 2
            cell_balancer_count = 0  # TODO(YT-23956): Increase it when it will be fixed.
            tablet_balancer_count = 2
            cypress_proxy_count = 2
            replicated_table_tracker_count = 2
        else:
            # Run only one instance of each component.
            master_count = 1
            node_count = 1
            clock_count = 1
            scheduler_count = 1
            controller_agent_count = 1
            discovery_server_count = 1
            queue_agent_count = 1
            kafka_proxy_count = 1
            timestamp_provider_count = 1
            http_proxy_count = 1
            rpc_proxy_count = 1
            master_cache_count = 1
            cell_balancer_count = 1
            tablet_balancer_count = 1
            cypress_proxy_count = 1
            replicated_table_tracker_count = 1

        with local_yt(id=_get_id("test_start_all_components"),
                      master_count=master_count,
                      node_count=node_count,
                      clock_count=clock_count,
                      scheduler_count=scheduler_count,
                      controller_agent_count=controller_agent_count,
                      discovery_server_count=discovery_server_count,
                      queue_agent_count=queue_agent_count,
                      kafka_proxy_count=kafka_proxy_count,
                      timestamp_provider_count=timestamp_provider_count,
                      http_proxy_count=http_proxy_count,
                      rpc_proxy_count=rpc_proxy_count,
                      master_cache_count=master_cache_count,
                      cell_balancer_count=cell_balancer_count,
                      tablet_balancer_count=tablet_balancer_count,
                      cypress_proxy_count=cypress_proxy_count,
                      replicated_table_tracker_count=replicated_table_tracker_count,
                      enable_multidaemon=enable_multidaemon) as environment:
            expected_pid_count = 2 if enable_multidaemon else 17
            assert len(_read_pids_file(environment.id)) == expected_pid_count
            assert len(environment.configs["master"]) == master_count
