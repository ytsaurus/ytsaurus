from __future__ import print_function

from yt.local import start, stop, delete
import yt.local as yt_local
from yt.wrapper import YtClient
from yt.common import remove_file, is_process_alive
from yt.wrapper.common import generate_uuid
from yt.environment.helpers import is_dead_or_zombie
import yt.subprocess_wrapper as subprocess

from yt.packages.six.moves import map as imap, xrange
from yt.packages.six import iteritems

import yt.yson as yson
import yt.json as json

import yt.wrapper as yt

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

import os
import sys
import pytest
import shutil
import tempfile
import signal
import contextlib
import time

def _get_tests_location():
    if yatest_common is not None:
        return yatest_common.source_path("yt/python/yt/local/tests")
    return os.path.dirname(os.path.abspath(__file__))

def _get_tests_sandbox():
    if "TESTS_SANDBOX" in os.environ:
        return os.environ["TESTS_SANDBOX"]

    if yatest_common is not None:
        return os.path.join(yatest_common.output_path(), "sandbox")

    return _get_tests_location() + ".sandbox"

def _get_local_mode_tests_sandbox():
    return os.path.join(_get_tests_sandbox(), "TestLocalMode")

def _get_yt_local_binary():
    if yatest_common is not None:
        return yatest_common.binary_path("yt/python/yt/local/bin/yt_local_make/yt_local")
    return os.path.join(os.path.dirname(_get_tests_location()), "bin", "yt_local")

def _get_instance_path(instance_id):
    return os.path.join(_get_local_mode_tests_sandbox(), instance_id)

def _read_pids_file(instance_id):
    pids_filename = os.path.join(_get_instance_path(instance_id), "pids.txt")
    if not os.path.exists(pids_filename):
        return []
    with open(pids_filename) as f:
        return list(imap(int, f))

def _is_exists(environment):
    return os.path.exists(_get_instance_path(environment.id))

def _wait_instance_to_become_ready(process, instance_id):
    special_file = os.path.join(_get_instance_path(instance_id), "started")

    attempt_count = 10
    for _ in xrange(attempt_count):
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
def prepare_path():
    try:
        import yt.environment.arcadia_interop as arcadia_interop
        destination = os.path.join(yatest_common.work_path(), "build")
        os.makedirs(destination)
        path, node_path = arcadia_interop.prepare_yt_environment(destination)
        os.environ["NODE_PATH"] = node_path
        os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])
    except ImportError:
        pass

@contextlib.contextmanager
def local_yt(*args, **kwargs):
    environment = None
    try:
        environment = start(*args, enable_debug_logging=True, **kwargs)
        yield environment
    finally:
        if environment is not None:
            stop(environment.id)

class YtLocalBinary(object):
    def __init__(self, root_path, port_locks_path):
        self.root_path = root_path
        self.port_locks_path = port_locks_path

    def _prepare_binary_command_and_env(self, *args, **kwargs):
        if yatest_common:
            command = [_get_yt_local_binary()]
        else:
            command = [sys.executable, _get_yt_local_binary()]
        command += list(args)

        for key, value in iteritems(kwargs):
            key = key.replace("_", "-")
            if value is True:
                command.extend(["--" + key])
            else:
                command.extend(["--" + key, str(value)])

            command.extend(["--enable-debug-logging"])

        env = {
            "YT_LOCAL_ROOT_PATH": self.root_path,
            "YT_LOCAL_PORT_LOCKS_PATH": self.port_locks_path,
            "PYTHONPATH": os.environ["PYTHONPATH"],
            "PATH": os.environ["PATH"],
        }
        if "NODE_PATH" in os.environ:
            env["NODE_PATH"] = os.environ["NODE_PATH"]
        return command, env

    def __call__(self, *args, **kwargs):
        command, env = self._prepare_binary_command_and_env(*args, **kwargs)
        return subprocess.check_output(command, env=env).strip()

    def run_async(self, *args, **kwargs):
        command, env = self._prepare_binary_command_and_env(*args, **kwargs)
        return subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

class TestLocalMode(object):
    @classmethod
    def setup_class(cls):
        cls.old_yt_local_root_path = os.environ.get("YT_LOCAL_ROOT_PATH", None)
        os.environ["YT_LOCAL_ROOT_PATH"] = _get_local_mode_tests_sandbox()
        # Add ports_lock_path argument to YTEnvironment for parallel testing.
        os.environ["YT_LOCAL_PORT_LOCKS_PATH"] = os.path.join(_get_tests_sandbox(), "ports")
        cls.yt_local = YtLocalBinary(os.environ["YT_LOCAL_ROOT_PATH"],
                                     os.environ["YT_LOCAL_PORT_LOCKS_PATH"])

    @classmethod
    def teardown_class(cls):
        if cls.old_yt_local_root_path is not None:
            os.environ["YT_LOCAL_ROOT_PATH"] = cls.old_yt_local_root_path
        del os.environ["YT_LOCAL_PORT_LOCKS_PATH"]

    def test_logging(self):
        path = os.environ.get("YT_LOCAL_ROOT_PATH")
        log_path = os.path.join(path, "test_logging", "logs")
        if os.path.exists(os.path.join(path, "test_logging")):
            shutil.rmtree(os.path.join(path, "test_logging"), ignore_errors=True)

        master_count = 3
        node_count = 2
        scheduler_count = 4

        with local_yt(id="test_logging", master_count=master_count, node_count=node_count,
                      scheduler_count=scheduler_count, start_proxy=True):
            pass

        for index in xrange(master_count):
            name = "master-0-" + str(index) + ".log"
            assert os.path.exists(os.path.join(log_path, name))

        for index in xrange(node_count):
            name = "node-" + str(index) + ".log"
            assert os.path.exists(os.path.join(log_path, name))

        for index in xrange(scheduler_count):
            name = "scheduler-" + str(index) + ".log"
            assert os.path.exists(os.path.join(log_path, name))

        assert os.path.exists(os.path.join(log_path, "http-application.log"))
        assert os.path.exists(os.path.join(log_path, "http-proxy.log"))

        assert os.path.exists(os.path.join(path, "test_logging", "stderrs"))

    def test_user_config_path(self):
        path = os.environ.get("YT_LOCAL_ROOT_PATH")
        config_path = os.path.join(path, "test_configs", "configs")
        if os.path.exists(os.path.join(path, "test_configs")):
            shutil.rmtree(os.path.join(path, "test_configs"), ignore_errors=True)

        master_count = 3
        node_count = 2
        scheduler_count = 4
        with local_yt(id="test_configs", master_count=master_count,
                      node_count=node_count, scheduler_count=scheduler_count,
                      start_proxy=True, start_rpc_proxy=True):
            pass

        assert os.path.exists(config_path)
        assert os.path.exists(os.path.join(config_path, "driver-0.yson"))

        for index in xrange(master_count):
            name = "master-0-" + str(index) + ".yson"
            assert os.path.exists(os.path.join(config_path, name))

        for index in xrange(node_count):
            name = "node-" + str(index) + ".yson"
            assert os.path.exists(os.path.join(config_path, name))

        for index in xrange(scheduler_count):
            name = "scheduler-" + str(index) + ".yson"
            assert os.path.exists(os.path.join(config_path, name))

        assert os.path.exists(os.path.join(config_path, "proxy.json"))
        assert os.path.exists(os.path.join(config_path, "rpc-client.yson"))

    def test_watcher(self):
        watcher_config = {
            "logs_rotate_size": "1k",
            "logs_rotate_interval": 1,
            "logs_rotate_max_part_count": 5
        }
        with local_yt(id="test_watcher", watcher_config=watcher_config) as environment:
            proxy_port = environment.get_proxy_address().rsplit(":", 1)[1]
            client = YtClient(proxy="localhost:{0}".format(proxy_port))

            for _ in xrange(300):
                client.mkdir("//test")
                client.set("//test/node", "abc")
                client.get("//test/node")
                client.remove("//test/node")
                client.remove("//test")

        path = os.environ.get("YT_LOCAL_ROOT_PATH")
        log_path = os.path.join(path, "test_watcher", "logs")

        # Some log file may be missing if we exited during log rotation.
        presented = 0
        for file_index in xrange(1, 5):
            presented += os.path.exists(os.path.join(log_path, "http-proxy.debug.log.{0}.gz".format(file_index)))
        assert presented in (3, 4)

        presented = 0
        for file_index in xrange(1, 5):
            presented += os.path.exists(os.path.join(log_path, "http-application.log.{0}.gz".format(file_index)))
        assert presented in (3, 4)

    def test_commands_sanity(self):
        with local_yt() as environment:
            pids = _read_pids_file(environment.id)
            assert len(pids) == 5
            # Should not delete running instance
            with pytest.raises(yt.YtError):
                delete(environment.id)
        # Should not stop already stopped instance
        with pytest.raises(yt.YtError):
            stop(environment.id)
        assert not os.path.exists(environment.pids_filename)
        delete(environment.id)
        assert not _is_exists(environment)

    def test_start(self):
        with pytest.raises(yt.YtError):
            start(master_count=0)

        with local_yt(master_count=3, node_count=0, scheduler_count=0) as environment:
            assert len(_read_pids_file(environment.id)) == 5 # + proxy
            assert len(environment.configs["master"]) == 3

        with local_yt(node_count=5, scheduler_count=2, start_proxy=False) as environment:
            assert len(environment.configs["node"]) == 5
            assert len(environment.configs["scheduler"]) == 2
            assert len(environment.configs["master"]) == 1
            assert len(_read_pids_file(environment.id)) == 9
            with pytest.raises(yt.YtError):
                environment.get_proxy_address()

        with local_yt(node_count=1) as environment:
            assert len(_read_pids_file(environment.id)) == 5  # + proxy

        with local_yt(node_count=0, scheduler_count=0, start_proxy=False) as environment:
            assert len(_read_pids_file(environment.id)) == 2

    def test_use_local_yt(self):
        with local_yt() as environment:
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
            assert client.get_type("//test/folder") == "map_node"

            table = "//test/table"
            client.create("table", table)
            client.write_table(table, [{"a": "b"}])
            assert [{"a": "b"}] == list(client.read_table(table))

            assert set(client.search("//test")) == set(["//test", "//test/folder", table])

    def test_use_context_manager(self):
        with yt_local.LocalYt() as client:
            client.config["tabular_data_format"] = yt.format.DsvFormat()
            client.mkdir("//test")

            client.set("//test/node", "abc")
            assert client.get("//test/node") == "abc"
            assert client.list("//test") == ["node"]

            client.remove("//test/node")
            assert not client.exists("//test/node")

            client.mkdir("//test/folder")
            assert client.get_type("//test/folder") == "map_node"

            table = "//test/table"
            client.create("table", table)
            client.write_table(table, [{"a": "b"}])
            assert [{"a": "b"}] == list(client.read_table(table))

            assert set(client.search("//test")) == set(["//test", "//test/folder", table])

        with yt_local.LocalYt(path="test_path"):
            pass

    def test_local_cypress_synchronization(self):
        local_cypress_path = os.path.join(_get_tests_location(), "local_cypress_tree")
        with local_yt(local_cypress_dir=local_cypress_path) as environment:
            proxy_port = environment.get_proxy_address().rsplit(":", 1)[1]
            client = YtClient(proxy="localhost:{0}".format(proxy_port))
            assert list(client.read_table("//table")) == [{"x": "1", "y": "1"}]
            assert client.get_type("//subdir") == "map_node"
            assert client.get_attribute("//table", "myattr") == 4
            assert client.get_attribute("//subdir", "other_attr") == 42
            assert client.get_attribute("/", "root_attr") == "ok"

            assert list(client.read_table("//sorted_table")) == [{"x": "0", "y": "2"}, {"x": "1", "y": "1"},
                                                                 {"x": "3", "y": "3"}]
            assert client.get_attribute("//sorted_table", "sorted_by") == ["x"]

    def test_preserve_state(self):
        with local_yt() as environment:
            client = environment.create_client()
            client.write_table("//home/my_table", [{"x": 1, "y": 2, "z": 3}])

        with local_yt(id=environment.id) as environment:
            client = environment.create_client()
            assert list(client.read_table("//home/my_table")) == [{"x": 1, "y": 2, "z": 3}]

    def test_configs_patches(self):
        patch = {"test_key": "test_value"}
        try:
            with tempfile.NamedTemporaryFile(dir=_get_tests_sandbox(), delete=False) as yson_file:
                yson.dump(patch, yson_file)
            with tempfile.NamedTemporaryFile(mode="w", dir=_get_tests_sandbox(), delete=False) as json_file:
                json.dump(patch, json_file)

            with local_yt(master_config=yson_file.name,
                          node_config=yson_file.name,
                          scheduler_config=yson_file.name,
                          proxy_config=json_file.name) as environment:
                for service in ["master", "node", "scheduler", "proxy"]:
                    if isinstance(environment.configs[service], list):
                        for config in environment.configs[service]:
                            assert config["test_key"] == "test_value"
                    else:  # Proxy config
                        assert environment.configs[service]["test_key"] == "test_value"
        finally:
            remove_file(yson_file.name, force=True)
            remove_file(json_file.name, force=True)

    def test_yt_local_binary(self):
        env_id = self.yt_local("start", fqdn="localhost")
        try:
            client = YtClient(proxy=self.yt_local("get_proxy", env_id))
            assert "sys" in client.list("/")
        finally:
            self.yt_local("stop", env_id)

        env_id = self.yt_local("start", fqdn="localhost", master_count=3, node_count=5, scheduler_count=2)
        try:
            client = YtClient(proxy=self.yt_local("get_proxy", env_id))
            assert len(client.list("//sys/nodes")) == 5
            assert len(client.list("//sys/scheduler/instances")) == 2
            assert len(client.list("//sys/primary_masters")) == 3
        finally:
            self.yt_local("stop", env_id, "--delete")

        patch = {"exec_agent": {"job_controller": {"resource_limits": {"user_slots": 100}}}}
        try:
            with tempfile.NamedTemporaryFile(dir=_get_tests_sandbox(), delete=False) as node_config:
                yson.dump(patch, node_config)
            with tempfile.NamedTemporaryFile(dir=_get_tests_sandbox(), delete=False) as config:
                yson.dump({"yt_local_test_key": "yt_local_test_value"}, config)

            env_id = self.yt_local(
                "start",
                fqdn="localhost",
                node_count=1,
                node_config=node_config.name,
                scheduler_count=1,
                scheduler_config=config.name,
                master_count=1,
                master_config=config.name)

            try:
                client = YtClient(proxy=self.yt_local("get_proxy", env_id))
                node_address = client.list("//sys/nodes")[0]
                assert client.get("//sys/nodes/{0}/@resource_limits/user_slots".format(node_address)) == 100
                for subpath in ["primary_masters", "scheduler/instances"]:
                    address = client.list("//sys/{0}".format(subpath))[0]
                    assert client.get("//sys/{0}/{1}/orchid/config/yt_local_test_key"
                                      .format(subpath, address)) == "yt_local_test_value"
            finally:
                self.yt_local("stop", env_id)
        finally:
            remove_file(node_config.name, force=True)
            remove_file(config.name, force=True)

    def test_tablet_cell_initialization(self):
        with local_yt(wait_tablet_cell_initialization=True) as environment:
            client = environment.create_client()
            tablet_cells = client.list("//sys/tablet_cells")
            assert len(tablet_cells) == 1
            assert client.get("//sys/tablet_cells/{0}/@health".format(tablet_cells[0])) == "good"

    def test_rpc_proxy_is_starting(self):
        with local_yt(start_rpc_proxy=True) as environment:
            client = environment.create_client()

            for _ in range(6):
                time.sleep(5)

                if len(client.list("//sys/rpc_proxies")) == 1:
                    break
            else:
                assert False, "RPC proxy failed to start in 30 seconds"

    @pytest.mark.skipif(True, reason="st/YT-6227")
    def test_all_processes_are_killed(self):
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
            env_id = generate_uuid()
            process = self.yt_local.run_async("start", sync=True, id=env_id,
                                              sync_mode_sleep_timeout=1)
            _wait_instance_to_become_ready(process, env_id)

            pids = _read_pids_file(env_id)
            assert all(is_process_alive(pid) for pid in pids)
            process.send_signal(sig)
            process.wait()

            start_time = time.time()
            WAIT_TIMEOUT = 10
            while True:
                print("Waiting all YT processes to exit...")

                all_processes_dead = all(is_dead_or_zombie(pid) for pid in pids)
                if all_processes_dead:
                    break

                if time.time() - start_time > WAIT_TIMEOUT:
                    assert False, "Not all processes were killed after {0} seconds".format(WAIT_TIMEOUT)

                time.sleep(1.0)
