from yt.local import start, stop, delete
from yt.wrapper.client import Yt
from yt.common import remove_file
import yt.wrapper as yt

import yt.yson as yson
import yt.json as json

import os
import pytest
import tempfile
import contextlib
import subprocess32 as subprocess

TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))
TESTS_SANDBOX = os.environ.get("TESTS_SANDBOX", os.path.join(TESTS_LOCATION, "sandbox"))
YT_LOCAL_BINARY = os.path.join(os.path.dirname(TESTS_LOCATION), "bin", "yt_local")

def _read_pids_file(environment):
    if not os.path.exists(environment.pids_filename):
        return []
    with open(environment.pids_filename) as f:
        return map(int, f)

def _is_exists(environment):
    return os.path.exists(os.path.join(TESTS_SANDBOX, environment.id))

@contextlib.contextmanager
def local_yt(*args, **kwargs):
    environment = None
    try:
        environment = start(*args, **kwargs)
        yield environment
    finally:
        if environment is not None:
            stop(environment.id)

class YtLocalBinary(object):
    def __init__(self, root_path, port_locks_path):
        self.root_path = root_path
        self.port_locks_path = port_locks_path

    def __call__(self, *args, **kwargs):
        args_str = " ".join(args)
        kwargs_str = " ".join("--{0} {1}".format(key.replace("_", "-"), value) for key, value in kwargs.items())
        command = "{0} {1} {2}".format(YT_LOCAL_BINARY, args_str, kwargs_str)
        env = {
            "YT_LOCAL_ROOT_PATH": self.root_path,
            "YT_LOCAL_PORT_LOCKS_PATH": self.port_locks_path,
            "PYTHONPATH": os.environ["PYTHONPATH"],
            "PATH": os.environ["PATH"],
            "YT_LOCAL_USE_PROXY_FROM_SOURCE": "1"
        }
        return subprocess.check_output(command, shell=True, env=env).strip()

class TestLocalMode(object):
    @classmethod
    def setup_class(cls):
        cls.old_yt_local_root_path = os.environ.get("YT_LOCAL_ROOT_PATH", None)
        cls.old_yt_local_use_proxy_from_source = \
                os.environ.get("YT_LOCAL_USE_PROXY_FROM_SOURCE", None)
        os.environ["YT_LOCAL_ROOT_PATH"] = os.path.join(TESTS_SANDBOX, "TestLocalMode")
        os.environ["YT_LOCAL_USE_PROXY_FROM_SOURCE"] = '1'
        # Add ports_lock_path argument to YTEnvironment for parallel testing.
        os.environ["YT_LOCAL_PORT_LOCKS_PATH"] = os.path.join(TESTS_SANDBOX, "ports")
        cls.yt_local = YtLocalBinary(os.environ["YT_LOCAL_ROOT_PATH"],
                                     os.environ["YT_LOCAL_PORT_LOCKS_PATH"])

    @classmethod
    def teardown_class(cls):
        if cls.old_yt_local_root_path is not None:
            os.environ["YT_LOCAL_ROOT_PATH"] = cls.old_yt_local_root_path
        if cls.old_yt_local_use_proxy_from_source is not None:
            os.environ["YT_LOCAL_USE_PROXY_FROM_SOURCE"] = \
                    cls.old_yt_local_use_proxy_from_source
        del os.environ["YT_LOCAL_PORT_LOCKS_PATH"]

    def test_commands_sanity(self):
        with local_yt() as environment:
            pids = _read_pids_file(environment)
            assert len(pids) == 4
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

        with local_yt(master_count=3, node_count=0, scheduler_count=0,
                      enable_debug_logging=True) as environment:
            assert len(_read_pids_file(environment)) == 4  # + proxy
            assert len(environment.configs["master"]) == 3

        with local_yt(node_count=5, scheduler_count=2, start_proxy=False) as environment:
            assert len(environment.configs["node"]) == 5
            assert len(environment.configs["scheduler"]) == 2
            assert len(environment.configs["master"]) == 1
            assert len(_read_pids_file(environment)) == 8
            with pytest.raises(yt.YtError):
                environment.get_proxy_address()

        with local_yt(node_count=1) as environment:
            assert len(_read_pids_file(environment)) == 4  # + proxy

        with local_yt(node_count=0, scheduler_count=0, start_proxy=False) as environment:
            assert len(_read_pids_file(environment)) == 1

    def test_use_local_yt(self):
        with local_yt() as environment:
            proxy_port = environment.get_proxy_address().rsplit(":", 1)[1]
            client = Yt(proxy="localhost:{0}".format(proxy_port))
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

    def test_local_cypress_synchronization(self):
        local_cypress_path = os.path.join(TESTS_LOCATION, "local_cypress_tree")
        with local_yt(local_cypress_dir=local_cypress_path) as environment:
            proxy_port = environment.get_proxy_address().rsplit(":", 1)[1]
            client = Yt(proxy="localhost:{0}".format(proxy_port))
            assert list(client.read_table("//table")) == [{"x": "1", "y": "1"}]
            assert client.get_type("//subdir") == "map_node"
            assert client.get_attribute("//table", "myattr") == 4
            assert client.get_attribute("//subdir", "other_attr") == 42
            assert client.get_attribute("/", "root_attr") == "ok"

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
            with tempfile.NamedTemporaryFile(dir=TESTS_SANDBOX, delete=False) as yson_file:
                yson.dump(patch, yson_file)
            with tempfile.NamedTemporaryFile(dir=TESTS_SANDBOX, delete=False) as json_file:
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
            client = Yt(proxy=self.yt_local("get_proxy", env_id))
            assert "sys" in client.list("/")
        finally:
            self.yt_local("stop", env_id)

        env_id = self.yt_local("start", fqdn="localhost", master_count=3, node_count=5, scheduler_count=2)
        try:
            client = Yt(proxy=self.yt_local("get_proxy", env_id))
            assert len(client.list("//sys/nodes")) == 5
            assert len(client.list("//sys/scheduler/instances")) == 2
            assert len(client.list("//sys/primary_masters")) == 3
        finally:
            self.yt_local("stop", env_id, "--delete")

        patch = {"exec_agent": {"job_controller": {"resource_limits": {"user_slots": 100}}}}
        try:
            with tempfile.NamedTemporaryFile(dir=TESTS_SANDBOX, delete=False) as node_config:
                yson.dump(patch, node_config)
            with tempfile.NamedTemporaryFile(dir=TESTS_SANDBOX, delete=False) as config:
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
                client = Yt(proxy=self.yt_local("get_proxy", env_id))
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
