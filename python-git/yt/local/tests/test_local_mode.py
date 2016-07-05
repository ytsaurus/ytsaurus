from yt.local import start, stop, delete
from yt.wrapper.client import Yt
import yt.wrapper as yt

import yt.yson as yson
import yt.json as json

import os
import pytest
import tempfile

TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))
TESTS_SANDBOX = os.environ.get("TESTS_SANDBOX", os.path.join(TESTS_LOCATION, "sandbox"))

def _read_pids_file(environment):
    if not os.path.exists(environment.pids_filename):
        return []
    with open(environment.pids_filename) as f:
        return map(int, f)

def _is_exists(environment):
    return os.path.exists(os.path.join(TESTS_SANDBOX, environment.id))

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

    @classmethod
    def teardown_class(cls):
        if cls.old_yt_local_root_path is not None:
            os.environ["YT_LOCAL_ROOT_PATH"] = cls.old_yt_local_root_path
        if cls.old_yt_local_use_proxy_from_source is not None:
            os.environ["YT_LOCAL_USE_PROXY_FROM_SOURCE"] = \
                    cls.old_yt_local_use_proxy_from_source
        del os.environ["YT_LOCAL_PORT_LOCKS_PATH"]

    def test_commands_sanity(self):
        environment = start()
        pids = _read_pids_file(environment)
        assert len(pids) == 4
        # Should not delete running instance
        with pytest.raises(yt.YtError):
            delete(environment.id)
        stop(environment.id)
        # Should not stop already stopped instance
        with pytest.raises(yt.YtError):
            stop(environment.id)
        assert not os.path.exists(environment.pids_filename)
        delete(environment.id)
        assert not _is_exists(environment)

    def test_start(self):
        with pytest.raises(yt.YtError):
            start(master_count=0)

        environment = start(master_count=3, node_count=0, scheduler_count=0, enable_debug_logging=True)
        assert len(_read_pids_file(environment)) == 4  # + proxy
        assert len(environment.configs["master"]) == 3
        stop(environment.id)

        environment = start(node_count=5, scheduler_count=2, start_proxy=False)
        assert len(environment.configs["node"]) == 5
        assert len(environment.configs["scheduler"]) == 2
        assert len(environment.configs["master"]) == 1
        assert len(_read_pids_file(environment)) == 8
        with pytest.raises(yt.YtError):
            environment.get_proxy_address()
        stop(environment.id)

        environment = start(node_count=1)
        assert len(_read_pids_file(environment)) == 4  # + proxy
        stop(environment.id)

        environment = start(node_count=0, scheduler_count=0, start_proxy=False)
        assert len(_read_pids_file(environment)) == 1
        stop(environment.id)

    def test_use_local_yt(self):
        environment = start()
        try:
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
        finally:
            stop(environment.id)

    def test_local_cypress_synchronization(self):
        local_cypress_path = os.path.join(TESTS_LOCATION, "local_cypress_tree")
        environment = start(local_cypress_dir=local_cypress_path)
        try:
            proxy_port = environment.get_proxy_address().rsplit(":", 1)[1]
            client = Yt(proxy="localhost:{0}".format(proxy_port))
            assert list(client.read_table("//table")) == [{"x": "1", "y": "1"}]
            assert client.get_type("//subdir") == "map_node"
            assert client.get_attribute("//table", "myattr") == 4
            assert client.get_attribute("//subdir", "other_attr") == 42
            assert client.get_attribute("/", "root_attr") == "ok"
        finally:
            stop(environment.id)

    def test_preserve_state(self):
        environment = start()
        client = environment.create_client()
        client.write_table("//home/my_table", [{"x": 1, "y": 2, "z": 3}])
        stop(environment.id)

        environment = start(id=environment.id)
        client = environment.create_client()
        assert list(client.read_table("//home/my_table")) == [{"x": 1, "y": 2, "z": 3}]

    def test_configs_patches(self):
        patch = {"test_key": "test_value"}
        with tempfile.NamedTemporaryFile(dir=TESTS_SANDBOX, delete=False) as yson_file:
            yson.dump(patch, yson_file)
        with tempfile.NamedTemporaryFile(dir=TESTS_SANDBOX, delete=False) as json_file:
            json.dump(patch, json_file)

        environment = start(
            master_config=yson_file.name,
            node_config=yson_file.name,
            scheduler_config=yson_file.name,
            proxy_config=json_file.name)

        try:
            for service in ["master", "node", "scheduler", "proxy"]:
                if isinstance(environment.configs[service], list):
                    for config in environment.configs[service]:
                        assert config["test_key"] == "test_value"
                else:  # Proxy config
                    assert environment.configs[service]["test_key"] == "test_value"
        finally:
            stop(environment.id)
