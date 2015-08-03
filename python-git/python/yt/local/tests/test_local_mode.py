from yt.local import start, stop, delete
from yt.wrapper.client import Yt
import yt.wrapper as yt

import os
import pytest

TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))
TESTS_SANDBOX = os.path.join(TESTS_LOCATION, "sandbox")

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
        os.environ["YT_LOCAL_ROOT_PATH"] = TESTS_SANDBOX
        os.environ["YT_LOCAL_USE_PROXY_FROM_SOURCE"] = '1'

    @classmethod
    def teardown_class(cls):
        if cls.old_yt_local_root_path is not None:
            os.environ["YT_LOCAL_ROOT_PATH"] = cls.old_yt_local_root_path
        if cls.old_yt_local_use_proxy_from_source is not None:
            os.environ["YT_LOCAL_USE_PROXY_FROM_SOURCE"] = \
                    cls.old_yt_local_use_proxy_from_source

    def test_commands_sanity(self):
        environment = start()
        pids = _read_pids_file(environment)
        assert len(pids) == 6
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
            start(masters_count=0)

        environment = start(masters_count=3, nodes_count=0, schedulers_count=0)
        assert len(_read_pids_file(environment)) == 4  # + proxy
        assert len(environment.get_master_addresses()) == 3
        stop(environment.id, remove_working_dir=True)

        environment = start(nodes_count=5, schedulers_count=2, start_proxy=False)
        assert len(environment.get_node_addresses()) == 5
        assert len(environment.get_scheduler_addresses()) == 2
        assert len(environment.get_master_addresses()) == 1
        assert len(_read_pids_file(environment)) == 8
        with pytest.raises(yt.YtError):
            environment.get_proxy_address()
        stop(environment.id, remove_working_dir=True)

        environment = start(nodes_count=1)
        assert len(_read_pids_file(environment)) == 4  # + proxy
        stop(environment.id, remove_working_dir=True)

        environment = start(nodes_count=0, schedulers_count=0, start_proxy=False)
        assert len(_read_pids_file(environment)) == 1
        stop(environment.id, remove_working_dir=True)

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
            client.write_table(table, ["a=b\n"])
            assert "a=b\n" == client.read_table(table, raw=True).read()

            assert set(client.search("//test")) == set(["//test", "//test/folder", table])
        finally:
            stop(environment.id, remove_working_dir=True)

    def test_local_cypress_synchronization(self):
        local_cypress_path = os.path.join(TESTS_LOCATION, "local_cypress_tree")
        environment = start(local_cypress_dir=local_cypress_path)
        try:
            proxy_port = environment.get_proxy_address().rsplit(":", 1)[1]
            client = Yt(proxy="localhost:{0}".format(proxy_port))
            assert list(client.read_table("//table", format="dsv")) == ["x=1\ty=1\n"]
            assert client.get_type("//subdir") == "map_node"
            assert client.get_attribute("//table", "myattr") == 4
            assert client.get_attribute("//subdir", "other_attr") == 42
            assert client.get_attribute("/", "root_attr") == "ok"
        finally:
            stop(environment.id, remove_working_dir=True)

