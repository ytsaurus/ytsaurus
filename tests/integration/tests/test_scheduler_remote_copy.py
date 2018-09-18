import pytest

from yt_env_setup import YTEnvSetup, wait
from yt_commands import *
from yt import yson

import time


##################################################################

class TestSchedulerRemoteCopyCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 9
    NUM_SCHEDULERS = 1

    NUM_REMOTE_CLUSTERS = 1

    NUM_MASTERS_REMOTE_0 = 1
    NUM_SCHEDULERS_REMOTE_0 = 0

    REMOTE_CLUSTER_NAME = "remote_0"

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
        }
    }

    @classmethod
    def setup_class(cls):
        super(TestSchedulerRemoteCopyCommands, cls).setup_class()
        cls.remote_driver = get_driver(cluster=cls.REMOTE_CLUSTER_NAME)

    def test_empty_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

        assert read_table("//tmp/t2") == []
        assert not get("//tmp/t2/@sorted")

    def test_non_empty_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

        assert read_table("//tmp/t2") == [{"a": "b"}]
        assert not get("//tmp/t2/@sorted")

    def test_schema_inference(self):
        schema = make_schema([{"name": "a", "type": "string"}], strict=True, unique_keys=False)

        create("table",
               "//tmp/t1",
               attributes={"schema" : schema},
               driver=self.remote_driver)

        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")
        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

        assert read_table("//tmp/t2") == [{"a": "b"}]
        assert get("//tmp/t2/@schema") == schema
        assert get("//tmp/t2/@schema_mode") == "strong"

        create("table",
               "//tmp/t3",
               attributes={"schema" : [{"name" : "b", "type" : "string"}]})

        with pytest.raises(YtError):
            # To do remote copy into table with "stong" schema mode schemas must be identical.
            remote_copy(in_="//tmp/t1", out="//tmp/t3", spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

        with pytest.raises(YtError):
            # To do remote copy into table with "stong" schema mode schemas must be identical.
            # Even if we force scheduler to infer schema from output.
            remote_copy(
                in_="//tmp/t1",
                out="//tmp/t3",
                spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "schema_inference_mode" : "from_output"})

    def test_cluster_connection_config(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        cluster_connection = get("//sys/clusters/" + self.REMOTE_CLUSTER_NAME)

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_connection": cluster_connection})

        assert read_table("//tmp/t2") == [{"a": "b"}]

    def test_multi_chunk_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("<append=true>//tmp/t1", {"a": "b"}, driver=self.remote_driver)
        write_table("<append=true>//tmp/t1", {"c": "d"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

        assert sorted(read_table("//tmp/t2")) == [{"a": "b"}, {"c": "d"}]
        assert get("//tmp/t2/@chunk_count") == 2

    def test_multi_chunk_sorted_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        for value in xrange(10):
            write_table("<append=true;sorted_by=[a]>//tmp/t1", {"a": value}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "job_count": 10})

        assert read_table("//tmp/t2") == [{"a": value} for value in xrange(10)]
        assert get("//tmp/t2/@chunk_count") == 10

    def test_multiple_jobs(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("<append=true>//tmp/t1", {"a": "b"}, driver=self.remote_driver)
        write_table("<append=true>//tmp/t1", {"c": "d"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "job_count": 2})

        assert sorted(read_table("//tmp/t2")) == [{"a": "b"}, {"c": "d"}]
        assert get("//tmp/t2/@chunk_count") == 2

    def test_heterogenius_chunk_in_one_job(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("<append=true>//tmp/t1", {"a": "b"}, driver=self.remote_driver)
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)
        write_table("<append=true>//tmp/t1", {"c": "d"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

        assert read_table("//tmp/t2") == [{"a": "b"}, {"c": "d"}]

    def test_sorted_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", [{"a": "b"}, {"a": "c"}], sorted_by="a", driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

        assert read_table("//tmp/t2") == [{"a": "b"}, {"a": "c"}]
        assert get("//tmp/t2/@sorted")
        assert get("//tmp/t2/@sorted_by") == ["a"]

    def test_erasure_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

        assert read_table("//tmp/t2") == [{"a": "b"}]

    def test_chunk_scraper(self):
        def set_banned_flag(value):
            if value:
                flag = True
                state = "offline"
            else:
                flag = False
                state = "online"

            address = get("//sys/nodes", driver=self.remote_driver).keys()[0]
            set("//sys/nodes/%s/@banned" % address, flag, driver=self.remote_driver)
            wait(lambda: get("//sys/nodes/%s/@state" % address, driver=self.remote_driver) == state)

        create("table", "//tmp/t1", driver=self.remote_driver)
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        set_banned_flag(True)

        time.sleep(1)

        create("table", "//tmp/t2")
        op = remote_copy(dont_track=True, in_="//tmp/t1", out="//tmp/t2",
                            spec={"cluster_name": self.REMOTE_CLUSTER_NAME,
                                  "unavailable_chunk_strategy": "wait",
                                  "network_name": "interconnect"})

        time.sleep(1)
        set_banned_flag(False)

        op.track()

        assert read_table("//tmp/t2") == [{"a": "b"}]

    def test_revive(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", [{"a" : i} for i in range(100)],
                    max_row_buffer_size=1,
                    table_writer={"desired_chunk_size": 1},
                    driver=self.remote_driver)

        create("table", "//tmp/t2")

        op = remote_copy(dont_track=True, in_="//tmp/t1", out="//tmp/t2",
                         spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

        wait(lambda: op.get_state() == "running")
        wait(lambda: exists(op.get_path() + "/snapshot"))

        input_tx = get(op.get_path() + "/@input_transaction_id")

        self.Env.kill_schedulers()
        self.Env.kill_controller_agents()
        time.sleep(1)
        self.Env.start_schedulers()
        self.Env.start_controller_agents()

        op.track()

        assert input_tx == get(op.get_path() + "/@input_transaction_id")

        assert read_table("//tmp/t2") == [{"a" : i} for i in xrange(100)]

    def test_revive_with_specified_connection(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", [{"a" : i} for i in xrange(100)],
                    max_row_buffer_size=1,
                    table_writer={"desired_chunk_size": 1},
                    driver=self.remote_driver)

        create("table", "//tmp/t2")

        clusters = get("//sys/clusters")
        cluster_connection = clusters[self.REMOTE_CLUSTER_NAME]
        try:
            set("//sys/clusters", {})
            time.sleep(2)
            op = remote_copy(dont_track=True, in_="//tmp/t1", out="//tmp/t2",
                    spec={"cluster_connection": cluster_connection, "job_count": 100})

            wait(lambda: op.get_state() == "running")
            wait(lambda: exists(op.get_path() + "/snapshot"))

            input_tx = get(op.get_path() + "/@input_transaction_id")

            self.Env.kill_schedulers()
            self.Env.kill_controller_agents()
            time.sleep(1)
            self.Env.start_schedulers()
            self.Env.start_controller_agents()

            op.track()

            assert input_tx != get(op.get_path() + "/@input_transaction_id")
        finally:
            set("//sys/clusters", clusters)

        assert read_table("//tmp/t2") ==  [{"a" : i} for i in xrange(100)]

    def test_failed_cases(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "unexisting"})

        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1", out="//tmp/t2",
                        spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "network_name": "unexisting"})

        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1", out="//tmp/unexisting",
                        spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

        write_table("//tmp/t1", [{"a": "b"}, {"c": "d"}], driver=self.remote_driver)
        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1[:#1]", out="//tmp/unexisting",
                        spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

    def test_acl(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")

        create_user("u")
        create_user("u", driver=self.remote_driver)

        remote_copy(in_="//tmp/t1", out="//tmp/t2",
                    spec={"cluster_name": self.REMOTE_CLUSTER_NAME}, authenticated_user="u")

        set("//tmp/t1/@acl/end", make_ace("deny", "u", "read"), driver=self.remote_driver)
        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1", out="//tmp/t2",
                        spec={"cluster_name": self.REMOTE_CLUSTER_NAME}, authenticated_user="u")
        set("//tmp/t1/@acl", [], driver=self.remote_driver)

        set("//sys/schemas/transaction/@acl/end", make_ace("deny", "u", "create"),
            driver=self.remote_driver)
        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1", out="//tmp/t2",
                        spec={"cluster_name": self.REMOTE_CLUSTER_NAME}, authenticated_user="u")

    def test_copy_attributes(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        set("//tmp/t1/@custom_attr1", "attr_value1", driver=self.remote_driver)
        set("//tmp/t1/@custom_attr2", "attr_value2", driver=self.remote_driver)

        remote_copy(in_="//tmp/t1", out="//tmp/t2",
                    spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "copy_attributes": True})

        assert get("//tmp/t2/@custom_attr1") == "attr_value1"
        assert get("//tmp/t2/@custom_attr2") == "attr_value2"

        remote_copy(in_="//tmp/t1", out="//tmp/t3", spec={
            "cluster_name": self.REMOTE_CLUSTER_NAME,
            "copy_attributes": True,
            "attribute_keys": ["custom_attr2"]})

        assert not exists("//tmp/t3/@custom_attr1")
        assert get("//tmp/t3/@custom_attr2") == "attr_value2"

        with pytest.raises(YtError):
            remote_copy(in_=["//tmp/t1", "//tmp/t1"], out="//tmp/t2",
                        spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "copy_attributes": True})

    def test_copy_strict_schema(self):
        create("table", "//tmp/t1", driver=self.remote_driver, attributes={"schema":
            make_schema([
                {"name": "a", "type": "string", "sort_order": "ascending"},
                {"name": "b", "type": "string"}],
                unique_keys=True)
            })
        assert get("//tmp/t1/@schema_mode", driver=self.remote_driver) == "strong"

        create("table", "//tmp/t2")

        rows = [{"a": "x", "b": "v"}, {"a": "y", "b": "v"}]
        write_table("//tmp/t1", rows, driver=self.remote_driver)

        assert get("//tmp/t1/@schema_mode", driver=self.remote_driver) == "strong"

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

        assert read_table("//tmp/t2") == rows
        assert get("//tmp/t2/@schema/@strict")
        assert get("//tmp/t2/@schema/@unique_keys")
        assert get("//tmp/t2/@schema_mode") == "strong"

##################################################################

class TestSchedulerRemoteCopyNetworks(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 9
    NUM_SCHEDULERS = 1

    NUM_REMOTE_CLUSTERS = 1

    NUM_MASTERS_REMOTE_0 = 1
    NUM_SCHEDULERS_REMOTE_0 = 0

    REMOTE_CLUSTER_NAME = "remote_0"

    @classmethod
    def setup_class(cls):
        super(TestSchedulerRemoteCopyNetworks, cls).setup_class()
        cls.remote_driver = get_driver(cluster=cls.REMOTE_CLUSTER_NAME)

    @classmethod
    def modify_node_config(cls, config):
        config["addresses"].append(["custom_network", dict(config["addresses"])["default"]])

    def test_default_network(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": self.REMOTE_CLUSTER_NAME})

        assert read_table("//tmp/t2") == [{"a": "b"}]

    def test_custom_network(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2",
                    spec={"cluster_name": self.REMOTE_CLUSTER_NAME, "network_name": "custom_network"})

        assert read_table("//tmp/t2") == [{"a": "b"}]

##################################################################

class TestSchedulerRemoteCopyCommandsMulticell(TestSchedulerRemoteCopyCommands):
    NUM_SECONDARY_MASTER_CELLS = 2
