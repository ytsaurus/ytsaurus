import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import time


##################################################################

class TestSchedulerRemoteCopyCommands(YTEnvSetup):
    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "chunk_scratch_period" : 500,
            "cluster_directory_update_period": 500
        }
    }

    NUM_MASTERS = 3
    NUM_NODES = 9
    NUM_SCHEDULERS = 1

    @classmethod
    def setup_class(cls):
        super(TestSchedulerRemoteCopyCommands, cls).setup_class()
        # Change cell tag of remote cluster
        cls.Env._run_all(masters_count=1, nodes_count=9, schedulers_count=0, has_proxy=False, instance_id="-remote", cell_tag=10)

    def setup(self):
        set("//sys/clusters/remote",
            {
                "connection": {
                    "master": self.Env.configs["master-remote"][0]["master"],
                    "timestamp_provider": self.Env.configs["master-remote"][0]["timestamp_provider"],
                    "transaction_manager": self.Env.configs["master-remote"][0]["transaction_manager"]
                },
                "cell_tag": 10
            })
        self.remote_driver = Driver(config=self.Env.configs["driver-remote"])
        time.sleep(1.0)

    def teardown(self):
        set("//tmp", {}, driver=self.remote_driver)


    def test_empty_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "remote"})

        assert read("//tmp/t2") == []

    def test_non_empty_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "remote"})

        assert read("//tmp/t2") == [{"a": "b"}]

    def test_multi_chunk_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write("<append=true>//tmp/t1", {"a": "b"}, driver=self.remote_driver)
        write("<append=true>//tmp/t1", {"c": "d"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "remote"})

        assert sorted(read("//tmp/t2")) == [{"a": "b"}, {"c": "d"}]
        assert get("//tmp/t2/@chunk_count") == 2

    def test_multiple_jobs(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write("<append=true>//tmp/t1", {"a": "b"}, driver=self.remote_driver)
        write("<append=true>//tmp/t1", {"c": "d"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "remote", "job_count": 2})

        assert sorted(read("//tmp/t2")) == [{"a": "b"}, {"c": "d"}]
        assert get("//tmp/t2/@chunk_count") == 2

    def test_heterogenius_chunk_in_one_job(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write("<append=true>//tmp/t1", {"a": "b"}, driver=self.remote_driver)
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)
        write("<append=true>//tmp/t1", {"c": "d"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "remote"})

        assert read("//tmp/t2") == [{"a": "b"}, {"c": "d"}]

    def test_sorted_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write("//tmp/t1", [{"a": "b"}, {"a": "c"}], sorted_by="a", driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "remote"})

        assert read("//tmp/t2") == [{"a": "b"}, {"a": "c"}]
        assert get("//tmp/t2/@sorted_by") == ["a"]

    def test_erasure_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)
        write("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "remote"})

        assert read("//tmp/t2") == [{"a": "b"}]

    def test_chunk_scratcher(self):
        def set_banned_flag(value):
            if value:
                flag = True
                state = "offline"
            else:
                flag = False
                state = "online"

            address = get("//sys/nodes", driver=self.remote_driver).keys()[0]
            set("//sys/nodes/%s/@banned" % address, flag, driver=self.remote_driver)

            # Give it enough time to register or unregister the node
            time.sleep(1.0)
            assert get("//sys/nodes/%s/@state" % address, driver=self.remote_driver) == state

        create("table", "//tmp/t1", driver=self.remote_driver)
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3", driver=self.remote_driver)
        write("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        set_banned_flag(True)

        time.sleep(1)

        create("table", "//tmp/t2")
        op_id = remote_copy(dont_track=True, in_="//tmp/t1", out="//tmp/t2",
                            spec={"cluster_name": "remote",
                                  "unavailable_chunk_strategy": "wait",
                                  "network_name": "default"})

        time.sleep(1)
        set_banned_flag(False)

        track_op(op_id)

        assert read("//tmp/t2") == [{"a": "b"}]

    def test_revive(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        op_id = remote_copy(dont_track=True, in_="//tmp/t1", out="//tmp/t2",
                            spec={"cluster_name": "remote"})

        self.Env._kill_service("scheduler")
        time.sleep(1)
        self.Env.start_schedulers("scheduler")

        track_op(op_id)

        assert read("//tmp/t2") == [{"a" : "b"}]

    def test_failed_cases(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "unexisting"})

        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "remote", "network_name": "unexisting"})

        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1", out="//tmp/unexisting", spec={"cluster_name": "remote"})

        write("//tmp/t1", [{"a": "b"}, {"c": "d"}], driver=self.remote_driver)
        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1[:#1]", out="//tmp/unexisting", spec={"cluster_name": "remote"})

    def test_acl(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")

        create_user("u")
        create_user("u", driver=self.remote_driver)

        remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "remote"}, user="u")

        set("//tmp/t1/@acl/end", {"action": "deny", "subjects": ["u"], "permissions": ["read"]}, driver=self.remote_driver)
        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "remote"}, user="u")
        set("//tmp/t1/@acl", [], driver=self.remote_driver)

        set("//sys/schemas/transaction/@acl/end", {"action": "deny", "subjects": ["u"], "permissions": ["create"]}, driver=self.remote_driver)
        with pytest.raises(YtError):
            remote_copy(in_="//tmp/t1", out="//tmp/t2", spec={"cluster_name": "remote"}, user="u")
