import pytest
import time

from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.yson import to_yson_type


##################################################################

class TestRff(YTEnvSetup):
    NUM_MASTERS = 5
    NUM_NODES = 3
    DELTA_MASTER_CONFIG = {
        "hydra" : {
            "max_commit_batch_delay" : 1000
        }
    }

    def test_plain_read_table(self):
        set('//tmp/x', 123)
        for i in xrange(100):
            assert get("//tmp/x", read_from="follower") == 123

    def test_sync(self):
        for i in xrange(100):
            set('//tmp/x', i)
            assert get("//tmp/x", read_from="follower") == i

    def test_leader_forwarding1(self):
        assert get("//sys/nodes/@chunk_replicator_enabled", read_from="follower")

    def test_leader_forwarding2(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        chunk_ids = get("//tmp/t/@chunk_ids", read_from="follower")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert get("#" + chunk_id + "/@available", read_from="follower")

    def test_access_stat(self):
        time.sleep(1.0)
        c1 = get("//tmp/@access_counter")
        for i in xrange(100):
            assert ls('//tmp', read_from="follower") == []
        time.sleep(1.0)
        c2 = get("//tmp/@access_counter")
        assert c2 == c1 + 100

    def test_request_stat(self):
        create_user("u")
        assert get("//sys/users/u/@request_counter") == 0
        for i in xrange(100):
            ls("//tmp", user="u", read_from="follower")
        time.sleep(1.0)
        assert get("//sys/users/u/@request_counter") == 100

##################################################################

class TestRffMulticell(TestRff):
    NUM_SECONDARY_MASTER_CELLS = 2
