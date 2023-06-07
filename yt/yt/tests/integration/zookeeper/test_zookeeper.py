from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create_zookeeper_shard, remove_zookeeper_shard, get,
    raises_yt_error, exists)

from kazoo.client import KazooClient

import pytest

##################################################################


class TestZookeeper(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    def _get_client(self):
        port = self.Env.configs["http_proxy"][0]["zookeeper_proxy"]["server"]["port"]
        client = KazooClient(hosts=f'[::1]:{port}')
        client.start()
        return client

    @authors("gritukan")
    def test_shard_attributes(self):
        create_zookeeper_shard("root", "/")
        assert get("//sys/zookeeper_shards/root/@name") == "root"
        assert get("//sys/zookeeper_shards/root/@root_path") == "/"
        assert get("//sys/zookeeper_shards/root/@cell_tag") == 10

        remove_zookeeper_shard("root")
        assert not exists("//sys/zookeeper_shards/root")

        create_zookeeper_shard("root", "/", cell_tag=11)
        assert get("//sys/zookeeper_shards/root/@name") == "root"
        assert get("//sys/zookeeper_shards/root/@root_path") == "/"
        assert get("//sys/zookeeper_shards/root/@cell_tag") == 11

    @authors("gritukan")
    def test_invalid_shard(self):
        create_zookeeper_shard("root", "/")

        with raises_yt_error("Zookeeper shard \"root\" already exists"):
            create_zookeeper_shard("root", "/foo")
        with raises_yt_error("Unknown cell tag 123"):
            create_zookeeper_shard("foo", "/foo", cell_tag=123)
        with raises_yt_error("Zookeeper over YT is not sharded yet"):
            create_zookeeper_shard("foo", "/foo")
        with raises_yt_error("Zookeeper shard \"/\" is already created"):
            create_zookeeper_shard("root2", "/")

    @authors("gritukan")
    def test_start_session(self):
        pytest.skip("Test is disabled until better times")
        client = self._get_client()
        session_id, _ = client.client_id
        assert session_id == 123456
