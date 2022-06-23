from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create)

from yt.test_helpers import assert_items_equal

from kazoo.client import KazooClient

##################################################################


class TestZookeeper(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    def _get_client(self):
        port = self.Env.configs["http_proxy"][0]["zookeeper"]["port"]
        client = KazooClient(hosts=f'[::1]:{port}', read_only=True)
        client.start()
        return client

    @authors("gritukan")
    def test_establish_connection(self):
        self._get_client()

    @authors("gritukan")
    def test_get_children_2(self):
        create("map_node", "//tmp/foo")
        create("map_node", "//tmp/bar")

        client = self._get_client()
        # TODO(gritukan): Check root stats when it will be implemented.
        children = client.get_children("/tmp", include_data=True)[0]
        assert_items_equal(children, ["foo", "bar"])
