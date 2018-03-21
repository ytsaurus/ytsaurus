import pytest

from yp.client import YpResponseError

@pytest.mark.usefixtures("yp_env")
class TestSelectObjects(object):
    def test_select_limit(self, yp_env):
        yp_client = yp_env.yp_client

        for _ in xrange(10):
            yp_client.create_object("node")

        assert len(yp_client.select_objects("node", selectors=["/meta/id"])) == 10
        assert len(yp_client.select_objects("node", selectors=["/meta/id"], limit=5)) == 5
        assert len(yp_client.select_objects("node", selectors=["/meta/id"], limit=0)) == 0
        with pytest.raises(YpResponseError):
            yp_client.select_objects("node", selectors=["/meta/id"], limit=-10)

    def test_select_offset(self, yp_env):
        yp_client = yp_env.yp_client

        for _ in xrange(10):
            yp_client.create_object("node")

        assert len(yp_client.select_objects("node", selectors=["/meta/id"])) == 10
        assert len(yp_client.select_objects("node", selectors=["/meta/id"], offset=20)) == 0
        assert len(yp_client.select_objects("node", selectors=["/meta/id"], offset=10)) == 0
        assert len(yp_client.select_objects("node", selectors=["/meta/id"], offset=2)) == 8
        assert len(yp_client.select_objects("node", selectors=["/meta/id"], offset=0)) == 10
        with pytest.raises(YpResponseError):
            yp_client.select_objects("node", selectors=["/meta/id"], offset=-10)

    def test_select_paging(self, yp_env):
        yp_client = yp_env.yp_client

        N = 10
        for _ in xrange(N):
            yp_client.create_object("node")

        node_ids = []
        for i in xrange(N):
            result = yp_client.select_objects("node", selectors=["/meta/id"], offset=i, limit=1)
            assert len(result) == 1
            assert len(result[0]) == 1
            node_ids.append(result[0][0])

        assert len(set(node_ids)) == N
