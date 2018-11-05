import pytest

from yp.common import YtResponseError
from six.moves import xrange

@pytest.mark.usefixtures("yp_env")
class TestObjects(object):
    def test_uuids(self, yp_env):
        yp_client = yp_env.yp_client

        ids = [yp_client.create_object("pod_set") for _ in xrange(10)]
        uuids = [yp_client.get_object("pod_set", id, selectors=["/meta/uuid"])[0] for id in ids]
        assert len(set(uuids)) == 10

    def test_cannot_change_uuid(self, yp_env):
        yp_client = yp_env.yp_client
        id = yp_client.create_object("pod_set")
        with pytest.raises(YtResponseError):
            yp_client.update_object("pod_set", id, set_updates=[{"path": "/meta/uuid", "value": "1-2-3-4"}])

    def test_names_allowed(self, yp_env):
        yp_client = yp_env.yp_client

        for type in ["account", "group"]:
            yp_client.create_object(type, attributes={"meta": {"name": "some_name"}})

    def test_names_forbidden(self, yp_env):
        yp_client = yp_env.yp_client

        for type in ["pod", "pod_set"]:
            with pytest.raises(YtResponseError):
                yp_client.create_object(type, attributes={"meta": {"name": "some_name"}})
