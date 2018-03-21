import pytest

from yp.client import YpResponseError

@pytest.mark.usefixtures("yp_env")
class TestEndpointSets(object):
    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        endpoint_set_id = yp_client.create_object("endpoint_set", attributes={"spec": {"port": 1234}})
        assert yp_client.get_object("endpoint_set", endpoint_set_id, selectors=["/meta/id"])[0] == endpoint_set_id
        yp_client.update_object("endpoint_set", endpoint_set_id, set_updates=[{"path": "/spec", "value": {"protocol": "udp"}}])
        yp_client.remove_object("endpoint_set", endpoint_set_id)
        assert yp_client.select_objects("endpoint_set", selectors=["/meta/id"]) == []
