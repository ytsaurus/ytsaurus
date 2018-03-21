import pytest

from yp.client import YpResponseError

@pytest.mark.usefixtures("yp_env")
class TestEndpoints(object):
    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        endpoint_set_id = yp_client.create_object("endpoint_set")
        endpoint_id = yp_client.create_object("endpoint", attributes={"meta": {"endpoint_set_id": endpoint_set_id}})
        assert yp_client.get_object("endpoint", endpoint_id, selectors=["/meta/id"])[0] == endpoint_id
        yp_client.update_object("endpoint", endpoint_id, set_updates=[{"path": "/spec", "value": {}}])
        yp_client.remove_object("endpoint_set", endpoint_set_id)
        assert yp_client.select_objects("endpoint", selectors=["/meta/id"]) == []
        assert yp_client.select_objects("endpoint_set", selectors=["/meta/id"]) == []

    def test_recreate(self, yp_env):
        yp_client = yp_env.yp_client

        endpoint_set_id = yp_client.create_object("endpoint_set")
        assert yp_client.get_object("endpoint_set", endpoint_set_id, selectors=["/meta/id"])[0] == endpoint_set_id

        yp_client.remove_object("endpoint_set", endpoint_set_id)
        with pytest.raises(YpResponseError): yp_client.get_object("endpoint_set", endpoint_set_id, selectors=["/meta/id"])

        yp_client.create_object("endpoint_set", attributes={"meta": {"id": endpoint_set_id}})
        assert yp_client.get_object("endpoint_set", endpoint_set_id, selectors=["/meta/id"])[0] == endpoint_set_id
        