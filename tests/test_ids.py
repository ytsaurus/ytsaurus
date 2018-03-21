import pytest
from yp.client import YpResponseError

@pytest.mark.usefixtures("yp_env")
class TestAnnotations(object):
    def test_valid_custom_dns_id(self, yp_env):
        yp_client = yp_env.yp_client

        id = "custom"
        assert yp_client.create_object("pod_set", attributes={"meta": {"id": id}}) == id
        assert yp_client.get_object("pod_set", id, selectors=["/meta/id"]) == [id]

    def test_valid_custom_generic_id(self, yp_env):
        yp_client = yp_env.yp_client

        id = "_."
        assert yp_client.create_object("node", attributes={"meta": {"id": id}, "spec": {"short_name": "short-name"}}) == id
        assert yp_client.get_object("node", id, selectors=["/meta/id"]) == [id]

    def test_invalid_custom_dns_id(self, yp_env):
        yp_client = yp_env.yp_client
        for id in [" ", "a" * 257, "/", "_", "."]:
            with pytest.raises(YpResponseError):
                yp_client.create_object("pod", attributes={"meta": {"id": id}})

    def test_invalid_custom_generic_id(self, yp_env):
        yp_client = yp_env.yp_client
        for id in [" ", "a" * 257, "/", "@"]:
            with pytest.raises(YpResponseError):
                yp_client.create_object("resource", attributes={"meta": {"id": id}})

    def test_duplicate_custom_id(self, yp_env):
        yp_client = yp_env.yp_client

        id = "custom"
        assert yp_client.create_object("pod_set", attributes={"meta": {"id": id}}) == id
        with pytest.raises(YpResponseError):
            yp_client.create_object("pod_set", attributes={"meta": {"id": id}})

    def test_duplicate_custom_id_in_tx(self, yp_env):
        yp_client = yp_env.yp_client

        tx_id = yp_client.start_transaction()
        id = "custom"
        assert yp_client.create_object("pod_set", attributes={"meta": {"id": id}}, transaction_id=tx_id) == id
        with pytest.raises(YpResponseError):
            yp_client.create_object("pod_set", attributes={"meta": {"id": id}}, transaction_id=tx_id)

    def test_get_missing_object(self, yp_env):
        yp_client = yp_env.yp_client

        for type in ["pod", "pod_set", "node", "resource"]:
            with pytest.raises(YpResponseError):
                yp_client.get_object(type, "missing", selectors=[])
