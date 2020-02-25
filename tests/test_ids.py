from yp.common import (
    YpInvalidObjectIdError,
    YpDuplicateObjectIdError,
    YpNoSuchObjectError,
    YpInvalidObjectStateError,
)

import pytest


@pytest.mark.usefixtures("yp_env")
class TestIds(object):
    def test_valid_custom_dns_id(self, yp_env):
        yp_client = yp_env.yp_client

        id = "custom"
        assert yp_client.create_object("pod_set", attributes={"meta": {"id": id}}) == id
        assert yp_client.get_object("pod_set", id, selectors=["/meta/id"]) == [id]

    def test_valid_custom_generic_id(self, yp_env):
        yp_client = yp_env.yp_client

        id = "_."
        assert (
            yp_client.create_object(
                "node", attributes={"meta": {"id": id}, "spec": {"short_name": "short-name"}}
            )
            == id
        )
        assert yp_client.get_object("node", id, selectors=["/meta/id"]) == [id]

    def test_invalid_custom_dns_id(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        for id in ["a" * 2561, " ", "/", "_", "."]:
            with pytest.raises(YpInvalidObjectIdError):
                attributes = {"meta": {"id": id, "pod_set_id": pod_set_id,}}
                yp_client.create_object("pod", attributes=attributes)

    def test_invalid_custom_generic_id(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = yp_client.create_object("node")
        for id in ["a" * 2561, " ", "/", "@"]:
            with pytest.raises(YpInvalidObjectIdError):
                attributes = {
                    "meta": {"id": id, "node_id": node_id,},
                    "spec": {"kind": "cpu", "total_capacity": 100500},
                }
                yp_client.create_object("resource", attributes=attributes)

    def test_duplicate_custom_id(self, yp_env):
        yp_client = yp_env.yp_client

        id = "custom"
        assert yp_client.create_object("pod_set", attributes={"meta": {"id": id}}) == id
        with pytest.raises(YpDuplicateObjectIdError):
            yp_client.create_object("pod_set", attributes={"meta": {"id": id}})

    def test_duplicate_custom_id_in_tx(self, yp_env):
        yp_client = yp_env.yp_client

        tx_id = yp_client.start_transaction()
        id = "custom"
        assert (
            yp_client.create_object(
                "pod_set", attributes={"meta": {"id": id}}, transaction_id=tx_id
            )
            == id
        )
        with pytest.raises(YpInvalidObjectStateError):
            yp_client.create_object(
                "pod_set", attributes={"meta": {"id": id}}, transaction_id=tx_id
            )

    def test_get_missing_object(self, yp_env):
        yp_client = yp_env.yp_client

        for type in ["pod", "pod_set", "node", "resource"]:
            with pytest.raises(YpNoSuchObjectError):
                yp_client.get_object(type, "missing", selectors=[])
