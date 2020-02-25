from yp.common import YpNoSuchObjectError

import pytest


@pytest.mark.usefixtures("yp_env")
class TestEndpoints(object):
    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        endpoint_set_id = yp_client.create_object("endpoint_set")
        endpoint_id = yp_client.create_object(
            "endpoint", attributes={"meta": {"endpoint_set_id": endpoint_set_id}}
        )
        assert (
            yp_client.get_object("endpoint", endpoint_id, selectors=["/meta/id"])[0] == endpoint_id
        )
        yp_client.update_object(
            "endpoint", endpoint_id, set_updates=[{"path": "/spec", "value": {}}]
        )
        yp_client.remove_object("endpoint_set", endpoint_set_id)
        assert yp_client.select_objects("endpoint", selectors=["/meta/id"]) == []
        assert yp_client.select_objects("endpoint_set", selectors=["/meta/id"]) == []

    def test_recreate(self, yp_env):
        yp_client = yp_env.yp_client

        endpoint_set_id = yp_client.create_object("endpoint_set")
        assert (
            yp_client.get_object("endpoint_set", endpoint_set_id, selectors=["/meta/id"])[0]
            == endpoint_set_id
        )

        yp_client.remove_object("endpoint_set", endpoint_set_id)
        with pytest.raises(YpNoSuchObjectError):
            yp_client.get_object("endpoint_set", endpoint_set_id, selectors=["/meta/id"])

        yp_client.create_object("endpoint_set", attributes={"meta": {"id": endpoint_set_id}})
        assert (
            yp_client.get_object("endpoint_set", endpoint_set_id, selectors=["/meta/id"])[0]
            == endpoint_set_id
        )

    def test_status(self, yp_env):
        yp_client = yp_env.yp_client

        endpoint_set_id = yp_client.create_object("endpoint_set")
        endpoint_id = yp_client.create_object(
            "endpoint", attributes={"meta": {"endpoint_set_id": endpoint_set_id}}
        )

        for ready in [True, False]:
            yp_client.update_object(
                "endpoint", endpoint_id, set_updates=[{"path": "/status/ready", "value": ready}]
            )
            assert (
                yp_client.get_object("endpoint", endpoint_id, selectors=["/status/ready"])[0]
                == ready
            )

            yp_client.update_object(
                "endpoint",
                endpoint_id,
                set_updates=[{"path": "/status", "value": {"ready": not ready}}],
            )
            assert yp_client.get_object("endpoint", endpoint_id, selectors=["/status/ready"])[
                0
            ] == (not ready)

            endpoint_with_status_id = yp_client.create_object(
                "endpoint",
                attributes={
                    "meta": {"endpoint_set_id": endpoint_set_id},
                    "status": {"ready": ready},
                },
            )
            assert (
                yp_client.get_object(
                    "endpoint", endpoint_with_status_id, selectors=["/status/ready"]
                )[0]
                == ready
            )
