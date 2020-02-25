import pytest

from yt.wrapper.errors import YtResponseError


@pytest.mark.usefixtures("yp_env")
class TestEndpointSets(object):
    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        endpoint_set_id = yp_client.create_object(
            "endpoint_set", attributes={"spec": {"port": 1234}}
        )
        assert (
            yp_client.get_object("endpoint_set", endpoint_set_id, selectors=["/meta/id"])[0]
            == endpoint_set_id
        )
        yp_client.update_object(
            "endpoint_set",
            endpoint_set_id,
            set_updates=[{"path": "/spec", "value": {"protocol": "udp"}}],
        )
        yp_client.remove_object("endpoint_set", endpoint_set_id)
        assert yp_client.select_objects("endpoint_set", selectors=["/meta/id"]) == []

    def test_last_endpoints_update_timestamp(self, yp_env):
        yp_client = yp_env.yp_client

        def get_last_endpoints_update_timestamp(endpoint_set_id):
            endpoint_set = yp_client.get_object(
                "endpoint_set",
                endpoint_set_id,
                selectors=["/status/last_endpoints_update_timestamp"],
            )
            assert len(endpoint_set) == 1
            return endpoint_set[0]

        transaction = yp_client.start_transaction()
        endpoint_set_id_1 = yp_client.create_object(
            "endpoint_set", attributes=({"spec": {"port": 1234}}), transaction_id=transaction
        )
        endpoint_set_id_2 = yp_client.create_object(
            "endpoint_set", attributes=({"spec": {"port": 5678}}), transaction_id=transaction
        )
        timestamp_1 = yp_client.commit_transaction(transaction)["commit_timestamp"]

        assert get_last_endpoints_update_timestamp(endpoint_set_id_1) == timestamp_1
        assert get_last_endpoints_update_timestamp(endpoint_set_id_2) == timestamp_1

        transaction = yp_client.start_transaction()
        endpoint_id = yp_client.create_object(
            "endpoint",
            attributes={"meta": {"endpoint_set_id": endpoint_set_id_1}},
            transaction_id=transaction,
        )
        timestamp_2 = yp_client.commit_transaction(transaction)["commit_timestamp"]

        assert get_last_endpoints_update_timestamp(endpoint_set_id_1) == timestamp_2
        assert get_last_endpoints_update_timestamp(endpoint_set_id_2) == timestamp_1

        transaction = yp_client.start_transaction()
        yp_client.update_object(
            "endpoint",
            endpoint_id,
            set_updates=[{"path": "/spec", "value": {}}],
            transaction_id=transaction,
        )
        timestamp_3 = yp_client.commit_transaction(transaction)["commit_timestamp"]

        assert get_last_endpoints_update_timestamp(endpoint_set_id_1) == timestamp_3
        assert get_last_endpoints_update_timestamp(endpoint_set_id_2) == timestamp_1

        transaction = yp_client.start_transaction()
        yp_client.remove_object("endpoint", endpoint_id, transaction_id=transaction)
        timestamp_4 = yp_client.commit_transaction(transaction)["commit_timestamp"]

        assert get_last_endpoints_update_timestamp(endpoint_set_id_1) == timestamp_4
        assert get_last_endpoints_update_timestamp(endpoint_set_id_2) == timestamp_1

    def test_liveness_limit_ratio(self, yp_env):
        yp_client = yp_env.yp_client

        for liveness_limit_ratio in [0.0, 0.111, 0.555, 0.999, 1.0]:
            # test created with correct value
            endpoint_set_id = yp_client.create_object(
                "endpoint_set", attributes={"spec": {"liveness_limit_ratio": liveness_limit_ratio}}
            )
            assert (
                yp_client.get_object(
                    "endpoint_set", endpoint_set_id, selectors=["/spec/liveness_limit_ratio"]
                )[0]
                == liveness_limit_ratio
            )

            # test updated with correct value
            yp_client.update_object(
                "endpoint_set",
                endpoint_set_id,
                set_updates=[{"path": "/spec/liveness_limit_ratio", "value": liveness_limit_ratio}],
            )
            assert (
                yp_client.get_object(
                    "endpoint_set", endpoint_set_id, selectors=["/spec/liveness_limit_ratio"]
                )[0]
                == liveness_limit_ratio
            )

        def assert_exception(func):
            try:
                func()
            except YtResponseError:
                return

            raise AssertionError("Expected exception was not thrown")

        for liveness_limit_ratio in [-100.0, -0.1, 1.1, 100.0]:
            # test created with wrong value
            assert_exception(
                lambda: yp_client.create_object(
                    "endpoint_set",
                    attributes={"spec": {"liveness_limit_ratio": liveness_limit_ratio}},
                )
            )

            # test updated with wrong value
            endpoint_set_id = yp_client.create_object(
                "endpoint_set", attributes={"spec": {"liveness_limit_ratio": 0.0}}
            )
            assert_exception(
                lambda: yp_client.update_object(
                    "endpoint_set",
                    endpoint_set_id,
                    set_updates=[
                        {"path": "/spec/liveness_limit_ratio", "value": liveness_limit_ratio}
                    ],
                )
            )

    def test_status_controller(self, yp_env):
        yp_client = yp_env.yp_client
        endpoint_set_id = yp_client.create_object("endpoint_set")

        assert not yp_client.get_object(
            "endpoint_set", endpoint_set_id, selectors=["/status/controller"]
        )[0]

        def do_check(code, message, attrs, inner_errors):
            yp_client.update_object(
                "endpoint_set",
                endpoint_set_id,
                set_updates=[
                    {
                        "path": "/status/controller",
                        "value": {
                            "error": {
                                "code": code,
                                "message": message,
                                "attributes": {"attributes": attrs,},
                                "inner_errors": inner_errors,
                            }
                        },
                    }
                ],
            )

            status_controller = yp_client.get_object(
                "endpoint_set", endpoint_set_id, selectors=["/status/controller"]
            )[0]
            assert status_controller["error"]["code"] == code
            assert status_controller["error"]["message"] == message
            assert status_controller["error"]["attributes"] == {"attributes": attrs}
            assert status_controller["error"]["inner_errors"] == inner_errors

        do_check(
            code=25,
            message="Hello world!",
            attrs=[{"key": "key1", "value": "bytes1"}, {"key": "key2", "value": "bytes2"}],
            inner_errors=[{"code": 26, "message": "Inner message"}, {"code": 0}],
        )

        do_check(
            code=0,
            message="Good evening!",
            attrs=[{"key": "key2", "value": "bytes2"}],
            inner_errors=[{"code": 26,},],
        )

        yp_client.update_object(
            "endpoint_set",
            endpoint_set_id,
            set_updates=[{"path": "/status/controller", "value": {}}],
        )
        assert (
            {}
            == yp_client.get_object(
                "endpoint_set", endpoint_set_id, selectors=["/status/controller"]
            )[0]
        )
