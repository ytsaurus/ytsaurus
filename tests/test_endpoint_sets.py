import pytest


@pytest.mark.usefixtures("yp_env")
class TestEndpointSets(object):
    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        endpoint_set_id = yp_client.create_object("endpoint_set", attributes={"spec": {"port": 1234}})
        assert yp_client.get_object("endpoint_set", endpoint_set_id, selectors=["/meta/id"])[0] == endpoint_set_id
        yp_client.update_object("endpoint_set", endpoint_set_id, set_updates=[{"path": "/spec", "value": {"protocol": "udp"}}])
        yp_client.remove_object("endpoint_set", endpoint_set_id)
        assert yp_client.select_objects("endpoint_set", selectors=["/meta/id"]) == []

    def test_last_endpoints_update_timestamp(self, yp_env):
        yp_client = yp_env.yp_client

        def get_last_endpoints_update_timestamp(endpoint_set_id):
            endpoint_set = yp_client.get_object("endpoint_set", endpoint_set_id, selectors=["/status/last_endpoints_update_timestamp"])
            assert len(endpoint_set) == 1
            return endpoint_set[0]

        transaction = yp_client.start_transaction()
        endpoint_set_id_1 = yp_client.create_object("endpoint_set", attributes=({"spec": {"port": 1234}}), transaction_id=transaction)
        endpoint_set_id_2 = yp_client.create_object("endpoint_set", attributes=({"spec": {"port": 5678}}), transaction_id=transaction)
        timestamp_1 = yp_client.commit_transaction(transaction)["commit_timestamp"]

        assert get_last_endpoints_update_timestamp(endpoint_set_id_1) == timestamp_1
        assert get_last_endpoints_update_timestamp(endpoint_set_id_2) == timestamp_1

        transaction = yp_client.start_transaction()
        endpoint_id = yp_client.create_object("endpoint", attributes={"meta": {"endpoint_set_id": endpoint_set_id_1}}, transaction_id=transaction)
        timestamp_2 = yp_client.commit_transaction(transaction)["commit_timestamp"]

        assert get_last_endpoints_update_timestamp(endpoint_set_id_1) == timestamp_2
        assert get_last_endpoints_update_timestamp(endpoint_set_id_2) == timestamp_1

        transaction = yp_client.start_transaction()
        yp_client.update_object("endpoint", endpoint_id, set_updates=[{"path": "/spec", "value": {}}], transaction_id=transaction)
        timestamp_3 = yp_client.commit_transaction(transaction)["commit_timestamp"]

        assert get_last_endpoints_update_timestamp(endpoint_set_id_1) == timestamp_3
        assert get_last_endpoints_update_timestamp(endpoint_set_id_2) == timestamp_1

        transaction = yp_client.start_transaction()
        yp_client.remove_object("endpoint", endpoint_id, transaction_id=transaction)
        timestamp_4 = yp_client.commit_transaction(transaction)["commit_timestamp"]

        assert get_last_endpoints_update_timestamp(endpoint_set_id_1) == timestamp_4
        assert get_last_endpoints_update_timestamp(endpoint_set_id_2) == timestamp_1
