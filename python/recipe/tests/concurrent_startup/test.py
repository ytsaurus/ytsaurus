from yp.client import YpClient

import os


def test():
    def validate_instance(address):
        with YpClient(address) as client:
            timestamp = client.generate_timestamp()
            timestamp > 0

            assert [[]] == client.select_objects("pod_set", selectors=["/meta/id"])
            id = client.create_object("pod_set")
            assert [[id]] == client.select_objects("pod_set", selectors=["/meta/id"])

    address = os.environ["YP_MASTER_GRPC_INSECURE_ADDR"]
    address1 = os.environ["YP_MASTER_GRPC_INSECURE_ADDR_1"]
    address2 = os.environ["YP_MASTER_GRPC_INSECURE_ADDR_2"]

    assert "YP_MASTER_GRPC_INSECURE_ADDR_3" not in os.environ
    assert address == address1
    assert address1 != address2

    validate_instance(address1)
    validate_instance(address2)
