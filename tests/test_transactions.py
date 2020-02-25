from yp.common import (
    YpNoSuchObjectError,
    YtResponseError,
)

import pytest


@pytest.mark.usefixtures("yp_env")
class TestTransaction(object):
    def test_create(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        id = "some_object_id"
        assert (
            yp_client.create_object(
                "pod_set", attributes={"meta": {"id": id}}, transaction_id=transaction_id
            )
            == id
        )
        yp_client.commit_transaction(transaction_id)

        assert len(yp_client.select_objects("pod_set", selectors=["/meta"])) == 1

    def test_create_remove(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        id = "some_object_id"
        assert (
            yp_client.create_object(
                "pod_set", attributes={"meta": {"id": id}}, transaction_id=transaction_id
            )
            == id
        )
        yp_client.remove_object("pod_set", id, transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        assert len(yp_client.select_objects("pod_set", selectors=["/meta"])) == 0

    def test_remove_create(self, yp_env):
        yp_client = yp_env.yp_client

        id = "some_object_id"
        old_proj_id = 123
        new_proj_id = 456

        transaction_id = yp_client.start_transaction()
        assert (
            yp_client.create_object(
                "network_project",
                attributes={"meta": {"id": id}, "spec": {"project_id": old_proj_id}},
                transaction_id=transaction_id,
            )
            == id
        )
        yp_client.commit_transaction(transaction_id)

        assert (
            yp_client.get_object("network_project", id, selectors=["/spec/project_id"])[0]
            == old_proj_id
        )

        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("network_project", id, transaction_id=transaction_id)
        assert (
            yp_client.create_object(
                "network_project",
                attributes={"meta": {"id": id}, "spec": {"project_id": new_proj_id}},
                transaction_id=transaction_id,
            )
            == id
        )
        yp_client.commit_transaction(transaction_id)

        assert len(yp_client.select_objects("network_project", selectors=["/meta"])) == 1
        assert (
            yp_client.select_objects("network_project", selectors=["/spec"])[0][0]["project_id"]
            == new_proj_id
        )
        assert (
            yp_client.get_object("network_project", id, selectors=["/spec/project_id"])[0]
            == new_proj_id
        )

    def test_remove_create_resource(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = yp_client.create_object("node")
        resource_id = "my-resource"

        def create_resource(transaction_id=None):
            yp_client.create_object(
                "resource",
                attributes=dict(
                    meta=dict(id=resource_id, node_id=node_id),
                    spec=dict(cpu=dict(total_capacity=100500)),
                ),
                transaction_id=transaction_id,
            )

        def get_resource_kind():
            return yp_client.get_object("resource", resource_id, selectors=["/meta/kind"],)[0]

        create_resource()

        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("resource", resource_id, transaction_id=transaction_id)
        create_resource(transaction_id)
        yp_client.commit_transaction(transaction_id)

        assert get_resource_kind() == "cpu"

    def test_remove_create_pod(self, yp_env):
        yp_client = yp_env.yp_client

        def run(pod_id, pod_set_id):
            transaction_id = yp_client.start_transaction()
            yp_client.remove_object("pod", pod_id, transaction_id=transaction_id)
            yp_client.create_object(
                "pod",
                attributes=dict(meta=dict(id=pod_id, pod_set_id=pod_set_id)),
                transaction_id=transaction_id,
            )
            yp_client.commit_transaction(transaction_id)
            scheduling, resource_requests = yp_client.get_object(
                "pod", pod_id, selectors=["/status/scheduling", "/spec/resource_requests"],
            )
            assert scheduling["state"] == "disabled" and len(scheduling["message"]) > 0
            assert resource_requests["vcpu_limit"] >= 0 and resource_requests["vcpu_guarantee"] >= 0

        pod_set_id1 = yp_client.create_object("pod_set")
        pod_set_id2 = yp_client.create_object("pod_set")

        pod_id1 = yp_client.create_object("pod", attributes=dict(meta=dict(pod_set_id=pod_set_id1)))
        run(pod_id1, pod_set_id1)

        pod_id2 = yp_client.create_object("pod", attributes=dict(meta=dict(pod_set_id=pod_set_id1)))
        run(pod_id2, pod_set_id2)

    def test_remove_create_endpoint_set(self, yp_env):
        yp_client = yp_env.yp_client

        endpoint_set_id = yp_client.create_object("endpoint_set")
        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("endpoint_set", endpoint_set_id, transaction_id=transaction_id)
        yp_client.create_object(
            "endpoint_set",
            attributes=dict(meta=dict(id=endpoint_set_id)),
            transaction_id=transaction_id,
        )
        yp_client.commit_transaction(transaction_id)

        timestamp = yp_client.get_object(
            "endpoint_set", endpoint_set_id, selectors=["/status/last_endpoints_update_timestamp"]
        )[0]
        assert timestamp > 0

    def test_remove_create_endpoint(self, yp_env):
        yp_client = yp_env.yp_client

        endpoint_set_id = yp_client.create_object("endpoint_set")
        endpoint_id = yp_client.create_object(
            "endpoint", attributes=dict(meta=dict(endpoint_set_id=endpoint_set_id))
        )

        timestamp1 = yp_client.get_object(
            "endpoint_set", endpoint_set_id, selectors=["/status/last_endpoints_update_timestamp"]
        )[0]

        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("endpoint", endpoint_id, transaction_id=transaction_id)
        yp_client.create_object(
            "endpoint",
            attributes=dict(meta=dict(id=endpoint_id, endpoint_set_id=endpoint_set_id)),
            transaction_id=transaction_id,
        )
        yp_client.commit_transaction(transaction_id)

        timestamp2 = yp_client.get_object(
            "endpoint_set", endpoint_set_id, selectors=["/status/last_endpoints_update_timestamp"]
        )[0]
        assert timestamp2 > timestamp1

    def test_remove_child_remove_parent_create_child(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        pod_id = "my-pod"

        def create_pod(transaction_id=None):
            yp_client.create_object(
                "pod",
                attributes=dict(meta=dict(id=pod_id, pod_set_id=pod_set_id),),
                transaction_id=transaction_id,
            )

        create_pod()

        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("pod", pod_id, transaction_id=transaction_id)
        yp_client.remove_object("pod_set", pod_set_id, transaction_id=transaction_id)
        create_pod(transaction_id)
        with pytest.raises(YpNoSuchObjectError):
            yp_client.commit_transaction(transaction_id)

    def test_many_create_remove(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        id = "some_object_id"

        for proj_id in range(10):
            assert (
                yp_client.create_object(
                    "network_project",
                    attributes={"meta": {"id": id}, "spec": {"project_id": proj_id}},
                    transaction_id=transaction_id,
                )
                == id
            )
            yp_client.remove_object("network_project", id, transaction_id=transaction_id)

        assert len(yp_client.select_objects("network_project", selectors=["/meta"])) == 0

    def test_many_remove_create(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        id = "some_object_id"
        old_proj_id = 123
        assert (
            yp_client.create_object(
                "network_project",
                attributes={"meta": {"id": id}, "spec": {"project_id": old_proj_id}},
                transaction_id=transaction_id,
            )
            == id
        )
        yp_client.commit_transaction(transaction_id)

        assert (
            yp_client.get_object("network_project", id, selectors=["/spec/project_id"])[0]
            == old_proj_id
        )

        transaction_id = yp_client.start_transaction()
        for proj_id in range(10):
            yp_client.remove_object("network_project", id, transaction_id=transaction_id)
            assert (
                yp_client.create_object(
                    "network_project",
                    attributes={"meta": {"id": id}, "spec": {"project_id": proj_id}},
                    transaction_id=transaction_id,
                )
                == id
            )

        yp_client.commit_transaction(transaction_id)

        assert len(yp_client.select_objects("network_project", selectors=["/meta"])) == 1
        assert (
            yp_client.select_objects("network_project", selectors=["/spec"])[0][0]["project_id"]
            == 9
        )
        assert yp_client.get_object("network_project", id, selectors=["/spec/project_id"])[0] == 9

    def test_create_remove_remove(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        id = "some_object_id"
        assert (
            yp_client.create_object(
                "pod_set", attributes={"meta": {"id": id}}, transaction_id=transaction_id
            )
            == id
        )
        yp_client.remove_object("pod_set", id, transaction_id=transaction_id)

        with pytest.raises(YtResponseError):
            yp_client.remove_object("pod_set", id, transaction_id=transaction_id)

    def test_create_create(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        id = "some_object_id"
        assert (
            yp_client.create_object(
                "pod_set", attributes={"meta": {"id": id}}, transaction_id=transaction_id
            )
            == id
        )

        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "pod_set", attributes={"meta": {"id": id}}, transaction_id=transaction_id
            )
