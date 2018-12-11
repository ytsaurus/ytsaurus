from yp.common import YtResponseError

import pytest

@pytest.mark.usefixtures("yp_env")
class TestTransaction(object):
    def test_create(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        id = "some_object_id"
        assert yp_client.create_object("pod_set", attributes={"meta": {"id": id}}, transaction_id=transaction_id) == id
        yp_client.commit_transaction(transaction_id)

        assert len(yp_client.select_objects("pod_set", selectors=["/meta"])) == 1

    def test_create_remove(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        id = "some_object_id"
        assert yp_client.create_object("pod_set", attributes={"meta": {"id": id}}, transaction_id=transaction_id) == id
        yp_client.remove_object("pod_set", id, transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        assert len(yp_client.select_objects("pod_set", selectors=["/meta"])) == 0

    def test_remove_create(self, yp_env):
        yp_client = yp_env.yp_client

        id = "some_object_id"
        old_proj_id = 123
        new_proj_id = 456

        transaction_id = yp_client.start_transaction()
        assert yp_client.create_object("network_project", attributes={"meta": {"id": id}, "spec": {"project_id": old_proj_id}}, transaction_id=transaction_id) == id
        yp_client.commit_transaction(transaction_id)

        assert yp_client.get_object("network_project", id, selectors=["/spec/project_id"])[0] == old_proj_id

        transaction_id = yp_client.start_transaction()
        yp_client.remove_object("network_project", id, transaction_id=transaction_id)
        assert yp_client.create_object("network_project", attributes={"meta": {"id": id}, "spec": {"project_id": new_proj_id}}, transaction_id=transaction_id) == id
        yp_client.commit_transaction(transaction_id)

        assert len(yp_client.select_objects("network_project", selectors=["/meta"])) == 1
        assert yp_client.select_objects("network_project", selectors=["/spec"])[0][0]['project_id'] == new_proj_id
        assert yp_client.get_object("network_project", id, selectors=["/spec/project_id"])[0] == new_proj_id

    def test_many_create_remove(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        id = "some_object_id"

        for proj_id in range(10):
            assert yp_client.create_object("network_project", attributes={"meta": {"id": id}, "spec": {"project_id": proj_id}}, transaction_id=transaction_id) == id
            yp_client.remove_object("network_project", id, transaction_id=transaction_id)


        assert len(yp_client.select_objects("network_project", selectors=["/meta"])) == 0

    def test_many_remove_create(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        id = "some_object_id"
        old_proj_id = 123
        assert yp_client.create_object("network_project", attributes={"meta": {"id": id}, "spec": {"project_id": old_proj_id}}, transaction_id=transaction_id) == id
        yp_client.commit_transaction(transaction_id)

        assert yp_client.get_object("network_project", id, selectors=["/spec/project_id"])[0] == old_proj_id

        transaction_id = yp_client.start_transaction()
        for proj_id in range(10):
            yp_client.remove_object("network_project", id, transaction_id=transaction_id)
            assert yp_client.create_object("network_project", attributes={"meta": {"id": id}, "spec": {"project_id": proj_id}}, transaction_id=transaction_id) == id

        yp_client.commit_transaction(transaction_id)

        assert len(yp_client.select_objects("network_project", selectors=["/meta"])) == 1
        assert yp_client.select_objects("network_project", selectors=["/spec"])[0][0]['project_id'] == 9
        assert yp_client.get_object("network_project", id, selectors=["/spec/project_id"])[0] == 9

    def test_create_remove_remove(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        id = "some_object_id"
        assert yp_client.create_object("pod_set", attributes={"meta": {"id": id}}, transaction_id=transaction_id) == id
        yp_client.remove_object("pod_set", id, transaction_id=transaction_id)

        with pytest.raises(YtResponseError):
            yp_client.remove_object("pod_set", id, transaction_id=transaction_id)

    def test_create_create(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()

        id = "some_object_id"
        assert yp_client.create_object("pod_set", attributes={"meta": {"id": id}}, transaction_id=transaction_id) == id

        with pytest.raises(YtResponseError):
            yp_client.create_object("pod_set", attributes={"meta": {"id": id}}, transaction_id=transaction_id)
