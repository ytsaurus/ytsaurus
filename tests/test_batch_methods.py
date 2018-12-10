import pytest


@pytest.mark.usefixtures("yp_env")
class TestBatchMethods(object):
    def test_create_objects(self, yp_env):
        yp_client = yp_env.yp_client
        object_type_attributes_tuple = (
            ("pod_set", dict(meta=dict(id="podset1"))),
            ("pod", dict(meta=dict(id="pod1", pod_set_id="podset1")))
        )
        object_ids = yp_client.create_objects(object_type_attributes_tuple)
        assert object_ids == ["podset1", "pod1"]
        assert yp_client.get_object("pod", "pod1", selectors=["/meta/pod_set_id"])[0] == "podset1"

    def test_create_objects_empty(self, yp_env):
        yp_client = yp_env.yp_client
        assert yp_client.create_objects([]) == []

    def test_create_objects_transaction(self, yp_env):
        yp_client = yp_env.yp_client
        transaction_id = yp_client.start_transaction()
        yp_client.create_object("pod_set", dict(meta=dict(id="podset1")), transaction_id=transaction_id)
        object_type_attributes_tuple = (
            ("pod", dict(meta=dict(id="pod1", pod_set_id="podset1"))),
            ("pod", dict(meta=dict(id="pod2", pod_set_id="podset1"))),
        )
        yp_client.create_objects(object_type_attributes_tuple, transaction_id=transaction_id)
        def select():
            responses = yp_client.select_objects(
                "pod",
                filter="[/meta/pod_set_id] = \"podset1\"",
                selectors=["/meta/id"]
            )
            return list(r[0] for r in responses)
        assert len(select()) == 0
        yp_client.commit_transaction(transaction_id)
        assert set(select()) == set(["pod1", "pod2"])
