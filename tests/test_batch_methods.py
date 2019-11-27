from yp.common import YpNoSuchObjectError

from yt.packages.six.moves import xrange

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

    def test_remove_objects_doesnt_change_order(self, yp_env):
        yp_client = yp_env.yp_client

        def _prepare_objects(pod_set_count, pod_count_per_pod_set):
            pod_set_ids = yp_client.create_objects(("pod_set", {}) for _ in xrange(pod_set_count))
            pods_attrs = [
                {"meta": {"pod_set_id": pod_set_id}}
                for pod_set_id in pod_set_ids
                for _ in xrange(pod_count_per_pod_set)
            ]
            pod_ids = yp_client.create_objects(("pod", pod_attrs) for pod_attrs in pods_attrs)

            return pod_set_ids, pod_ids

        POD_SET_COUNT = 4
        POD_COUNT_PER_POD_SET = 5

        pod_set_ids, pod_ids = _prepare_objects(POD_SET_COUNT, POD_COUNT_PER_POD_SET)

        assert all(map(lambda response: response[1] in pod_set_ids, yp_client.get_objects("pod", pod_ids, selectors=["/meta/id", "/meta/pod_set_id"])))

        # There are no pods after removing pod sets associated with pods, so removing pods should raise YpNoSuchObjectError
        with pytest.raises(YpNoSuchObjectError):
            yp_client.remove_objects([("pod_set", pod_set_id) for pod_set_id in pod_set_ids] + [("pod", pod_id) for pod_id in pod_ids])

        # Verify that remove_objects doesn't remove any object as it should be transactional
        assert len(yp_client.select_objects("pod_set", selectors=["/meta/id"])) == POD_SET_COUNT
        assert len(yp_client.select_objects("pod", selectors=["/meta/id"])) == POD_SET_COUNT * POD_COUNT_PER_POD_SET

        yp_client.remove_objects([("pod", pod_id) for pod_id in pod_ids] + [("pod_set", pod_set_id) for pod_set_id in pod_set_ids])

        assert len(yp_client.select_objects("pod_set", selectors=["/meta/id"])) == 0
        assert len(yp_client.select_objects("pod", selectors=["/meta/id"])) == 0

