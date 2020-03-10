from yp.common import YtResponseError, YpNoSuchObjectError

from yt.yson import YsonEntity

from yt.packages.six.moves import xrange

import pytest


@pytest.mark.usefixtures("yp_env")
class TestAnnotations(object):
    def test_set_on_create(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object("pod_set", attributes={"annotations": {"hello": "world"}})
        assert yp_client.get_object("pod_set", id, selectors=["/annotations/hello"]) == ["world"]

    def test_update(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object("pod_set", attributes={"annotations": {"hello": "world"}})
        yp_client.update_object(
            "pod_set", id, set_updates=[{"path": "/annotations/a", "value": "v"}]
        )
        yp_client.update_object(
            "pod_set", id, set_updates=[{"path": "/annotations/b", "value": "w"}]
        )
        assert yp_client.get_object(
            "pod_set", id, selectors=["/annotations/a", "/annotations/b"]
        ) == ["v", "w"]
        yp_client.update_object("pod_set", id, remove_updates=[{"path": "/annotations/a"}])
        assert yp_client.get_object(
            "pod_set", id, selectors=["/annotations/a", "/annotations/b"]
        ) == [YsonEntity(), "w"]

    def test_update_in_tx(self, yp_env):
        yp_client = yp_env.yp_client

        tx_id = yp_client.start_transaction()

        id = yp_client.create_object("pod_set", transaction_id=tx_id)
        yp_client.update_object(
            "pod_set",
            id,
            set_updates=[{"path": "/annotations/a", "value": []}],
            transaction_id=tx_id,
        )
        yp_client.update_object(
            "pod_set",
            id,
            set_updates=[{"path": "/annotations/a/end", "value": 1}],
            transaction_id=tx_id,
        )
        yp_client.update_object(
            "pod_set",
            id,
            set_updates=[{"path": "/annotations/a/end", "value": 2}],
            transaction_id=tx_id,
        )

        yp_client.commit_transaction(tx_id)
        assert yp_client.get_object("pod_set", id, selectors=["/annotations/a"]) == [[1, 2]]

    def test_set_recursive(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object("pod_set")
        with pytest.raises(YtResponseError):
            yp_client.update_object(
                "pod_set", id, set_updates=[{"path": "/annotations/a/b", "value": 123}]
            )
        yp_client.update_object(
            "pod_set",
            id,
            set_updates=[{"path": "/annotations/a/b", "value": 123, "recursive": True}],
        )
        assert yp_client.get_object("pod_set", id, selectors=["/annotations/a"]) == [{"b": 123}]

    def test_get_all(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object("pod_set")
        assert yp_client.get_object("pod_set", id, selectors=["/annotations"]) == [{}]

        yp_client.update_object("pod_set", id, set_updates=[{"path": "/annotations/a", "value": 1}])
        assert yp_client.get_object("pod_set", id, selectors=["/annotations"]) == [{"a": 1}]

        yp_client.update_object(
            "pod_set", id, set_updates=[{"path": "/annotations/b", "value": "test"}]
        )
        assert yp_client.get_object("pod_set", id, selectors=["/annotations"]) == [
            {"a": 1, "b": "test"}
        ]

        yp_client.update_object("pod_set", id, set_updates=[{"path": "/labels/l", "value": "x"}])
        full = yp_client.get_object("pod_set", id, selectors=[""])[0]
        assert full["annotations"] == {"a": 1, "b": "test"}
        assert full["meta"]["type"] == "pod_set"
        assert full["meta"]["id"] == id
        assert "spec" in full
        assert "status" in full
        assert full["labels"] == {"l": "x"}

    def test_select_all(self, yp_env):
        yp_client = yp_env.yp_client

        for i in xrange(10):
            yp_client.create_object(
                "pod_set", attributes={"meta": {"id": str(i)}, "annotations": {"a": str(i)}}
            )

        results = yp_client.select_objects(
            "pod_set", selectors=["/meta/id", "/annotations", "/annotations/a"]
        )
        assert len(results) == 10
        for r in results:
            id = r[0]
            assert r[1] == {"a": id}
            assert r[2] == id

    def test_set_all(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object("pod_set", attributes={"annotations": {"hello": "world"}})
        yp_client.update_object(
            "pod_set", id, set_updates=[{"path": "/annotations", "value": {"hello": "yp"}}]
        )
        assert yp_client.get_object("pod_set", id, selectors=["/annotations"]) == [{"hello": "yp"}]

    def test_set_all_in_tx1(self, yp_env):
        yp_client = yp_env.yp_client

        tx_id = yp_client.start_transaction()
        id = yp_client.create_object(
            "pod_set", attributes={"annotations": {"hello": "world"}}, transaction_id=tx_id
        )
        yp_client.update_object(
            "pod_set",
            id,
            set_updates=[{"path": "/annotations/extra", "value": "value"}],
            transaction_id=tx_id,
        )
        yp_client.update_object(
            "pod_set",
            id,
            set_updates=[{"path": "/annotations", "value": {"hello": "yp"}}],
            transaction_id=tx_id,
        )
        yp_client.update_object(
            "pod_set",
            id,
            set_updates=[{"path": "/annotations/extra", "value": "value2"}],
            transaction_id=tx_id,
        )
        yp_client.commit_transaction(tx_id)

        assert yp_client.get_object("pod_set", id, selectors=["/annotations"]) == [
            {"hello": "yp", "extra": "value2"}
        ]

    def test_set_all_in_tx2(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object("pod_set", attributes={"annotations": {"hello": "world"}})
        tx_id = yp_client.start_transaction()
        yp_client.update_object(
            "pod_set",
            id,
            set_updates=[{"path": "/annotations/extra", "value": "value"}],
            transaction_id=tx_id,
        )
        yp_client.update_object(
            "pod_set",
            id,
            set_updates=[{"path": "/annotations", "value": {"hello": "yp"}}],
            transaction_id=tx_id,
        )
        yp_client.update_object(
            "pod_set",
            id,
            set_updates=[{"path": "/annotations/extra", "value": "value2"}],
            transaction_id=tx_id,
        )
        yp_client.commit_transaction(tx_id)

        assert yp_client.get_object("pod_set", id, selectors=["/annotations"]) == [
            {"hello": "yp", "extra": "value2"}
        ]

    def test_remove_create_object_in_tx(self, yp_env):
        yp_client = yp_env.yp_client

        annotations = dict(hello="world")

        def validate(pod_set_id):
            assert (
                annotations
                == yp_client.get_object("pod_set", pod_set_id, selectors=["/annotations"])[0]
            )

        def create_pod_set(pod_set_id, transaction_id=None):
            yp_client.create_object(
                "pod_set",
                attributes=dict(meta=dict(id=pod_set_id), annotations=annotations,),
                transaction_id=transaction_id,
            )

        pod_set_id = "pod_set_id"
        create_pod_set(pod_set_id)
        tx_id = yp_client.start_transaction()
        yp_client.remove_object("pod_set", pod_set_id, transaction_id=tx_id)
        create_pod_set(pod_set_id, tx_id)
        yp_client.commit_transaction(tx_id)
        validate(pod_set_id)

        pod_set_id2 = "pod_set_id2"
        tx_id = yp_client.start_transaction()
        create_pod_set(pod_set_id2, tx_id)
        yp_client.remove_object("pod_set", pod_set_id2, transaction_id=tx_id)
        create_pod_set(pod_set_id2, tx_id)
        yp_client.commit_transaction(tx_id)
        validate(pod_set_id2)

        pod_set_id3 = "pod_set_id3"
        tx_id = yp_client.start_transaction()
        create_pod_set(pod_set_id3, tx_id)
        yp_client.remove_object("pod_set", pod_set_id3, transaction_id=tx_id)
        create_pod_set(pod_set_id3, tx_id)
        yp_client.remove_object("pod_set", pod_set_id3, transaction_id=tx_id)
        yp_client.commit_transaction(tx_id)
        with pytest.raises(YpNoSuchObjectError):
            validate(pod_set_id3)

        pod_set_id4 = "pod_set_id4"
        tx_id = yp_client.start_transaction()
        create_pod_set(pod_set_id4, tx_id)
        yp_client.remove_object("pod_set", pod_set_id4, transaction_id=tx_id)
        create_pod_set(pod_set_id4, tx_id)
        yp_client.remove_object("pod_set", pod_set_id4, transaction_id=tx_id)
        create_pod_set(pod_set_id4, tx_id)
        yp_client.commit_transaction(tx_id)
        validate(pod_set_id4)

    @pytest.mark.parametrize("remove_type", ("by-key", "all"))
    def test_remove_set_in_tx(self, yp_env, remove_type):
        yp_client = yp_env.yp_client

        pod_set_id = "pod_set_id"

        yp_client.create_object(
            "pod_set", attributes=dict(meta=dict(id=pod_set_id), annotations=dict(hello="world"),),
        )

        tx_id = yp_client.start_transaction()
        if remove_type == "by-key":
            yp_client.update_object(
                "pod_set",
                pod_set_id,
                remove_updates=[dict(path="/annotations/hello")],
                transaction_id=tx_id,
            )
        else:
            assert "all" == remove_type
            yp_client.update_object(
                "pod_set",
                pod_set_id,
                set_updates=[dict(path="/annotations", value=dict(),)],
                transaction_id=tx_id,
            )

        yp_client.update_object(
            "pod_set",
            pod_set_id,
            set_updates=[dict(path="/annotations/hello", value="world2",)],
            transaction_id=tx_id,
        )

        yp_client.commit_transaction(tx_id)

        assert (
            "world2"
            == yp_client.get_object("pod_set", pod_set_id, selectors=["/annotations/hello"])[0]
        )
