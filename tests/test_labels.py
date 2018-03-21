import pytest

from yp.client import YpResponseError

@pytest.mark.usefixtures("yp_env")
class TestLabels(object):
    def test_set_on_create(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object("pod_set", attributes={"labels": {"hello": "world"}})
        assert yp_client.get_object("pod_set", id, selectors=["/labels/hello"]) == ["world"]

    def test_update(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object("pod_set")
        yp_client.update_object("pod_set", id, set_updates=[{"path": "/labels/hello", "value": "world"}])
        assert yp_client.get_object("pod_set", id, selectors=["/labels/hello"]) == ["world"]

        yp_client.update_object("pod_set", id, remove_updates=[{"path": "/labels/hello"}])
        assert yp_client.get_object("pod_set", id, selectors=["/labels/hello"]) == [None]

        yp_client.update_object("pod_set", id, set_updates=[{"path": "/labels/l", "value": []}])
        yp_client.update_object("pod_set", id, set_updates=[{"path": "/labels/l/end", "value": 1}])
        yp_client.update_object("pod_set", id, set_updates=[{"path": "/labels/l/end", "value": 2}])
        yp_client.update_object("pod_set", id, set_updates=[{"path": "/labels/l/end", "value": 3}])
        assert yp_client.get_object("pod_set", id, selectors=["/labels/l"]) == [[1, 2, 3]]

    def test_select(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object("pod_set", attributes={"labels": {"hello": "world", "l": [1, 2, 3]}})
        assert yp_client.select_objects("pod_set", selectors=["/labels/l/0"], filter="[/labels/l/1] = 2") == [[1]]
        assert yp_client.select_objects("pod_set", selectors=["/meta/id"], filter="[/labels/hello] = \"world\"") == [[id]]

    def test_set_recursive(self, yp_env):
        yp_client = yp_env.yp_client

        id = yp_client.create_object("pod_set")
        with pytest.raises(YpResponseError):
            yp_client.update_object("pod_set", id, set_updates=[{"path": "/labels/a/b", "value": 123}])
        yp_client.update_object("pod_set", id, set_updates=[{"path": "/labels/a/b", "value": 123, "recursive": True}])
        assert yp_client.get_object("pod_set", id, selectors=["/labels"]) == [{"a": {"b": 123}}]
