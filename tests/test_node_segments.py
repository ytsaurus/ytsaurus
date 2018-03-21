import pytest

from yp.client import YpResponseError
from yp.client import YpResponseError

@pytest.mark.usefixtures("yp_env")
class TestNodeSegments(object):
    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        segment_id = yp_client.create_object("node_segment", attributes={"spec": {"node_filter": "0=0"}})
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": segment_id}})
        assert yp_client.get_object("pod_set", pod_set_id, selectors=["/spec/node_segment_id"])[0] == segment_id
        assert yp_client.select_objects("pod_set", filter="[/spec/node_segment_id] = \"{}\"".format(segment_id), selectors=["/meta/id"]) == [[pod_set_id]]
        
    def test_cannot_remove_nonempty(self, yp_env):
        yp_client = yp_env.yp_client

        segment_id = yp_client.create_object("node_segment", attributes={"spec": {"node_filter": "0=0"}})
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": segment_id}})
        with pytest.raises(YpResponseError):
            yp_client.remove_object("node_segment", segment_id)
        yp_client.remove_object("pod_set", pod_set_id)
        yp_client.remove_object("node_segment", segment_id)

    def test_pod_set_must_refer_to_valid_node_segment(self, yp_env):
        yp_client = yp_env.yp_client
        
        with pytest.raises(YpResponseError):
            yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": "nonexisting"}})

