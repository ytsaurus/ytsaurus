import pytest

from yt.environment.helpers import wait
from yp.client import YpResponseError

@pytest.mark.usefixtures("yp_env_configurable")
class TestSweep(object):
    YP_MASTER_CONFIG = {
        "object_manager": {
            "removed_objects_sweep_period": 1000,
            "removed_objects_grace_timeout": 2000
        }
    }

    def test_nodes(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        yt_client = yp_env_configurable.yt_client

        node_id = yp_client.create_object(object_type="node")
        yp_client.remove_object("node", node_id)
        with pytest.raises(YpResponseError):
            print yp_client.get_object("node", node_id, selectors=["/meta/id"])
        assert len(list(yp_client.select_objects("node", selectors=["/meta/id"], filter="[/meta/id] = \"{}\"".format(node_id)))) == 0
        assert len(list(yt_client.select_rows("* from [//yp/db/nodes] where [meta.id] = \"{}\"".format(node_id)))) == 1

        wait(lambda: len(list(yt_client.select_rows("* from [//yp/db/nodes] where [meta.id] = \"{}\"".format(node_id)))) == 0)

    def test_pods(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        yt_client = yp_env_configurable.yt_client

        pod_set_id = yp_client.create_object(object_type="pod_set")
        pod_id = yp_client.create_object(object_type="pod", attributes={"meta": {"pod_set_id": pod_set_id}})
        yp_client.remove_object("pod", pod_id)
        with pytest.raises(YpResponseError):
            print yp_client.get_object("pod", pod_id, selectors=["/meta/id"])
        assert len(list(yp_client.select_objects("pod", selectors=["/meta/id"], filter="[/meta/id] = \"{}\"".format(pod_id)))) == 0
        assert len(list(yt_client.select_rows("* from [//yp/db/pods] where [meta.pod_set_id] = \"{}\" and [meta.id] = \"{}\"".format(pod_set_id, pod_id)))) == 1

        wait(lambda: len(list(yt_client.select_rows("* from [//yp/db/pods] where [meta.pod_set_id] = \"{}\" and [meta.id] = \"{}\"".format(pod_set_id, pod_id)))) == 0)
