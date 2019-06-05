from yp.common import YtResponseError

import pytest

@pytest.mark.usefixtures("yp_env")
class TestObjectService(object):
    def test_get_nonexistent(self, yp_env):
        yp_client = yp_env.yp_client

        existent_id = "existent_id"
        nonexistent_id = "nonexistent_id"
        assert yp_client.create_object("pod_set", attributes={"meta": {"id": existent_id}}) == existent_id

        with pytest.raises(YtResponseError):
            yp_client.get_objects("pod_set", [existent_id, nonexistent_id], selectors=["/meta/id"])

        with pytest.raises(YtResponseError):
            yp_client.get_object("pod_set", nonexistent_id, selectors=["/meta/id"])

        objects = yp_client.get_objects("pod_set", [existent_id, nonexistent_id], selectors=["/meta/id"], options={"ignore_nonexistent": True})
        assert len(objects) == 2
        assert len(objects[0]) == 1 and objects[0][0] == existent_id
        assert objects[1] is None

        assert yp_client.get_object("pod_set", nonexistent_id, selectors=["/meta/id"], options={"ignore_nonexistent": True}) is None
