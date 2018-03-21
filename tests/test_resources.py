import pytest

from yp.client import YpResponseError

@pytest.mark.usefixtures("yp_env")
class TestResources(object):
    def test_node_required_on_create(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YpResponseError): yp_client.create_object(object_type="resource", attributes={"spec": {"kind": "cpu"}})

    def test_get(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = yp_client.create_object("node")
        resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {"kind": "memory", "total_capacity": 1000}
        })
        result = yp_client.get_object("resource", resource_id, selectors=[
            "/spec/kind",
            "/spec/total_capacity",
            "/meta/id",
            "/meta/node_id"
        ])
        assert result[0] == "memory"
        assert result[1] == 1000
        assert result[2] == resource_id
        assert result[3] == node_id

    def test_parent_node_must_exist(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YpResponseError): yp_client.create_object(object_type="resource", attributes={
            "meta": {"node_id": "nonexisting_node_id"}
        })

    def test_create_destroy(self, yp_env):
        yp_client = yp_env.yp_client
        yt_client = yp_env.yt_client

        node_id = yp_client.create_object(object_type="node")
        resource_attributes = {
            "meta": {"node_id": node_id},
            "spec": {"kind": "cpu", "total_capacity": 100}
        }
        resource_ids = [yp_client.create_object(object_type="resource", attributes=resource_attributes)
                        for i in xrange(10)]

        def get_counts():
            return (len(list(yt_client.select_rows("* from [//yp/db/nodes] where is_null([meta.removal_time])"))),
                    len(list(yt_client.select_rows("* from [//yp/db/resources] where is_null([meta.removal_time])"))),
                    len(list(yt_client.select_rows("* from [//yp/db/parents]"))))

        assert get_counts() == (1, 10, 10)

        yp_client.remove_object("resource", resource_ids[0])

        assert get_counts() == (1, 9, 9)

        yp_client.remove_object("node", node_id)

        assert get_counts() == (0, 0, 0)
