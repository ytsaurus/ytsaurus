import pytest

from yp.common import YtResponseError, wait

from yt.yson import YsonEntity

from yt.packages.six.moves import xrange

@pytest.mark.usefixtures("yp_env")
class TestResources(object):
    def test_node_required_on_create(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.create_object("resource", attributes={"spec": {"cpu": {"total_capacity": 100}}})

    def test_kind_required_on_create(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = yp_client.create_object("node")
        with pytest.raises(YtResponseError):
            yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {}})

    def test_kind_set_on_create(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = yp_client.create_object("node")
        cpu_resource_id = yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {"cpu": {"total_capacity": 1}}})
        memory_resource_id = yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {"memory": {"total_capacity": 1}}})
        disk_resource_id = yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {"disk": {"total_capacity": 1000, "storage_class": "hdd", "device": "/dev/null"}}})

        assert yp_client.get_object("resource", cpu_resource_id, selectors=["/meta/kind"])[0] == "cpu"
        assert yp_client.get_object("resource", memory_resource_id, selectors=["/meta/kind"])[0] == "memory"
        assert yp_client.get_object("resource", disk_resource_id, selectors=["/meta/kind"])[0] == "disk"

    def test_cannot_change_kind(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = yp_client.create_object("node")
        resource_id = yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {"cpu": {"total_capacity": 1}}})
        with pytest.raises(YtResponseError):
            yp_client.update_object("resource", resource_id, set_updates=[
                    {"path": "/spec", "value": {"memory": {"total_capacity": 1}}}
                ])

    def test_get(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = yp_client.create_object("node")
        resource_id = yp_client.create_object("resource", attributes={
            "meta": {"node_id": node_id},
            "spec": {"memory": {"total_capacity": 1000}}
        })
        result = yp_client.get_object("resource", resource_id, selectors=[
            "/meta/kind",
            "/spec/memory/total_capacity",
            "/meta/id",
            "/meta/node_id"
        ])
        assert result[0] == "memory"
        assert result[1] == 1000
        assert result[2] == resource_id
        assert result[3] == node_id

    def test_parent_node_must_exist(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.create_object(object_type="resource", attributes={
                "meta": {"node_id": "nonexisting_node_id"}
            })

    def test_create_destroy(self, yp_env):
        yp_client = yp_env.yp_client
        yt_client = yp_env.yt_client

        node_id = yp_client.create_object(object_type="node")
        resource_attributes = {
            "meta": {"node_id": node_id},
            "spec": {"disk": {"total_capacity": 1000, "storage_class": "hdd", "device": "/dev/null"}}
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

    def test_node_segment_totals(self, yp_env):
        yp_client = yp_env.yp_client

        segment_id = yp_client.create_object("node_segment", attributes={"spec": {"node_filter": '[/labels/status] = "good"'}})

        def create_bad_node():
            node_id = yp_client.create_object("node", attributes={"labels": {"status": "bad"}})
            yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {"cpu": {"total_capacity": 100}}})
            return node_id

        def create_good_node():
            node_id = yp_client.create_object("node", attributes={"labels": {"status": "good"}})
            yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {"cpu": {"total_capacity": 1}}})
            yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {"memory": {"total_capacity": 10}}})
            yp_client.create_object("resource", attributes={"meta": {"node_id": node_id}, "spec": {"disk": {"total_capacity": 100, "storage_class": "hdd"}}})
            return node_id

        for _ in xrange(5):
            create_bad_node()

        for _ in xrange(5):
            node_id = create_good_node()

        for _ in xrange(5):
            node_id = create_good_node()
            yp_client.update_object("node", node_id, set_updates=[{
                "path": "/control/update_hfsm_state",
                "value": {"state": "up", "message": "test"}}])

        def _check(totals_field, n):
            totals = yp_client.get_object("node_segment", segment_id, selectors=["/status/" + totals_field])[0]
            return totals != YsonEntity() and \
                   totals["cpu"]["capacity"] == n and \
                   totals["memory"]["capacity"] == n * 10 and \
                   totals["disk_per_storage_class"] == {"hdd": {"capacity": n * 100}}

        wait(lambda: _check("total_resources", 10))
        wait(lambda: _check("schedulable_resources", 5))
