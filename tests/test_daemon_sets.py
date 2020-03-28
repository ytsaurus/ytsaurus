from .conftest import (
    are_pods_touched_by_scheduler,
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
    is_pod_assigned,
    wait,
)

import pytest


@pytest.mark.usefixtures("yp_env")
class TestDaemonSets(object):
    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        ds_id = yp_client.create_object(
            object_type="daemon_set",
            attributes={"meta": {"pod_set_id": "foo"}, "spec": {"strong": True}},
        )

        result = yp_client.get_object("daemon_set", ds_id, selectors=["/meta", "/spec"])
        assert result[0]["id"] == ds_id
        assert result[0]["pod_set_id"] == "foo"
        assert result[1]["strong"] == True  # noqa

        yp_client.update_object(
            "daemon_set", ds_id, set_updates=[{"path": "/meta/pod_set_id", "value": "bar"}]
        )

        result = yp_client.get_object("daemon_set", ds_id, selectors=["/meta/pod_set_id"])
        assert result == ["bar"]

    def test_allocation(self, yp_env):
        yp_client = yp_env.yp_client
        ds_pod_set_id = create_pod_set(yp_client)

        yp_client.create_object(
            object_type="daemon_set",
            attributes={"meta": {"pod_set_id": ds_pod_set_id}, "spec": {"strong": True}},
        )

        regular_pod_set_id = create_pod_set(yp_client)
        create_nodes(yp_client, 1)

        regular_pod_id = create_pod_with_boilerplate(
            yp_client, regular_pod_set_id, spec=dict(enable_scheduling=True),
        )
        wait(lambda: are_pods_touched_by_scheduler(yp_client, [regular_pod_id]))
        assert not is_pod_assigned(yp_client, regular_pod_id)

        daemon_id = create_pod_with_boilerplate(
            yp_client, ds_pod_set_id, spec=dict(enable_scheduling=True),
        )

        wait(lambda: is_pod_assigned(yp_client, daemon_id))
        wait(lambda: is_pod_assigned(yp_client, regular_pod_id))
