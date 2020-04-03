from .conftest import (
    assert_over_time,
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
    def _create_regular_objects(self, yp_client):
        regular_pod_set_id = create_pod_set(yp_client)
        create_nodes(yp_client, 1)

        return create_pod_with_boilerplate(
            yp_client, regular_pod_set_id, spec={"enable_scheduling": True},
        )

    def _create_daemon_set(self, yp_client, ds_pod_set_id, strong=True):
        return yp_client.create_object(
            object_type="daemon_set",
            attributes={"meta": {"pod_set_id": ds_pod_set_id}, "spec": {"strong": strong}},
        )

    def _create_daemon(self, yp_client, ds_pod_set_id):
        return create_pod_with_boilerplate(
            yp_client, ds_pod_set_id, spec={"enable_scheduling": True},
        )

    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        ds_id = self._create_daemon_set(yp_client, "foo")

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

        self._create_daemon_set(yp_client, ds_pod_set_id)

        regular_pod_id = self._create_regular_objects(yp_client)
        wait(lambda: are_pods_touched_by_scheduler(yp_client, [regular_pod_id]))
        assert not is_pod_assigned(yp_client, regular_pod_id)

        daemon_id = self._create_daemon(yp_client, ds_pod_set_id)

        wait(lambda: is_pod_assigned(yp_client, daemon_id))
        wait(lambda: is_pod_assigned(yp_client, regular_pod_id))

    def test_multiple_daemon_sets(self, yp_env):
        yp_client = yp_env.yp_client
        ds_pod_set1_id = create_pod_set(yp_client)
        ds_pod_set2_id = create_pod_set(yp_client)
        ds_pod_set3_id = create_pod_set(yp_client)

        self._create_daemon_set(yp_client, ds_pod_set1_id)
        self._create_daemon_set(yp_client, ds_pod_set2_id)
        self._create_daemon_set(yp_client, ds_pod_set3_id, strong=False)

        regular_pod_id = self._create_regular_objects(yp_client)
        wait(lambda: are_pods_touched_by_scheduler(yp_client, [regular_pod_id]))
        assert not is_pod_assigned(yp_client, regular_pod_id)

        self._create_daemon(yp_client, ds_pod_set1_id)
        assert_over_time(lambda: not is_pod_assigned(yp_client, regular_pod_id))

        self._create_daemon(yp_client, ds_pod_set2_id)
        wait(lambda: is_pod_assigned(yp_client, regular_pod_id))

    def test_weak_daemon_set(self, yp_env):
        yp_client = yp_env.yp_client
        ds_pod_set_id = create_pod_set(yp_client)

        self._create_daemon_set(yp_client, ds_pod_set_id, strong=False)

        regular_pod_id = self._create_regular_objects(yp_client)
        wait(lambda: is_pod_assigned(yp_client, regular_pod_id))

    def test_unattached_daemon_set(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object(object_type="daemon_set", attributes={"spec": {"strong": True}})

        regular_pod_id = self._create_regular_objects(yp_client)
        wait(lambda: is_pod_assigned(yp_client, regular_pod_id))

    def test_daemon_set_with_bad_pod_set(self, yp_env):
        yp_client = yp_env.yp_client

        ds_id = self._create_daemon_set(yp_client, "bad_pod_set")

        regular_pod_id = self._create_regular_objects(yp_client)
        assert_over_time(lambda: not are_pods_touched_by_scheduler(yp_client, [regular_pod_id]))

        ds_pod_set_id = create_pod_set(yp_client)
        self._create_daemon(yp_client, ds_pod_set_id)
        yp_client.update_object(
            "daemon_set", ds_id, set_updates=[{"path": "/meta/pod_set_id", "value": ds_pod_set_id}]
        )
        wait(lambda: is_pod_assigned(yp_client, regular_pod_id))

    def test_daemon_set_collision(self, yp_env):
        yp_client = yp_env.yp_client

        ds_pod_set_id = create_pod_set(yp_client)
        self._create_daemon_set(yp_client, ds_pod_set_id)
        ds2_id = self._create_daemon_set(yp_client, ds_pod_set_id)
        self._create_daemon(yp_client, ds_pod_set_id)

        regular_pod_id = self._create_regular_objects(yp_client)
        assert_over_time(lambda: not are_pods_touched_by_scheduler(yp_client, [regular_pod_id]))

        ds_pod_set2_id = create_pod_set(yp_client)
        yp_client.update_object(
            "daemon_set",
            ds2_id,
            set_updates=[{"path": "/meta/pod_set_id", "value": ds_pod_set2_id}],
        )
        self._create_daemon(yp_client, ds_pod_set2_id)
        wait(lambda: is_pod_assigned(yp_client, regular_pod_id))

    def test_bad_node_filter(self, yp_env):
        yp_client = yp_env.yp_client

        ds_pod_set_id = create_pod_set(yp_client, spec={"node_filter": "abracadabra"})
        self._create_daemon_set(yp_client, ds_pod_set_id)

        regular_pod_id = self._create_regular_objects(yp_client)
        assert_over_time(lambda: not are_pods_touched_by_scheduler(yp_client, [regular_pod_id]))

        yp_client.update_object(
            "pod_set", ds_pod_set_id, set_updates=[{"path": "/spec/node_filter", "value": "%true"}]
        )
        self._create_daemon(yp_client, ds_pod_set_id)
        wait(lambda: is_pod_assigned(yp_client, regular_pod_id))
