from .conftest import (
    assert_over_time,
    check_over_time,
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
    get_pod_scheduling_status,
    is_error_pod_scheduling_status,
    update_node_id,
    wait_pod_is_assigned,
)

from yp.common import wait, YtResponseError

from yt.wrapper.errors import YtTabletTransactionLockConflict
from yt.wrapper.retries import run_with_retries

from yt.packages.six.moves import xrange

import pytest


def get_pod_eviction(yp_client, pod_id):
    return yp_client.get_object(
        "pod",
        pod_id,
        selectors=["/status/eviction"],
    )[0]


def get_pod_eviction_state(*args, **kwargs):
    return get_pod_eviction(*args, **kwargs)["state"]


def prepare_objects(yp_client):
    node_id = create_nodes(yp_client, 1)[0]
    pod_set_id = create_pod_set(yp_client)
    pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, dict(enable_scheduling=True))
    wait_pod_is_assigned(yp_client, pod_id)
    return node_id, pod_set_id, pod_id


def with_lock_conflict_retries(action):
    def wrapper(*args, **kwargs):
        return run_with_retries(
            lambda: action(*args, **kwargs),
            exceptions=(YtTabletTransactionLockConflict,),
        )
    return wrapper


@pytest.mark.usefixtures("yp_env")
class TestNodeMaintenanceInterface(object):
    def test_info(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = create_nodes(yp_client, 1)[0]

        def update_once(state, info=None):
            yp_client.update_hfsm_state(
                node_id,
                state,
                "Test",
                maintenance_info=info,
            )

        update = with_lock_conflict_retries(update_once)

        def get_info():
            return yp_client.get_object("node", node_id, selectors=["/status/maintenance/info"])[0]

        update("prepare_maintenance", info=dict(id="123"))
        info = get_info()
        uuid = info["uuid"]
        assert info["id"] == "123"
        # Info uuid is randomly generated.
        assert len(uuid) > 5

        # Info can be specified only for prepare maintenance hfsm state.
        with pytest.raises(YtResponseError):
            update("suspected", info=dict(id="123"))

        # Info uuid is unique.
        update("prepare_maintenance", info=dict(id="123"))
        info = get_info()
        assert info["uuid"] != uuid
        assert len(info["uuid"]) > 5
        assert info["id"] == "123"

        # Info id is randomly generated if not specified.
        update("prepare_maintenance", info=dict(node_set_id="123"))
        info = get_info()
        assert len(info["id"]) > 5
        assert info["node_set_id"] == "123"

        # Info default.
        update("prepare_maintenance")
        info = get_info()
        assert len(info["id"]) > 5
        assert len(info["uuid"]) > 5

        # Info uuid cannot be specified.
        with pytest.raises(YtResponseError):
            update("prepare_maintenance", info=dict(uuid="123"))

    def test_transitions(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = create_nodes(yp_client, 1)[0]

        def update_once(state, info=None):
            yp_client.update_hfsm_state(
                node_id,
                state,
                "Test",
                maintenance_info=info,
            )

        update = with_lock_conflict_retries(update_once)

        def get_info():
            return yp_client.get_object("node", node_id, selectors=["/status/maintenance/info"])[0]

        update("prepare_maintenance", info=dict(id="aba"))
        info = get_info()
        assert info["id"] == "aba"

        update("maintenance")
        assert info == get_info()

        update("up")
        assert get_info() == None


@pytest.mark.usefixtures("yp_env")
class TestNodeAlerts(object):
    def test_api(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = create_nodes(yp_client, 1)[0]

        def get():
            result = yp_client.get_object("node", node_id, selectors=["/status/alerts"])[0]
            return [] if result == None else result

        assert [] == get()

        # Test alert without specified type.
        with pytest.raises(YtResponseError):
            yp_client.add_node_alert(node_id, type="")
        assert [] == get()

        # Test all fields.
        yp_client.add_node_alert(node_id, type="omg", description="description")
        alerts = get()
        assert 1 == len(alerts)
        assert "description" == alerts[0]["description"]
        assert "omg" == alerts[0]["type"]
        assert alerts[0]["creation_time"]["seconds"] > 0
        assert len(alerts[0]["uuid"]) > 0

        # Test alert removing without specified uuid.
        with pytest.raises(YtResponseError):
            yp_client.remove_node_alert(node_id, uuid="")

        # Test alert removing with missing uuid.
        with pytest.raises(YtResponseError):
            yp_client.remove_node_alert(node_id, uuid="123")

        # Test alert removing.
        yp_client.remove_node_alert(node_id, uuid=alerts[0]["uuid"], message="Test")
        assert [] == get()

        # Test several alerts.
        yp_client.add_node_alert(node_id, type="omg1", description="description1")
        yp_client.add_node_alert(node_id, type="omg1", description="description2")
        yp_client.add_node_alert(node_id, type="omg2", description="description3")

        alerts = get()
        assert 3 == len(alerts)

        yp_client.remove_node_alert(node_id, uuid=alerts[1]["uuid"])
        assert 2 == len(get())

    def test_disabled_scheduling(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = create_nodes(yp_client, 1)[0]

        def create_pod():
            pod_set_id = yp_client.create_object("pod_set")
            return create_pod_with_boilerplate(yp_client, pod_set_id, dict(enable_scheduling=True))

        pod_id = create_pod()
        wait_pod_is_assigned(yp_client, pod_id)
        yp_client.remove_object("pod", pod_id)

        yp_client.add_node_alert(node_id, type="omg")

        pod_id = create_pod()
        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

        alert_uuid = yp_client.get_object("node", node_id, selectors=["/status/alerts/0/uuid"])[0]
        yp_client.remove_node_alert(node_id, alert_uuid)

        wait_pod_is_assigned(yp_client, pod_id)

    def test_pod_sync(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = create_nodes(yp_client, 1)[0]

        pod_set_id = yp_client.create_object("pod_set")
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, dict(enable_scheduling=True))
        wait_pod_is_assigned(yp_client, pod_id)

        def get_node_alerts():
            result = yp_client.get_object("node", node_id, selectors=["/status/alerts"])[0]
            return [] if result == None else result

        def get_pod_alerts(pod_id):
            result = yp_client.get_object("pod", pod_id, selectors=["/status/node_alerts"])[0]
            return [] if result == None else result

        def get_pod_alerts_timestamp():
            return yp_client.get_object(
                "pod",
                pod_id,
                selectors=["/status/node_alerts"],
                options=dict(fetch_timestamps=True),
                enable_structured_response=True,
            )["result"][0]["timestamp"]

        assert [] == get_pod_alerts(pod_id)
        assert [] == get_node_alerts()

        for i in xrange(2):
            yp_client.add_node_alert(node_id, "omg")
            node_alerts = get_node_alerts()
            assert i + 1 == len(node_alerts)
            wait(lambda: node_alerts == get_pod_alerts(pod_id))

        # No zero-diff updates.
        timestamp = get_pod_alerts_timestamp()
        assert_over_time(lambda: timestamp == get_pod_alerts_timestamp())

        all_node_alerts = get_node_alerts()
        for i in xrange(len(all_node_alerts)):
            yp_client.remove_node_alert(node_id, all_node_alerts[i]["uuid"])
            node_alerts = get_node_alerts()
            assert len(all_node_alerts) - i - 1 == len(node_alerts)
            wait(lambda: node_alerts == get_pod_alerts(pod_id))

        yp_client.add_node_alert(node_id, "omg")
        wait(lambda: 1 == len(get_pod_alerts(pod_id)))

        # Pod node alerts are reset due to pod assignment change.
        yp_client.request_pod_eviction(pod_id, "Test")
        yp_client.acknowledge_pod_eviction(pod_id, "Test")

        wait(lambda: "" == yp_client.get_object("pod", pod_id, selectors=["/spec/node_id"])[0])

        assert [] == get_pod_alerts(pod_id)

        # Pod with disabled scheduling.
        pod_id2 = create_pod_with_boilerplate(yp_client, pod_set_id)
        update_node_id(yp_client, pod_id2, node_id)
        wait(lambda: 1 == len(get_pod_alerts(pod_id2)))
        yp_client.add_node_alert(node_id, "omg")
        wait(lambda: 2 == len(get_pod_alerts(pod_id2)))


@pytest.mark.usefixtures("yp_env_configurable")
class TestPodMaintenanceController(object):
    # Choosing a pretty small period to optimize tests duration.
    SCHEDULER_LOOP_PERIOD_MILLISECONDS = 1 * 1000

    YP_MASTER_CONFIG = dict(
        scheduler=dict(
            loop_period=SCHEDULER_LOOP_PERIOD_MILLISECONDS,
        )
    )

    def test_abort_requested_eviction_at_up_nodes(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        node_id, _, pod_id = prepare_objects(yp_client)
        yp_client.update_hfsm_state(node_id, "prepare_maintenance", "Test")
        wait(lambda: get_pod_eviction_state(yp_client, pod_id) == "requested")
        yp_client.update_hfsm_state(node_id, "up", "Test")
        wait(lambda: get_pod_eviction_state(yp_client, pod_id) == "none")

    def test_scheduler_disable_stage_reconfiguration(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        node_id, _, pod_id = prepare_objects(yp_client)

        def is_controller_disabled():
            yp_client.update_hfsm_state(node_id, "prepare_maintenance", "Test")
            result = check_over_time(
                lambda: get_pod_eviction_state(yp_client, pod_id) == "none",
                iter=20,
                sleep_backoff=1,
            )
            if not result:
                # Rollback.
                assert get_pod_eviction_state(yp_client, pod_id) == "requested"
                transaction_id = yp_client.start_transaction()
                yp_client.update_hfsm_state(node_id, "up", "Test", transaction_id=transaction_id)
                yp_client.abort_pod_eviction(pod_id, "Test", transaction_id=transaction_id)
                yp_client.commit_transaction(transaction_id)
            return result

        # Test that pod maintenance controller works fine.
        assert not is_controller_disabled()

        yp_env_configurable.set_cypress_config_patch(dict(scheduler=dict(disable_stage=dict(
            run_pod_maintenance_request_eviction=True,
        ))))

        wait(is_controller_disabled, iter=5, sleep_backoff=10)

        # Sanity check.
        assert get_pod_scheduling_status(yp_client, pod_id)["node_id"] == node_id

    def test_sync_node_maintenance(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        node_id, pod_set_id, pod_id = prepare_objects(yp_client)

        def get_maintenance(pod_id):
            return yp_client.get_object(
                "pod",
                pod_id,
                selectors=["/status/maintenance"],
            )[0]

        yp_client.update_hfsm_state(node_id, "prepare_maintenance", "Test", maintenance_info=dict(id="first"))
        def is_requested_first_maintenance():
            maintenance = get_maintenance(pod_id)
            return maintenance["state"] == "requested" and maintenance["info"]["id"] == "first"
        wait(is_requested_first_maintenance)

        yp_client.update_hfsm_state(node_id, "prepare_maintenance", "Test", maintenance_info=dict(id="second"))
        def is_requested_second_maintenance():
            maintenance = get_maintenance(pod_id)
            return maintenance["state"] == "requested" and maintenance["info"]["id"] == "second"
        wait(is_requested_second_maintenance)

        second_maintenance_info = get_maintenance(pod_id)["info"]

        yp_client.acknowledge_pod_maintenance(pod_id, "Test")

        acknowledged_maintenance = get_maintenance(pod_id)
        assert second_maintenance_info == acknowledged_maintenance["info"]
        assert "acknowledged" == acknowledged_maintenance["state"]

        wait(lambda: yp_client.get_object("node", node_id, selectors=["/status/maintenance/state"])[0] == "acknowledged")
        assert_over_time(lambda: acknowledged_maintenance == get_maintenance(pod_id))

        yp_client.update_hfsm_state(node_id, "maintenance", "Test")
        def is_in_progress_maintenance():
            maintenance = get_maintenance(pod_id)
            return maintenance["state"] == "in_progress" and maintenance["info"] == second_maintenance_info
        wait(is_in_progress_maintenance)

        yp_client.update_hfsm_state(node_id, "up", "Test")
        def is_none_maintenance():
            maintenance = get_maintenance(pod_id)
            return maintenance["state"] == "none" and maintenance.get("info") is None
        wait(is_none_maintenance)

        # Pod with disabled scheduling.
        pod_id2 = create_pod_with_boilerplate(yp_client, pod_set_id)
        update_node_id(yp_client, pod_id2, node_id)
        yp_client.update_hfsm_state(node_id, "prepare_maintenance", "Test", maintenance_info=dict(id="abacaba"))
        wait(lambda: "abacaba" == get_maintenance(pod_id2).get("info", {}).get("id"))


@pytest.mark.usefixtures("yp_env")
class TestPodMaintenanceInterface(object):
    def test_acknowledge_renounce(self, yp_env):
        yp_client = yp_env.yp_client

        node_id, pod_set_id, pod_id = prepare_objects(yp_client)

        # Another pod is used for blocking maintenance acknowledgement on node.
        another_pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, dict(enable_scheduling=True))
        wait_pod_is_assigned(yp_client, another_pod_id)

        def acknowledge(transaction_id=None):
            yp_client.acknowledge_pod_maintenance(pod_id, "Test", transaction_id=transaction_id)

        def renounce(transaction_id=None):
            yp_client.renounce_pod_maintenance(pod_id, "Test", transaction_id=transaction_id)

        # Acknowledgement of none maintenance.
        with pytest.raises(YtResponseError):
            acknowledge()

        # Renouncement of none maintenance.
        with pytest.raises(YtResponseError):
            renounce()

        yp_client.update_hfsm_state(
            node_id,
            "prepare_maintenance",
            "Test",
            maintenance_info=dict(id="aba"),
        )

        def get_maintenance():
            return yp_client.get_object(
                "pod",
                pod_id,
                selectors=["/status/maintenance"],
            )[0]

        def get_node_maintenance_state():
            return yp_client.get_object(
                "node",
                node_id,
                selectors=["/status/maintenance/state"],
            )[0]

        wait(lambda: get_maintenance().get("state") == "requested")

        def compare_dicts(lhs, rhs, fields):
            for field in fields:
                if (field in lhs) != (field in rhs) or (lhs.get(field) != rhs.get(field)):
                    return False
            return True

        maintenance = get_maintenance()
        assert maintenance["info"]["id"] == "aba"
        assert len(maintenance["info"]["uuid"]) > 0

        # Renouncement of requested maintenance.
        with pytest.raises(YtResponseError):
            renounce()

        # Acknowledgement + renouncement in one transaction.
        transaction_id = yp_client.start_transaction()
        acknowledge(transaction_id)
        renounce(transaction_id)
        yp_client.commit_transaction(transaction_id)

        assert compare_dicts(maintenance, get_maintenance(), ("state", "info"))

        # Acknowledgement of requested maintenance.
        acknowledge()
        new_maintenance = get_maintenance()
        assert compare_dicts(maintenance, new_maintenance, ("info",))
        assert new_maintenance["state"] == "acknowledged"
        assert new_maintenance["last_updated"] > maintenance["last_updated"]

        # Renouncement of acknowledged maintenance.
        renounce()
        assert compare_dicts(maintenance, get_maintenance(), ("state", "info"))

        # Acknowledgement of acknowledged maintenance is forbidden.
        acknowledge()
        with pytest.raises(YtResponseError):
            acknowledge()

        # Renouncement of acknowledged on node maintenance is allowed.
        # Node maintenance renouncement is performed synchronously.
        yp_client.remove_object("pod", another_pod_id)
        wait(lambda: get_node_maintenance_state() == "acknowledged")
        renounce()
        assert "requested" == get_node_maintenance_state()
        assert_over_time(lambda: compare_dicts(maintenance, get_maintenance(), ("state", "info")))
        assert "requested" == get_node_maintenance_state()

        # Renouncement of in progress on node maintenance is not allowed.
        acknowledge()
        wait(lambda: get_node_maintenance_state() == "acknowledged")
        yp_client.update_hfsm_state(node_id, "maintenance", "Test")
        assert get_node_maintenance_state() == "in_progress"
        with pytest.raises(YtResponseError):
            renounce()

        # Pod renouncement locks node maintenance.
        yp_client.update_hfsm_state(
            node_id,
            "prepare_maintenance",
            "Test",
            maintenance_info=dict(id="caba"),
        )
        wait(lambda: get_maintenance()["state"] == "requested")
        acknowledge()
        wait(lambda: get_node_maintenance_state() == "acknowledged")
        transaction_id = yp_client.start_transaction()
        renounce(transaction_id)
        # Concurrent write to the node maintenance.
        yp_client.update_hfsm_state(node_id, "maintenance", "Test")
        with pytest.raises(YtTabletTransactionLockConflict):
            yp_client.commit_transaction(transaction_id)

        # Drop maintenance.
        yp_client.update_hfsm_state(
            node_id,
            "probation",
            "Test",
        )
        wait(lambda: get_maintenance()["state"] == "none")

    def test_node_acknowledgement_via_pod_eviction(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = create_nodes(yp_client, 1)
        node_id = node_ids[0]

        pod_set_id = create_pod_set(yp_client)
        pod_ids = []
        for _ in xrange(5):
            pod_ids.append(create_pod_with_boilerplate(yp_client, pod_set_id, {
                "enable_scheduling": True
            }))

        disabled_scheduling_pod_id = create_pod_with_boilerplate(yp_client, pod_set_id)
        update_node_id(yp_client, disabled_scheduling_pod_id, node_id)

        for pod_id in pod_ids:
            wait_pod_is_assigned(yp_client, pod_id)
        assert all(x[0] == node_id for x in yp_client.select_objects("pod", selectors=["/status/scheduling/node_id"]))
        assert all(x[0] == "none" for x in yp_client.select_objects("pod", selectors=["/status/eviction/state"]))

        yp_client.update_hfsm_state(node_id, "prepare_maintenance", "Test")
        assert yp_client.get_object("node", node_id, selectors=["/status/maintenance/state"])[0] == "requested"

        wait(lambda: all(x[0] == "requested" for x in yp_client.get_objects("pod", pod_ids, selectors=["/status/eviction/state"])))
        assert_over_time(lambda: "none" == yp_client.get_object("pod", disabled_scheduling_pod_id, selectors=["/status/eviction/state"])[0])

        for pod_id in pod_ids:
            yp_client.acknowledge_pod_eviction(pod_id, "Test")

        wait(lambda: all(x[0] == "" for x in yp_client.get_objects("pod", pod_ids, selectors=["/spec/node_id"])))
        assert all(x[0] == "none" for x in yp_client.get_objects("pod", pod_ids, selectors=["/status/eviction/state"]))

        assert_over_time(lambda: "requested" == yp_client.get_object("node", node_id, selectors=["/status/maintenance/state"])[0])

        wait(lambda: "requested" == yp_client.get_object("pod", disabled_scheduling_pod_id, selectors=["/status/maintenance/state"])[0])
        yp_client.acknowledge_pod_maintenance(disabled_scheduling_pod_id, "Test")
        assert "acknowledged" == yp_client.get_object("pod", disabled_scheduling_pod_id, selectors=["/status/maintenance/state"])[0]

        wait(lambda: "acknowledged" == yp_client.get_object("node", node_id, selectors=["/status/maintenance/state"])[0])

        yp_client.update_hfsm_state(node_id, "maintenance", "Test")
        assert yp_client.get_object("node", node_id, selectors=["/status/maintenance/state"])[0] == "in_progress"

        yp_client.update_hfsm_state(node_id, "down", "Test")
        assert yp_client.get_object("node", node_id, selectors=["/status/maintenance/state"])[0] == "none"

    def test_node_acknowledgement_via_pod_acknowledgement_and_eviction(self, yp_env):
        yp_client = yp_env.yp_client

        node_id, pod_set_id, pod_id = prepare_objects(yp_client)

        another_pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, dict(enable_scheduling=True))
        wait_pod_is_assigned(yp_client, another_pod_id)

        disabled_scheduling_pod_id = create_pod_with_boilerplate(yp_client, pod_set_id)
        update_node_id(yp_client, disabled_scheduling_pod_id, node_id)

        yp_client.update_hfsm_state(
            node_id,
            "prepare_maintenance",
            "Test",
            maintenance_info=dict(id="aba"),
        )

        def get_maintenance(pod_id):
            return yp_client.get_object(
                "pod",
                pod_id,
                selectors=["/status/maintenance"],
            )[0]

        def get_node_maintenance_state():
            return yp_client.get_object(
                "node",
                node_id,
                selectors=["/status/maintenance/state"],
            )[0]

        def get_pod_node(pod_id):
            return yp_client.get_object(
                "pod",
                pod_id,
                selectors=["/status/scheduling/node_id"],
            )[0]

        # Node maintenance is not acknowledged.
        assert_over_time(lambda: get_node_maintenance_state() == "requested")

        # Acknowledge first pod maintenance.
        wait(lambda: get_maintenance(pod_id)["state"] == "requested")
        yp_client.acknowledge_pod_maintenance(pod_id, "Test")
        assert get_maintenance(pod_id)["state"] == "acknowledged"

        # Node maintenance is not acknowledged yet.
        assert_over_time(lambda: get_node_maintenance_state() == "requested")

        # Evict second pod.
        wait(lambda: get_pod_eviction_state(yp_client, another_pod_id) == "requested")
        yp_client.acknowledge_pod_eviction(another_pod_id, "Test")
        wait(lambda: get_pod_node(another_pod_id) == "")

        # Node maintenance is not acknowledged yet.
        assert_over_time(lambda: get_node_maintenance_state() == "requested")

        # Acknowledge maintenance for pod with disabled scheduling.
        wait(lambda: get_maintenance(disabled_scheduling_pod_id)["state"] == "requested")
        yp_client.acknowledge_pod_maintenance(disabled_scheduling_pod_id, "Test")
        assert get_maintenance(disabled_scheduling_pod_id)["state"] == "acknowledged"

        # Wait for node maintenance acknowledgement.
        wait(lambda: get_node_maintenance_state() == "acknowledged")

        # Pod maintenance renouncement is allowed even for already acknowledged node maintenance.
        yp_client.renounce_pod_maintenance(pod_id, "Test")
        assert_over_time(lambda: get_maintenance(pod_id)["state"] == "requested")

        # Node maintenance start is allowed even if not all pod maintenances are acknowledged.
        yp_client.update_hfsm_state(node_id, "maintenance", "Test")

        # State of in progress maintenance will be pushed down to pods forcefully (i.e. even without acknowledgement).
        wait(lambda: get_maintenance(pod_id)["state"] == "in_progress")
