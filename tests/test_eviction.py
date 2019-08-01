from .conftest import (
    check_over_time,
    create_nodes,
    create_pod_with_boilerplate,
    get_pod_scheduling_status,
    is_assigned_pod_scheduling_status,
)

from yp.common import wait, YpAuthorizationError, YtResponseError

from yt.packages.six.moves import xrange

import pytest
import time


def get_pod_eviction(yp_client, pod_id):
    return yp_client.get_object(
        "pod",
        pod_id,
        selectors=["/status/eviction"],
    )[0]


def get_pod_eviction_state(*args, **kwargs):
    return get_pod_eviction(*args, **kwargs)["state"]


def wait_for_pod_assignment(yp_client, pod_id):
    wait(lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))


def prepare_objects(yp_client):
    node_id = create_nodes(yp_client, 1)[0]
    pod_set_id = yp_client.create_object("pod_set")
    pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, dict(enable_scheduling=True))
    wait_for_pod_assignment(yp_client, pod_id)
    return node_id, pod_set_id, pod_id


@pytest.mark.usefixtures("yp_env_configurable")
class TestEvictionByHfsmController(object):
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

        # Test that pod-eviction-by-hfsm controller works fine.
        assert not is_controller_disabled()

        yp_env_configurable.set_cypress_config_patch(dict(scheduler=dict(disable_stage=dict(
            run_pod_eviction_by_hfsm_controller=True,
        ))))

        wait(is_controller_disabled, iter=5, sleep_backoff=10)

        # Sanity check.
        assert get_pod_scheduling_status(yp_client, pod_id)["node_id"] == node_id


@pytest.mark.usefixtures("yp_env_configurable")
class TestEviction(object):
    # Choosing a pretty small period to optimize tests duration.
    SCHEDULER_LOOP_PERIOD_MILLISECONDS = 1 * 1000

    YP_MASTER_CONFIG = dict(
        scheduler=dict(
            loop_period=SCHEDULER_LOOP_PERIOD_MILLISECONDS,
        )
    )

    def _wait_for_scheduler_loop(self, yp_env):
        time.sleep((5 * TestEviction.SCHEDULER_LOOP_PERIOD_MILLISECONDS) / 1000.0)

    def _validate_eviction(self, yp_client, pod_id, reason, state):
        eviction = get_pod_eviction(yp_client, pod_id)
        assert eviction["reason"] == reason
        assert eviction["message"] == "Test"
        assert eviction["state"] == state

    def _validate_requested_eviction(self, yp_client, pod_id):
        self._validate_eviction(yp_client, pod_id, "client", "requested")

    def _validate_none_eviction(self, yp_client, pod_id):
        self._validate_eviction(yp_client, pod_id, "none", "none")

    def test_only_superuser_can_abort_pod_eviction(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        _, pod_set_id, pod_id = prepare_objects(yp_client)

        yp_client.create_object("user", attributes=dict(meta=dict(id="u")))
        yp_env_configurable.sync_access_control()

        yp_client.update_object(
            "pod_set",
            pod_set_id,
            set_updates=[
                dict(
                    path="/meta/acl/end",
                    value=dict(
                        permissions=["write"],
                        subjects=["u"],
                        action="allow",
                    ),
                ),
            ],
        )
        yp_env_configurable.sync_access_control()

        yp_client.request_pod_eviction(pod_id, "Test")

        with yp_env_configurable.yp_instance.create_client(config=dict(user="u")) as yp_client1:
            with pytest.raises(YpAuthorizationError):
                yp_client1.abort_pod_eviction(pod_id, "Test")
            self._validate_requested_eviction(yp_client, pod_id)

            yp_client.update_object("group", "superusers", set_updates=[
                dict(
                    path="/spec/members/end",
                    value="u",
                ),
            ])
            yp_env_configurable.sync_access_control()

            yp_client1.abort_pod_eviction(pod_id, "Test")
            self._validate_none_eviction(yp_client, pod_id)

    # Pod eviction state must be requested before the request.
    def test_abort_eviction_state_prerequisites(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        _, _, pod_id = prepare_objects(yp_client)

        # State is none before the request.
        with pytest.raises(YtResponseError):
            yp_client.abort_pod_eviction(pod_id, "Test")

        # State is requested before the request.
        yp_client.request_pod_eviction(pod_id, "Test")
        self._validate_requested_eviction(yp_client, pod_id)
        yp_client.abort_pod_eviction(pod_id, "Test")
        self._validate_none_eviction(yp_client, pod_id)

        # State is acknowledged before the request.
        transaction_id = yp_client.start_transaction()
        yp_client.request_pod_eviction(pod_id, "Test", transaction_id=transaction_id)
        yp_client.acknowledge_pod_eviction(pod_id, "Test", transaction_id=transaction_id)
        with pytest.raises(YtResponseError):
            yp_client.abort_pod_eviction(pod_id, "Test", transaction_id=transaction_id)

    def test_only_superuser_can_request_pod_eviction(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        _, pod_set_id, pod_id = prepare_objects(yp_client)

        yp_client.create_object("user", attributes=dict(meta=dict(id="u")))
        yp_env_configurable.sync_access_control()

        yp_client.update_object(
            "pod_set",
            pod_set_id,
            set_updates=[
                dict(
                    path="/meta/acl/end",
                    value=dict(
                        permissions=["write"],
                        subjects=["u"],
                        action="allow",
                    ),
                ),
            ],
        )
        yp_env_configurable.sync_access_control()

        with yp_env_configurable.yp_instance.create_client(config=dict(user="u")) as yp_client1:
            with pytest.raises(YpAuthorizationError):
                yp_client1.request_pod_eviction(pod_id, "Test")

            yp_client.update_object("group", "superusers", set_updates=[
                dict(
                    path="/spec/members/end",
                    value="u",
                ),
            ])
            yp_env_configurable.sync_access_control()

            yp_client1.request_pod_eviction(pod_id, "Test")

    def test_request_and_acknowledge_eviction(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        node_id, _, pod_id = prepare_objects(yp_client)
        yp_client.request_pod_eviction(pod_id, "Test")
        yp_client.acknowledge_pod_eviction(pod_id, "Test")
        wait(lambda: get_pod_eviction_state(yp_client, pod_id) == "none")

    # Requested pod eviction cannot be aborted or overwritten without appropriate reason.
    # Particularly, it must be persistent in the presence of concurrent processes
    # related to the eviction (e.g. hfsm).
    def test_request_eviction_persistence(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        node_id, _, pod_id = prepare_objects(yp_client)

        yp_client.request_pod_eviction(pod_id, "Test")
        self._validate_requested_eviction(yp_client, pod_id)
        self._wait_for_scheduler_loop(yp_env_configurable)
        self._validate_requested_eviction(yp_client, pod_id)

        yp_client.update_hfsm_state(node_id, "prepare_maintenance", "Test")
        self._wait_for_scheduler_loop(yp_env_configurable)
        self._validate_requested_eviction(yp_client, pod_id)

        with pytest.raises(YtResponseError):
            yp_client.request_pod_eviction(pod_id, "Test")

    # Pod eviction state must be none before the request.
    def test_request_eviction_state_prerequisites(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        _, _, pod_id = prepare_objects(yp_client)

        # State is none before the request.
        yp_client.request_pod_eviction(pod_id, "Test")

        # State is requested before the request.
        with pytest.raises(YtResponseError):
            yp_client.request_pod_eviction(pod_id, "Test")

        # State is acknowledged before the request.
        transaction_id = yp_client.start_transaction()
        yp_client.acknowledge_pod_eviction(pod_id, "Test", transaction_id=transaction_id)
        with pytest.raises(YtResponseError):
            yp_client.request_pod_eviction(pod_id, "Test", transaction_id=transaction_id)

    def test_request_eviction_of_unassigned_pod(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        pod_set_id = yp_client.create_object("pod_set")
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id)
        with pytest.raises(YtResponseError):
            yp_client.request_pod_eviction(pod_id, "Test")

    def test_request_eviction_conflicts(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        node_id, _, pod_id = prepare_objects(yp_client)

        # Without concurrent changes.
        transaction_id = yp_client.start_transaction()
        yp_client.request_pod_eviction(pod_id, "Test", transaction_id=transaction_id)
        yp_client.commit_transaction(transaction_id)

        yp_client.abort_pod_eviction(pod_id, "Test")

        # With concurrent pod eviction by the hfsm.
        transaction_id = yp_client.start_transaction()
        yp_client.request_pod_eviction(pod_id, "Test", transaction_id=transaction_id)

        yp_client.update_hfsm_state(node_id, "down", "Test")
        wait(lambda: get_pod_eviction_state(yp_client, pod_id) == "requested")

        with pytest.raises(YtResponseError):
            yp_client.commit_transaction(transaction_id)

        yp_client.update_hfsm_state(node_id, "up", "Test")
        wait(lambda: get_pod_eviction_state(yp_client, pod_id) == "none")

        # With concurrent pod eviction by the client.
        transaction_id = yp_client.start_transaction()
        yp_client.request_pod_eviction(pod_id, "Test", transaction_id=transaction_id)

        yp_client.request_pod_eviction(pod_id, "Test")

        with pytest.raises(YtResponseError):
            yp_client.commit_transaction(transaction_id)

        yp_client.abort_pod_eviction(pod_id, "Test")


@pytest.mark.usefixtures("yp_env_configurable")
class TestEvictionAcknowledgement(object):
    # Choosing long period we hope to catch different
    # relative positions of the acknowledgement request and the scheduler loop.
    SCHEDULER_LOOP_PERIOD_MILLISECONDS = 10 * 1000

    YP_MASTER_CONFIG = dict(
        scheduler=dict(
            loop_period=SCHEDULER_LOOP_PERIOD_MILLISECONDS,
        )
    )

    def test_multiple_uniform_acknowledgements(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        node_id = create_nodes(yp_client, 1)[0]

        pod_set_id = yp_client.create_object("pod_set")

        pod_count = 20
        pod_ids = []
        for _ in xrange(pod_count):
            pod_ids.append(
                create_pod_with_boilerplate(yp_client, pod_set_id, dict(enable_scheduling=True))
            )

        def get_assigned_pod_count():
            return sum(map(
                lambda pod_id: is_assigned_pod_scheduling_status(
                    get_pod_scheduling_status(yp_client, pod_id)
                ),
                pod_ids,
            ))

        def custom_wait(callback):
            iter_count = 10
            wait(
                callback,
                iter=iter_count,
                sleep_backoff=2 * TestEvictionAcknowledgement.SCHEDULER_LOOP_PERIOD_MILLISECONDS / 1000.0 / iter_count,
            )

        custom_wait(lambda: get_assigned_pod_count() == len(pod_ids))

        yp_client.update_hfsm_state(node_id, "prepare_maintenance", "Test")

        def are_pod_evictions_in_state(state):
            return all(map(
                lambda pod_id: get_pod_eviction_state(yp_client, pod_id) == state,
                pod_ids,
            ))

        custom_wait(lambda: are_pod_evictions_in_state("requested"))

        # We hope to send acknowledgements uniformly and intersecting several scheduler loops.
        sleep_time_milliseconds = (2.0 * TestEvictionAcknowledgement.SCHEDULER_LOOP_PERIOD_MILLISECONDS) / pod_count
        for pod_id in pod_ids:
            yp_client.acknowledge_pod_eviction(pod_id, "Test")
            time.sleep(sleep_time_milliseconds / 1000.0)

        custom_wait(lambda: are_pod_evictions_in_state("none"))
        assert get_assigned_pod_count() == 0
