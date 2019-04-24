from .conftest import (
    create_nodes,
    create_pod_with_boilerplate,
    get_pod_scheduling_status,
    is_assigned_pod_scheduling_status,
)

from yp.common import wait

from yt.packages.six.moves import xrange

import pytest
import time


def get_pod_eviction_state(yp_client, pod_id):
    return yp_client.get_object("pod", pod_id, selectors=["/status/eviction/state"])[0]


@pytest.mark.usefixtures("yp_env")
class TestEviction(object):
    def test_abort_requested_eviction_at_up_nodes(self, yp_env):
        yp_client = yp_env.yp_client

        node_id = create_nodes(yp_client, 1)[0]

        pod_set_id = yp_client.create_object("pod_set")
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, dict(enable_scheduling=True))

        wait(lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

        yp_client.update_hfsm_state(node_id, "prepare_maintenance", "Test")

        wait(lambda: get_pod_eviction_state(yp_client, pod_id) == "requested")

        yp_client.update_hfsm_state(node_id, "up", "Test")

        wait(lambda: get_pod_eviction_state(yp_client, pod_id) == "none")


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
