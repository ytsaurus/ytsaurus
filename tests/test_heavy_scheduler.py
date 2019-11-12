from .conftest import (
    are_error_pod_scheduling_statuses,
    are_pods_assigned,
    assert_over_time,
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
    get_pod_scheduling_statuses,
    run_eviction_acknowledger,
    wait,
)

from yt.wrapper.errors import YtCypressTransactionLockConflict

from yt.packages.six import PY3
from yt.packages.six.moves import xrange

import pytest


@pytest.mark.usefixtures("yp_env_configurable")
class TestHeavyScheduler(object):
    START_YP_HEAVY_SCHEDULER = True

    YP_HEAVY_SCHEDULER_CONFIG = dict(
        heavy_scheduler = dict(
            safe_suitable_node_count = 1,
            validate_pod_disruption_budget = False,
        ),
    )

    def test_yt_lock(self, yp_env_configurable):
        yt_client = yp_env_configurable.yt_client

        monitoring_client = yp_env_configurable.yp_heavy_scheduler_instance.create_monitoring_client()

        def is_leading():
            try:
                is_leading_repr = monitoring_client.get("/yt_connector/is_leading")
            except Exception:
                return False
            if PY3:
                expected = b"true"
            else:
                expected = "true"
            return is_leading_repr == expected

        def not_is_leading():
            return not is_leading()

        wait(is_leading)

        iteration_count = 100
        for _ in xrange(iteration_count):
            transaction_id = yt_client.get("//yp/heavy_scheduler/leader/@locks")[0]["transaction_id"]
            yt_client.abort_transaction(transaction_id)
            try:
                yt_client.remove("//yp/heavy_scheduler/leader")
            except YtCypressTransactionLockConflict:
                continue
            else:
                break
        else:
            assert False, "Cannot take away lock from Heavy Scheduler for {} iterations".format(iteration_count)

        wait(not_is_leading)
        assert_over_time(not_is_leading)

        yt_client.create("map_node", "//yp/heavy_scheduler/leader")
        wait(is_leading)

    def test_strategy(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        def create_pods(count, cpu, memory):
            pod_ids = []
            for _ in xrange(count):
                pod_ids.append(create_pod_with_boilerplate(
                    yp_client,
                    pod_set_id,
                    spec=dict(
                        enable_scheduling=True,
                        resource_requests=dict(
                            vcpu_guarantee=cpu,
                            memory_limit=memory,
                        ),
                    ),
                ))
            return pod_ids

        pod_set_id = create_pod_set(yp_client)

        cpu = 100
        memory = 10 * (1024 ** 2)

        batch_size = 3

        # Consider two batches of pods and nodes with the following conditions:
        # - First batch pod can be assigned to any node, but would prefer second batch node.
        # - Second batch pod can be assigned only to a first batch node.
        # - There is no node capable of containing more than one pod.

        # Create first batch of nodes and pods and wait for pods assignment.
        first_node_ids = create_nodes(
            yp_client,
            node_count=batch_size,
            cpu_total_capacity=4 * cpu,
            memory_total_capacity=4 * memory,
        )

        first_pod_ids = create_pods(batch_size, 4 * cpu, 2 * memory)

        wait(lambda: are_pods_assigned(yp_client, first_pod_ids))

        # Create second batch of nodes and pods and wait for scheduling errors.
        second_node_ids = create_nodes(
            yp_client,
            node_count=batch_size,
            cpu_total_capacity=6 * cpu,
            memory_total_capacity=3 * memory,
        )

        second_pod_ids = create_pods(batch_size, 2 * cpu, 4 * memory)

        wait(lambda: are_error_pod_scheduling_statuses(get_pod_scheduling_statuses(yp_client, second_pod_ids)))

        # Run eviction acknowledger and wait for Heavy Scheduler to schedule all pods.
        time_per_pod = 30
        run_eviction_acknowledger(yp_client, iteration_count=time_per_pod * batch_size, sleep_time=1)

        wait(lambda: are_pods_assigned(yp_client, first_pod_ids + second_pod_ids))
