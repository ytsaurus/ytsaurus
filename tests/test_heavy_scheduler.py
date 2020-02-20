from .conftest import (
    are_error_pod_scheduling_statuses,
    assert_over_time,
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
    get_pod_scheduling_statuses,
    run_eviction_acknowledger,
    update_node_id,
    wait,
    wait_pods_are_assigned,
)

from yp.local import set_account_infinite_resource_limits
from yp.logger import logger

from yt.wrapper.errors import YtCypressTransactionLockConflict

from yt.packages.six import PY3
from yt.packages.six.moves import xrange

import json
import logging
import pytest
import time


logger.setLevel(logging.DEBUG)


def get_evictions_count(yp_client, pod_ids):
    states = []
    for pod_id in pod_ids:
        state = yp_client.get_object("pod", pod_id, selectors=["/status/eviction/state"])[0]
        logger.info("Got eviction state for pod '%s': '%s'", pod_id, state)
        states.append(state)
    count = sum(state == "requested" for state in states)
    logger.info("Evictions count: %d", count)
    return count


@pytest.mark.usefixtures("yp_env_configurable")
class TestHeavyScheduler(object):
    START_YP_HEAVY_SCHEDULER = True

    YP_HEAVY_SCHEDULER_CONFIG = dict(
        heavy_scheduler = dict(
            verbose = True,
            node_segment = "test-segment",
            disruption_throttler = dict(
                validate_pod_disruption_budget = False,
                safe_suitable_node_count = 1,
            ),
        ),
    )

    def test_yt_lock(self, yp_env_configurable):
        yt_client = yp_env_configurable.yt_client

        monitoring_client = yp_env_configurable.yp_heavy_scheduler_instance.create_monitoring_client()

        def is_leading():
            try:
                return json.loads(monitoring_client.get("/yt_connector/is_leading"))
            except Exception:
                return False

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

    def test_task_manager_profiling(self, yp_env_configurable):
        monitoring_client = yp_env_configurable.yp_heavy_scheduler_instance.create_monitoring_client()

        def are_task_counters_initialized():
            for name in ("active", "succeeded", "failed", "timed_out"):
                samples = json.loads(monitoring_client.get("/profiling/heavy_scheduler/task_manager/{}".format(name)))
                if len(samples) > 0 and samples[-1]["value"] == 0:
                    continue
                else:
                    return False
            return True

        wait(are_task_counters_initialized, ignore_exceptions=True)

    def _prepare_strategy_test_segment(self, yp_client, node_segment_id):
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

        yp_client.create_object(
            "node_segment",
            attributes=dict(
                meta=dict(id=node_segment_id),
                spec=dict(node_filter="[/labels/segment] = \"{}\"".format(node_segment_id)),
            ),
        )
        node_labels = dict(segment=node_segment_id)

        set_account_infinite_resource_limits(yp_client, "tmp", node_segment_id)

        pod_set_id = yp_client.create_object(
            "pod_set",
            attributes=dict(spec=dict(node_segment_id=node_segment_id)),
        )

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
            labels=node_labels,
        )

        first_pod_ids = create_pods(batch_size, 4 * cpu, 2 * memory)

        wait_pods_are_assigned(yp_client, first_pod_ids)

        # Create second batch of nodes and pods and wait for scheduling errors.
        second_node_ids = create_nodes(
            yp_client,
            node_count=batch_size,
            cpu_total_capacity=6 * cpu,
            memory_total_capacity=3 * memory,
            labels=node_labels,
        )

        second_pod_ids = create_pods(batch_size, 2 * cpu, 4 * memory)

        wait(lambda: are_error_pod_scheduling_statuses(get_pod_scheduling_statuses(yp_client, second_pod_ids)))

        return first_pod_ids + second_pod_ids

    def test_strategy(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        first_segment_pod_ids = self._prepare_strategy_test_segment(yp_client, "test-segment")
        second_segment_pod_ids = self._prepare_strategy_test_segment(yp_client, "test-segment2")

        time_per_pod = 10
        wait_time = time_per_pod * (len(first_segment_pod_ids) + len(second_segment_pod_ids))

        def are_none_eviction_states(pod_ids):
            return all(map(
                lambda response: response[0] == "none",
                yp_client.get_objects("pod", pod_ids, selectors=["/status/eviction/state"]),
            ))

        # Only first segment is processed by the Heavy Scheduler.
        assert_over_time(lambda: are_none_eviction_states(second_segment_pod_ids), iter=wait_time, sleep_backoff=1.0)

        # Implicitly validates that Heavy Scheduler requests eviction with reason = "scheduler".
        run_eviction_acknowledger(yp_client, iteration_count=wait_time, sleep_time=1.0, eviction_reason="scheduler")
        wait_pods_are_assigned(yp_client, first_segment_pod_ids)


@pytest.mark.usefixtures("yp_env_configurable")
class TestHeavySchedulerEvictionGarbageCollector(object):
    START_YP_HEAVY_SCHEDULER = True

    YP_HEAVY_SCHEDULER_CONFIG = dict(
        heavy_scheduler = dict(
            eviction_garbage_collector = dict(
                time_limit = 20 * 1000,
            ),
        ),
    )

    def test(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        time_limit = self.YP_HEAVY_SCHEDULER_CONFIG["heavy_scheduler"]["eviction_garbage_collector"]["time_limit"] / 1000.0

        def get_eviction_state(pod_id):
            return yp_client.get_object("pod", pod_id, selectors=["/status/eviction/state"])[0]

        node_id = create_nodes(yp_client, 1)[0]
        pod_set_id = create_pod_set(yp_client)

        def create_pod(spec=dict(), reason=None):
            pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, spec=spec)
            update_node_id(yp_client, pod_id, node_id)
            yp_client.request_pod_eviction(pod_id, "Test", reason=reason)
            return pod_id

        client_pod_id = create_pod()

        for enable_scheduling in (False, True):
            pod_id = create_pod(spec=dict(enable_scheduling=enable_scheduling), reason="scheduler")
            time.sleep(time_limit / 5.0)
            assert "requested" == get_eviction_state(pod_id)
            wait(lambda: "none" == get_eviction_state(pod_id))

        assert "requested" == get_eviction_state(client_pod_id)


class TestConcurrentHeavySchedulerBase(object):
    START_YP_HEAVY_SCHEDULER = True

    def _prepare(self, yp_client, pod_set_ids):
        node_ids = create_nodes(
            yp_client,
            node_count=6,
            cpu_total_capacity=10000,
            memory_total_capacity=2 ** 60,
            labels=dict(segment="default"),
        )

        bloat_pod_ids = []
        for node_id, pod_set_id in zip(node_ids, pod_set_ids):
            bloat_pod_ids.append(create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                spec=dict(
                    node_id=node_id,
                    resource_requests=dict(
                        vcpu_guarantee=5000,
                        memory_limit=2 ** 30,
                    ),
                ),
            ))
            # NB: can not create pod with /spec/node_id and /spec/enable_scheduling both set.
            yp_client.update_object("pod", bloat_pod_ids[-1],
                                    set_updates=[dict(path="/spec/enable_scheduling", value=True)])

        for pod_set_id in pod_set_ids[len(bloat_pod_ids):]:
            create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                spec=dict(
                    enable_scheduling=True,
                    resource_requests=dict(
                        vcpu_guarantee=10000,
                        memory_limit=2 ** 30,
                    ),
                ),
            )

        return bloat_pod_ids

    def _check(self, yp_client, bloat_pod_ids, expected_eviction_count):
        wait(lambda: get_evictions_count(yp_client, bloat_pod_ids) == expected_eviction_count)
        time.sleep(20)
        assert get_evictions_count(yp_client, bloat_pod_ids) == expected_eviction_count


@pytest.mark.usefixtures("yp_env_configurable")
class TestConcurrentHeavyScheduler(TestConcurrentHeavySchedulerBase):
    YP_HEAVY_SCHEDULER_CONFIG = dict(
        heavy_scheduler = dict(
            verbose = True,
            node_segment = "default",
            safe_cluster_pod_eviction_count = 9,
            task_manager = dict(
                task_slots_per_source = dict(
                    swap_defragmentator = 2,
                ),
            ),
            disruption_throttler = dict(
                validate_pod_disruption_budget = False,
                safe_suitable_node_count = 1,
            ),
        ),
    )

    def test_concurrent_tasks(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        pod_set_ids = [create_pod_set(yp_client) for _ in range(9)]
        bloat_pod_ids = self._prepare(yp_client, pod_set_ids)
        self._check(yp_client, bloat_pod_ids, expected_eviction_count=2)

    def test_concurrent_tasks_disruption(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        pod_set_ids = [create_pod_set(yp_client)] * 9
        bloat_pod_ids = self._prepare(yp_client, pod_set_ids)
        self._check(yp_client, bloat_pod_ids, expected_eviction_count=1)


@pytest.mark.usefixtures("yp_env_configurable")
class TestConcurrentHeavySchedulerLimitEvictions(TestConcurrentHeavySchedulerBase):
    YP_HEAVY_SCHEDULER_CONFIG = dict(
        heavy_scheduler = dict(
            node_segment = "default",
            safe_cluster_pod_eviction_count = 9,
            task_manager = dict(
                task_slots_per_source = dict(
                    swap_defragmentator = 2,
                ),
            ),
            disruption_throttler = dict(
                limit_evictions_by_pod_set = False,
                validate_pod_disruption_budget = False,
                safe_suitable_node_count = 1,
            ),
        ),
    )

    def test_concurrent_tasks_evictions_limit(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        pod_set_ids = [create_pod_set(yp_client)] * 9
        bloat_pod_ids = self._prepare(yp_client, pod_set_ids)
        self._check(yp_client, bloat_pod_ids, expected_eviction_count=2)


@pytest.mark.usefixtures("yp_env_configurable")
class TestConcurrentHeavySchedulerSubsequentEvictions(TestConcurrentHeavySchedulerBase):
    YP_HEAVY_SCHEDULER_CONFIG = dict(
        heavy_scheduler = dict(
            verbose = True,
            node_segment = "default",
            safe_cluster_pod_eviction_count = 9,
            task_manager = dict(
                task_slots_per_source = dict(
                    swap_defragmentator = 9,
                ),
            ),
            disruption_throttler = dict(
                limit_evictions_by_pod_set = True,
                validate_pod_disruption_budget = False,
                safe_suitable_node_count = 1,
            ),
        ),
    )

    def test_concurrent_tasks_subsequent_evictions(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        pod_set_ids = [create_pod_set(yp_client) for _ in range(9)]
        bloat_pod_ids = self._prepare(yp_client, pod_set_ids)
        self._check(yp_client, bloat_pod_ids, expected_eviction_count=3)


@pytest.mark.usefixtures("yp_env_configurable")
class TestAntiaffinityHealer(object):
    START_YP_HEAVY_SCHEDULER = True

    YP_HEAVY_SCHEDULER_CONFIG = dict(
        heavy_scheduler = dict(
            verbose = True,
            node_segment = "default",
            safe_cluster_pod_eviction_count = 9,
            disruption_throttler = dict(
                validate_pod_disruption_budget = False,
                safe_suitable_node_count = 1,
            ),
        ),
    )

    def _make_node(self, yp_client, rack="default", dc="default", name=None):
        node_id = create_nodes(
            yp_client,
            node_count=1,
            cpu_total_capacity=100000,
            memory_total_capacity=2 ** 60,
            labels=dict(
                segment="default",
            ),
        )[0]
        topology = dict(
            node=node_id,
            rack=rack,
            dc=dc,
        )
        yp_client.update_object("node", node_id, set_updates=[dict(path="/labels/topology", value=topology)])
        return node_id

    def test_simple_antiaffinity_healing(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        self._make_node(yp_client, "one")

        pod_set_id = create_pod_set(yp_client)
        pod_ids = [create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(
                enable_scheduling=True,
                resource_requests=dict(
                    vcpu_guarantee=1000,
                    memory_limit=2 ** 30,
                ),
            ),
        ) for _ in range(2)]
        wait_pods_are_assigned(yp_client, pod_ids)

        yp_client.update_object("pod_set", pod_set_id, set_updates=[
            dict(path="/spec/antiaffinity_constraints", value=[dict(key="node", max_pods=1)])
        ])

        self._make_node(yp_client, "one")

        wait(lambda: get_evictions_count(yp_client, pod_ids) == 1)
        time.sleep(5)
        assert get_evictions_count(yp_client, pod_ids) == 1

    def test_sharded_antiaffinity_healing(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        pod_set_id = create_pod_set(yp_client, spec=dict(antiaffinity_constraints=[
            dict(key="node", max_pods=1, pod_group_id_path="/labels/shard"),
        ]))

        self._make_node(yp_client)
        pod_ids = [create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(
                enable_scheduling=True,
                resource_requests=dict(
                    vcpu_guarantee=1000,
                    memory_limit=2 ** 30,
                ),
            ),
            labels=dict(
                shard=str(i),
            ),
        ) for i in range(10)]

        wait_pods_are_assigned(yp_client, pod_ids)

        self._make_node(yp_client)

        yp_client.update_object("pod", pod_ids[7], set_updates=[dict(path="/labels/shard", value="2")])

        new_shard_pod_ids = [pod_ids[2], pod_ids[7]]
        wait(lambda: get_evictions_count(yp_client, new_shard_pod_ids) == 1)
        time.sleep(5)
        assert get_evictions_count(yp_client, new_shard_pod_ids) == 1
        assert get_evictions_count(yp_client, pod_ids) == 1

    def test_antiaffinity_healer_limit_by_pod_set(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        pod_set_id = create_pod_set(yp_client, spec=dict(antiaffinity_constraints=[
            dict(key="rack", max_pods=1),
        ]))

        old_node_ids = [self._make_node(yp_client, "old_{}".format(i)) for i in range(3)]

        pod_ids = [create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(
                enable_scheduling=True,
                resource_requests=dict(
                    vcpu_guarantee=1000,
                    memory_limit=2 ** 30,
                ),
            ),
        ) for i in range(3)]

        wait_pods_are_assigned(yp_client, pod_ids)

        for node_id in old_node_ids:
            yp_client.update_object("node", node_id, set_updates=[dict(path="/labels/topology/rack", value="old")])

        for i in range(10):
            self._make_node(yp_client, str(i))

        wait(lambda: get_evictions_count(yp_client, pod_ids) == 1)
        time.sleep(5)
        assert get_evictions_count(yp_client, pod_ids) == 1

    def test_antiaffinity_healer_no_overevictions(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        node_ids = [self._make_node(yp_client, rack=str(i // 3), dc=str(i // 6)) for i in range(9)]

        pod_set_id = create_pod_set(yp_client)

        pod_spec = dict(
            resource_requests=dict(
                vcpu_guarantee=1000,
                memory_limit=2**30,
            )
        )

        pod_ids = []
        for i in range(12):
            pod_spec["node_id"] = node_ids[i % 6]
            pod_ids.append(create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec))

        pod_spec["node_id"] = node_ids[0]
        pod_ids.append(create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec))

        for pod_id in pod_ids:
            yp_client.update_object("pod", pod_id, set_updates=[dict(path="/spec/enable_scheduling",
                                                                     value=True)])

        antiaffinity_constraints = [
            dict(key="node", max_pods=2),
            dict(key="rack", max_pods=6),
            dict(key="dc", max_pods=12),
        ]
        yp_client.update_object("pod_set", pod_set_id, set_updates=[dict(path="/spec/antiaffinity_constraints",
                                                                         value=antiaffinity_constraints)])

        pods_to_evict = [pod_ids[0], pod_ids[6], pod_ids[12]]
        wait(lambda: get_evictions_count(yp_client, pod_ids) == 1)
        assert get_evictions_count(yp_client, pods_to_evict) == 1

        time.sleep(5)
        assert get_evictions_count(yp_client, pod_ids) == 1
        assert get_evictions_count(yp_client, pods_to_evict) == 1
