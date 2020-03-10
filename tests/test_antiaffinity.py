from .conftest import (
    are_pods_assigned,
    are_pods_touched_by_scheduler,
    assert_over_time,
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
    get_pod_scheduling_status,
    is_error_pod_scheduling_status,
    is_pod_assigned,
    wait,
)

from yt.common import flatten

from yt.packages.six import itervalues
from yt.packages.six.moves import xrange

import pytest

from collections import Counter


@pytest.mark.usefixtures("yp_env")
class TestAntiaffinity(object):
    class AssignmentCounters(object):
        def __init__(self, yp_client, pod_ids):
            used_node_ids = list(
                response[0]
                for response in yp_client.get_objects(
                    "pod", pod_ids, selectors=["/status/scheduling/node_id"],
                )
            )
            used_node_ids = list(filter(lambda node_id: node_id != "", used_node_ids))
            used_rack_ids = list(
                response[0]
                for response in yp_client.get_objects(
                    "node", used_node_ids, selectors=["/labels/topology/rack"],
                )
            )
            self.assigned_pod_count = len(used_node_ids)
            self.nonassigned_pod_count = len(pod_ids) - self.assigned_pod_count
            self.node_ids_set = set(used_node_ids)
            self.node_pod_count = Counter(used_node_ids)
            self.rack_pod_count = Counter(used_rack_ids)

    def _prepare_pod_set(self, yp_client, antiaffinity_constraints):
        return create_pod_set(
            yp_client, spec=dict(antiaffinity_constraints=antiaffinity_constraints),
        )

    def _prepare_pod_groups(self, yp_client, pod_set_id, pod_per_group_count, group_count):
        pod_ids = []
        for group_id in xrange(group_count):
            group_pod_ids = []
            for _ in xrange(pod_per_group_count):
                group_pod_ids.append(
                    create_pod_with_boilerplate(
                        yp_client,
                        pod_set_id,
                        spec=dict(enable_scheduling=True),
                        labels=dict(group_id=str(group_id)),
                    )
                )
            pod_ids.append(group_pod_ids)
        return pod_ids

    def _prepare_objects(
        self,
        yp_client,
        node_per_rack_count=1,
        rack_count=1,
        pod_per_group_count=1,
        group_count=1,
        antiaffinity_constraints=None,
    ):
        create_nodes(
            yp_client, node_per_rack_count * rack_count, rack_count,
        )
        pod_set_id = self._prepare_pod_set(yp_client, antiaffinity_constraints,)
        return self._prepare_pod_groups(yp_client, pod_set_id, pod_per_group_count, group_count,)

    def test_per_node(self, yp_env):
        yp_client = yp_env.yp_client

        node_pod_limit = 1
        node_count = 10
        pod_count = node_count * node_pod_limit + 1

        pod_ids = self._prepare_objects(
            yp_client,
            node_per_rack_count=node_count,
            pod_per_group_count=pod_count,
            antiaffinity_constraints=[dict(key="node", max_pods=node_pod_limit)],
        )[0]

        wait(lambda: are_pods_touched_by_scheduler(yp_client, pod_ids))

        counters = self.AssignmentCounters(yp_client, pod_ids)

        assert all(node_pod_limit == count for count in itervalues(counters.node_pod_count))

    def test_per_node_and_rack(self, yp_env):
        yp_client = yp_env.yp_client

        node_per_rack_count = 5
        node_pod_limit = 1
        rack_count = 2
        rack_pod_limit = 3

        assert node_per_rack_count * node_pod_limit > rack_pod_limit

        pod_count = rack_count * rack_pod_limit + 1

        pod_ids = self._prepare_objects(
            yp_client,
            node_per_rack_count=node_per_rack_count,
            rack_count=rack_count,
            pod_per_group_count=pod_count,
            antiaffinity_constraints=[
                dict(key="node", max_pods=node_pod_limit),
                dict(key="rack", max_pods=rack_pod_limit),
            ],
        )[0]

        wait(lambda: are_pods_touched_by_scheduler(yp_client, pod_ids))

        counters = self.AssignmentCounters(yp_client, pod_ids)

        assert all(rack_pod_limit == count for count in itervalues(counters.rack_pod_count))
        assert all(node_pod_limit >= count for count in itervalues(counters.node_pod_count))

    def test_pod_group(self, yp_env):
        yp_client = yp_env.yp_client

        node_count = 3
        node_pod_limit = 1
        group_count = 2
        pod_per_group_count = node_count * node_pod_limit + 1

        pod_ids = self._prepare_objects(
            yp_client,
            node_per_rack_count=node_count,
            pod_per_group_count=pod_per_group_count,
            group_count=group_count,
            antiaffinity_constraints=[
                dict(key="node", max_pods=node_pod_limit, pod_group_id_path="/labels/group_id",),
            ],
        )

        all_pod_ids = flatten(pod_ids)

        wait(lambda: are_pods_touched_by_scheduler(yp_client, all_pod_ids))

        counters = self.AssignmentCounters(yp_client, all_pod_ids)
        assert all(
            group_count * node_pod_limit == count for count in itervalues(counters.node_pod_count)
        )

        for group_id in xrange(group_count):
            group_pod_ids = pod_ids[group_id]
            counters = self.AssignmentCounters(yp_client, group_pod_ids)
            assert all(node_pod_limit == count for count in itervalues(counters.node_pod_count))

    def test_pod_group_per_node_and_rack(self, yp_env):
        yp_client = yp_env.yp_client

        rack_count = 2
        node_per_rack_count = 3
        group_count = 2

        node_pod_limit = 1
        rack_pod_limit = node_pod_limit * node_per_rack_count - 1

        pod_per_group_count = rack_count * rack_pod_limit + 1

        pod_ids = self._prepare_objects(
            yp_client,
            node_per_rack_count=node_per_rack_count,
            rack_count=rack_count,
            pod_per_group_count=pod_per_group_count,
            group_count=group_count,
            antiaffinity_constraints=[
                dict(key="node", max_pods=node_pod_limit, pod_group_id_path="/labels/group_id",),
                dict(key="rack", max_pods=rack_pod_limit, pod_group_id_path="/labels/group_id",),
            ],
        )

        wait(lambda: are_pods_touched_by_scheduler(yp_client, flatten(pod_ids)))

        for group_id in xrange(group_count):
            group_pod_ids = pod_ids[group_id]
            counters = self.AssignmentCounters(yp_client, group_pod_ids)
            assert all(rack_pod_limit == count for count in itervalues(counters.rack_pod_count))
            assert all(node_pod_limit >= count for count in itervalues(counters.node_pod_count))

    def test_with_and_without_pod_group(self, yp_env):
        yp_client = yp_env.yp_client

        node_pod_limit = 3
        node_pod_per_group_limit = 2

        group_count = 2
        pod_per_group_count = 3

        pod_ids = self._prepare_objects(
            yp_client,
            pod_per_group_count=pod_per_group_count,
            group_count=group_count,
            antiaffinity_constraints=[
                dict(key="node", max_pods=node_pod_limit,),
                dict(
                    key="node",
                    max_pods=node_pod_per_group_limit,
                    pod_group_id_path="/labels/group_id",
                ),
            ],
        )

        all_pod_ids = flatten(pod_ids)

        wait(lambda: are_pods_touched_by_scheduler(yp_client, all_pod_ids))

        counters = self.AssignmentCounters(yp_client, all_pod_ids)
        assert node_pod_limit == counters.assigned_pod_count

        for group_id in xrange(group_count):
            group_pod_ids = pod_ids[group_id]
            counters = self.AssignmentCounters(yp_client, group_pod_ids)
            assert all(
                1 <= count <= node_pod_per_group_limit
                for count in itervalues(counters.node_pod_count)
            )

    def test_incorrect_pod_group_path(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1)

        pod_set_id = self._prepare_pod_set(
            yp_client,
            antiaffinity_constraints=[
                dict(key="node", max_pods=1, pod_group_id_path="/lbls/group_id",),
            ],
        )

        pod_id = create_pod_with_boilerplate(
            yp_client, pod_set_id, spec=dict(enable_scheduling=True),
        )

        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

        yp_client.update_object(
            "pod_set",
            pod_set_id,
            set_updates=[
                dict(
                    path="/spec/antiaffinity_constraints/0/pod_group_id_path",
                    value="/labels/group_id",
                ),
            ],
        )

        wait(lambda: is_pod_assigned(yp_client, pod_id))

    def test_incorrect_pod_group_id(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1)

        pod_set_id = self._prepare_pod_set(
            yp_client,
            antiaffinity_constraints=[
                dict(key="node", max_pods=100500, pod_group_id_path="/labels/group_id",),
            ],
        )

        for pod_group_id in (123, ""):
            pod_id = create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                spec=dict(enable_scheduling=True),
                labels=dict(group_id=pod_group_id),
            )

            wait(
                lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id))
            )

            yp_client.update_object(
                "pod", pod_id, set_updates=[dict(path="/labels", value=dict(group_id="1"),)],
            )

            wait(lambda: is_pod_assigned(yp_client, pod_id))

    def test_missing_pod_group_id(self, yp_env):
        yp_client = yp_env.yp_client

        node_pod_limit = 1
        pod_count = 3

        assert pod_count > node_pod_limit

        pod_ids = self._prepare_objects(
            yp_client,
            pod_per_group_count=pod_count,
            antiaffinity_constraints=[
                dict(
                    key="node",
                    max_pods=node_pod_limit,
                    pod_group_id_path="/labels/unknown_group_id_attribute",
                ),
            ],
        )[0]

        wait(lambda: are_pods_assigned(yp_client, pod_ids))

    def test_set_incorrect_pod_group_id_after_allocation(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1)

        pod_set_id = self._prepare_pod_set(
            yp_client,
            antiaffinity_constraints=[
                dict(key="node", max_pods=2, pod_group_id_path="/labels/group_id",),
            ],
        )

        pod_id1 = create_pod_with_boilerplate(
            yp_client, pod_set_id, spec=dict(enable_scheduling=True), labels=dict(group_id="42"),
        )

        wait(lambda: is_pod_assigned(yp_client, pod_id1))

        yp_client.update_object(
            "pod", pod_id1, set_updates=[dict(path="/labels", value=dict(group_id=123123),)],
        )

        pod_id2 = create_pod_with_boilerplate(
            yp_client, pod_set_id, spec=dict(enable_scheduling=True), labels=dict(group_id="42"),
        )

        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id2)))

        yp_client.update_object(
            "pod", pod_id1, set_updates=[dict(path="/labels", value=dict(group_id="42"),)],
        )

        wait(lambda: is_pod_assigned(yp_client, pod_id2))

    def test_overcommit_after_allocation(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1)

        pod_set_id = self._prepare_pod_set(
            yp_client, antiaffinity_constraints=[dict(key="node", max_pods=3,)],
        )

        def create_pod():
            return create_pod_with_boilerplate(
                yp_client, pod_set_id, spec=dict(enable_scheduling=True),
            )

        def update_antiaffinity(max_pods):
            yp_client.update_object(
                "pod_set",
                pod_set_id,
                set_updates=[
                    dict(path="/spec/antiaffinity_constraints/0/max_pods", value=max_pods,),
                ],
            )

        pod_ids = []
        for _ in xrange(2):
            pod_ids.append(create_pod())

        wait(lambda: are_pods_assigned(yp_client, pod_ids))

        new_pod_id = create_pod()
        wait(lambda: is_pod_assigned(yp_client, new_pod_id))
        yp_client.remove_object("pod", new_pod_id)

        update_antiaffinity(max_pods=1)

        new_pod_id = create_pod()
        wait(
            lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, new_pod_id))
        )

        update_antiaffinity(max_pods=3)

        wait(lambda: is_pod_assigned(yp_client, new_pod_id))


@pytest.mark.usefixtures("yp_env_configurable")
class TestAntiaffinityConstraintsUniqueBucketLimit(object):
    ANTIAFFINITY_CONSTRAINTS_UNIQUE_BUCKET_LIMIT = 5

    YP_MASTER_CONFIG = {
        "scheduler": {
            "cluster": {
                "antiaffinity_constraints_unique_bucket_limit": ANTIAFFINITY_CONSTRAINTS_UNIQUE_BUCKET_LIMIT,
                "bypass_validation_errors": True,
            }
        }
    }

    def test(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client

        antiaffinity_constraints_names = ["key", "max_pods", "pod_group_id_path"]
        topology_zones = ["node", "rack", "dc"]

        pod_count = 2
        node_pod_limit = 1

        create_nodes(yp_client, 2)

        def prepare_objects_with_pod_group_id_paths(antiaffinity_constraints_buckets):
            pod_set_id = create_pod_set(
                yp_client,
                spec=dict(
                    antiaffinity_constraints=[
                        dict(zip(antiaffinity_constraints_names, bucket))
                        for bucket in antiaffinity_constraints_buckets
                    ]
                ),
            )
            return [
                create_pod_with_boilerplate(
                    yp_client,
                    pod_set_id,
                    spec={"enable_scheduling": True},
                    labels=dict(group_id="test_group_id"),
                )
                for _ in range(pod_count)
            ]

        def is_pending_pod(pod_id):
            scheduling_status = get_pod_scheduling_status(yp_client, pod_id)
            return (
                "error" not in scheduling_status
                and scheduling_status.get("state", None) == "pending"
                and scheduling_status.get("node_id", "") == ""
            )

        def are_pending_pods(pod_ids):
            return all(map(is_pending_pod, pod_ids))

        antiaffinity_constraints_buckets_over_limit = []

        # Topology zone (key) = "node", group_id_path count exceeds limit.
        antiaffinity_constraints_buckets_over_limit.append(
            [
                ("node", node_pod_limit, "/labels/group_id_type_%d" % index)
                for index in xrange(self.ANTIAFFINITY_CONSTRAINTS_UNIQUE_BUCKET_LIMIT + 1)
            ]
        )

        # Topology zone (key) has 3 types, group_id_path has (limit // 3) = 1 variants (and one empty variant).
        # So antiaffinity constraints bucket count is 3 * (1 + 1) = 6 - exceeds limit,
        # though every parameter count is under limit.
        antiaffinity_constraints_buckets_over_limit.append(
            [
                (topology_zone, node_pod_limit, "/labels/group_id_type_%d" % index)
                for topology_zone in topology_zones
                for index in xrange(self.ANTIAFFINITY_CONSTRAINTS_UNIQUE_BUCKET_LIMIT // 3)
            ]
            + [(topology_zone, node_pod_limit + 1) for topology_zone in topology_zones]
        )

        # Take only antiaffinity_constraints_unique_bucket_limit buckets.
        # So antiaffinity constraints bucket count in under limit.
        antiaffinity_constraints_buckets_under_limit = [
            buckets_over_limit[: self.ANTIAFFINITY_CONSTRAINTS_UNIQUE_BUCKET_LIMIT]
            for buckets_over_limit in antiaffinity_constraints_buckets_over_limit
        ]

        assert all(
            map(
                lambda buckets: len(buckets) > self.ANTIAFFINITY_CONSTRAINTS_UNIQUE_BUCKET_LIMIT,
                antiaffinity_constraints_buckets_over_limit,
            )
        )
        assert all(
            map(
                lambda buckets: len(buckets) <= self.ANTIAFFINITY_CONSTRAINTS_UNIQUE_BUCKET_LIMIT,
                antiaffinity_constraints_buckets_under_limit,
            )
        )

        pod_ids_under_limit = flatten(
            prepare_objects_with_pod_group_id_paths(antiaffinity_constraints_buckets)
            for antiaffinity_constraints_buckets in antiaffinity_constraints_buckets_under_limit
        )

        pod_ids_over_limit = flatten(
            prepare_objects_with_pod_group_id_paths(antiaffinity_constraints_buckets)
            for antiaffinity_constraints_buckets in antiaffinity_constraints_buckets_over_limit
        )

        wait(lambda: are_pods_assigned(yp_client, pod_ids_under_limit))

        assert_over_time(lambda: are_pending_pods(pod_ids_over_limit), iter=5, sleep_backoff=3)
