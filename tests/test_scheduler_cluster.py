from .conftest import (
    DEFAULT_POD_SET_SPEC,
    are_pods_assigned,
    attach_pod_set_to_disruption_budget,
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
    is_assigned_pod_scheduling_status,
    run_eviction_acknowledger,
    wait,
    wait_pod_is_assigned,
)

from yt.common import update

from yt.packages.six.moves import xrange

import pytest


class BaseTestSchedulerCluster(object):
    def impl(self, yp_client):
        yp_client.create_object("ip4_address_pool")

        internet_address_count = 10
        for i in xrange(internet_address_count):
            yp_client.create_object(
                object_type="internet_address",
                attributes=dict(
                    meta=dict(
                        ip4_address_pool_id="default_ip4_address_pool",
                    ),
                    spec=dict(
                        ip4_address="1.2.3.{}".format(i + 1),
                        network_module_id="VLA01,VLA02,VLA03",
                    ),
                ),
            )

        pod_cpu = 100

        create_nodes(yp_client, node_count=10, cpu_total_capacity=pod_cpu * 5)

        yp_client.create_object(
            "account",
            attributes=dict(
                meta=dict(id="test"),
                spec=dict(parent_id="tmp"),
            ),
        )

        pod_disruption_budget_id = yp_client.create_object(
            "pod_disruption_budget",
            attributes=dict(
                spec=dict(max_pods_unavailable=1, max_pod_disruptions_between_syncs=10),
            ),
        )

        pod_set_ids = []
        for _ in xrange(5):
            pod_set_ids.append(create_pod_set(yp_client))

        pod_set_id = pod_set_ids[0]

        attach_pod_set_to_disruption_budget(yp_client, pod_set_id, pod_disruption_budget_id)

        pod_count = 10
        pod_ids = []
        for i in xrange(pod_count):
            pod_ids.append(create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                dict(
                    enable_scheduling=True,
                    resource_requests=dict(vcpu_guarantee=pod_cpu),
                ),
            ))

        wait(lambda: are_pods_assigned(yp_client, pod_ids))


@pytest.mark.usefixtures("yp_env")
class TestSchedulerCluster(BaseTestSchedulerCluster):
    def _validate_scheduler_liveness(self, yp_client):
        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(enable_scheduling=True),
        )
        wait_pod_is_assigned(yp_client, pod_id)

    def test_cluster_load(self, yp_env):
        yp_client = yp_env.yp_client
        self.impl(yp_client)
        self._validate_scheduler_liveness(yp_client)


@pytest.mark.usefixtures("yp_env_configurable")
class TestSchedulerClusterWithHeavyScheduler(BaseTestSchedulerCluster):
    START_YP_HEAVY_SCHEDULER = True

    def _validate_heavy_scheduler_liveness(self, yp_client):
        responses = yp_client.select_objects(
            "resource",
            filter="[/meta/kind] = \"cpu\"",
            selectors=["/meta/node_id", "/status/free/cpu/capacity", "/spec/cpu/total_capacity"],
        )
        pod_cpu = 0
        for response in responses:
            node_id, free_cpu, total_cpu = response
            if free_cpu < total_cpu:
                pod_cpu = max(pod_cpu, free_cpu + 1)
        assert pod_cpu > 0
        pod_set_id = create_pod_set(yp_client)
        pod_id = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(enable_scheduling=True, resource_requests=dict(vcpu_guarantee=pod_cpu)),
        )

        run_eviction_acknowledger(yp_client, iteration_count=60, sleep_time=1)

        responses = yp_client.select_objects(
            "pod",
            selectors=["/status/scheduling"],
        )
        scheduling_statuses = list(map(lambda response: response[0], responses))
        assert all(map(is_assigned_pod_scheduling_status, scheduling_statuses))

    def test_cluster_load(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        self.impl(yp_client)
        self._validate_heavy_scheduler_liveness(yp_client)
