from .conftest import (
    are_pods_assigned,
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
    get_pod_scheduling_status,
    is_error_pod_scheduling_status,
)

from yp.common import wait, WaitFailed

from yt.packages.six.moves import xrange

import pytest


@pytest.mark.usefixtures("yp_env")
class TestSchedulerNodeScoreFeatures(object):
    def _create_scheduler_config(self, **global_resource_allocator):
        return dict(scheduler=dict(global_resource_allocator=global_resource_allocator))

    def test_invalid_filter_query(self, yp_env):
        yp_client = yp_env.yp_client

        def check_scheduler_liveness(**wait_options):
            create_nodes(yp_client, node_count=1)
            pod_set_id = create_pod_set(yp_client)
            pod_id = create_pod_with_boilerplate(
                yp_client, pod_set_id, spec=dict(enable_scheduling=True),
            )
            wait(lambda: are_pods_assigned(yp_client, [pod_id]), **wait_options)

        def check_scheduler_lifelessness():
            with pytest.raises(WaitFailed):
                check_scheduler_liveness(iter=20, sleep_backoff=1.0)

        yp_env.reset_cypress_config_patch()
        check_scheduler_liveness()

        broken_filter_queries = (
            "abcacba",
            '[/meta/id] = "node1"',
            '[/lables/avx2] = "pampam"',
            "/labels = null",
        )

        correct_filter_query = "[/labels/cpu_flags/avx2] = %true"

        for broken_filter_query in broken_filter_queries:
            node_score_config = dict(
                features=[
                    dict(filter_query=broken_filter_query),
                    dict(filter_query=correct_filter_query),
                ]
            )
            yp_env.set_cypress_config_patch(
                self._create_scheduler_config(node_score=node_score_config,)
            )
            yp_env.sync_scheduler()
            check_scheduler_lifelessness()

        yp_env.set_cypress_config_patch(
            self._create_scheduler_config(
                node_score=dict(features=[dict(filter_query=correct_filter_query, weight=42)]),
            )
        )
        check_scheduler_liveness()

    def _test_scheduling(self, yp_env, scheduler_config, node_limit=None):
        yp_client = yp_env.yp_client

        assert "node_score" not in scheduler_config
        scheduler_config["node_score"] = dict(
            features=[
                dict(filter_query="[/labels/cpu_flags/avx2] = %true", weight=2),
                dict(filter_query="[/labels/stability] = %false", weight=-1),
            ]
        )

        yp_env.set_cypress_config_patch(self._create_scheduler_config(**scheduler_config))
        yp_env.sync_scheduler()

        # Ordered from more to less preferrable nodes.
        node_per_template = 2
        node_templates = (
            dict(stability=False),
            None,
            dict(cpu_flags=dict(avx2=True), stability=False),
            dict(cpu_flags=dict(avx2=True)),
        )

        node_count = node_per_template * len(node_templates)
        if node_limit is not None:
            assert (
                node_count <= node_limit
            ), "General test is not designed to satisfy node limit of {}".format(node_limit)

        node_ids_per_template = []
        pod_cpu_capacity = 100
        pod_memory_capacity = 2 ** 32
        pod_per_node = 2
        for labels in node_templates:
            node_ids_per_template.append(
                set(
                    create_nodes(
                        yp_client,
                        node_count=node_per_template,
                        cpu_total_capacity=pod_per_node * pod_cpu_capacity,
                        memory_total_capacity=pod_per_node * pod_memory_capacity,
                        labels=labels,
                    ),
                )
            )

        def get_template_id(node_id):
            result = None
            for template_id, node_ids in enumerate(node_ids_per_template):
                if node_id in node_ids:
                    assert result is None
                    result = template_id
            return result

        def create_pod(pod_set_id):
            return create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                spec=dict(
                    enable_scheduling=True,
                    resource_requests=dict(
                        vcpu_guarantee=pod_cpu_capacity, memory_limit=pod_memory_capacity,
                    ),
                ),
            )

        pod_set_id = create_pod_set(yp_client)
        for expected_template_id in xrange(len(node_templates)):
            pod_ids = []
            for _ in xrange(pod_per_node * node_per_template):
                pod_ids.append(create_pod(pod_set_id))
            wait(lambda: are_pods_assigned(yp_client, pod_ids))

            response = yp_client.get_objects(
                "pod", pod_ids, selectors=["/status/scheduling/node_id"],
            )
            for pod_index, pod_response in enumerate(response):
                node_id = pod_response[0]
                template_id = get_template_id(node_id)
                assert expected_template_id == template_id

        pod_id = create_pod(pod_set_id)
        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

    def test_scheduling(self, yp_env):
        self._test_scheduling(
            yp_env, scheduler_config=dict(pod_node_score=dict(type="node_random_hash"),),
        )

    def test_scheduling_with_disabled_every_node_selection(self, yp_env):
        # Allocator with disabled every node selection strategy
        # only considers 10 random nodes per iteration.
        self._test_scheduling(
            yp_env,
            scheduler_config=dict(
                every_node_selection_strategy=dict(enable=False),
                pod_node_score=dict(type="node_random_hash"),
            ),
            node_limit=10,
        )
