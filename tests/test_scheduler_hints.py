from .conftest import (
    are_pods_assigned,
    are_pods_touched_by_scheduler,
    create_nodes,
    create_pod_set,
    get_pod_scheduling_status,
    is_error_pod_scheduling_status,
    is_pod_assigned,
)

from yp.common import wait, YtResponseError

import pytest

import random


@pytest.mark.usefixtures("yp_env")
class TestSchedulerHints(object):
    def _create_pod_with_scheduling_hint(
        self, yp_client, pod_set_id, node_filter, scheduling_hints, enable_scheduling=True
    ):
        pod_id = yp_client.create_object(
            "pod",
            dict(
                meta=dict(pod_set_id=pod_set_id),
                spec=dict(
                    resource_requests=(dict(vcpu_guarantee=100, vcpu_limit=100)),
                    node_filter=node_filter,
                ),
            ),
        )
        for hint_node_id, strong in scheduling_hints:
            yp_client.add_scheduling_hint(pod_id, hint_node_id, strong)
        if enable_scheduling:
            yp_client.update_object(
                "pod", pod_id, set_updates=[dict(path="/spec/enable_scheduling", value=True)]
            )
        return pod_id

    def test_weak(self, yp_env):
        yp_client = yp_env.yp_client

        small_node_ids = create_nodes(yp_client, 2, cpu_total_capacity=200)
        bigger_node_ids = create_nodes(yp_client, 2, cpu_total_capacity=300)

        pod_set_id = create_pod_set(yp_client)
        pod_ids = yp_client.create_objects(
            [
                (
                    "pod",
                    dict(
                        meta=dict(pod_set_id=pod_set_id),
                        spec=dict(resource_requests=(dict(vcpu_guarantee=100, vcpu_limit=100))),
                    ),
                )
            ]
            * 2
        )

        scheduling_hint_requests = [
            [(small_node_ids[0], False), (bigger_node_ids[0], False)],
            [(small_node_ids[0], False), (bigger_node_ids[1], False)],
        ]

        for node_id, strong in scheduling_hint_requests[0]:
            yp_client.add_scheduling_hint(pod_ids[0], node_id, strong)

        for node_id, strong in scheduling_hint_requests[1]:
            yp_client.add_scheduling_hint(pod_ids[1], node_id, strong)

        scheduling_hint_responses = yp_client.get_objects(
            "pod", pod_ids, selectors=["/spec/scheduling/hints"]
        )[0]
        for scheduling_hint_request, scheduling_hint_response in zip(
            scheduling_hint_requests, scheduling_hint_responses
        ):
            for request, response in zip(scheduling_hint_request, scheduling_hint_response):
                for key, value in zip(["node_id", "strong"], request):
                    assert value == response[key]

        yp_client.update_objects(
            [
                dict(
                    object_type="pod",
                    object_id=pod_id,
                    set_updates=[dict(path="/spec/enable_scheduling", value=True,)],
                )
                for pod_id in pod_ids
            ]
        )

        wait(lambda: are_pods_assigned(yp_client, pod_ids))
        assert (
            list(
                map(
                    lambda response: response[0],
                    yp_client.get_objects("pod", pod_ids, selectors=["/status/scheduling/node_id"]),
                )
            )
            == bigger_node_ids
        )

    def test_weak_and_strong(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = create_nodes(yp_client, 3, cpu_total_capacity=100)
        random.shuffle(node_ids)

        pod_set_id = create_pod_set(yp_client)
        pod_id = self._create_pod_with_scheduling_hint(
            yp_client,
            pod_set_id,
            "true",
            [(node_ids[0], False), (node_ids[1], False), (node_ids[2], True)],
        )

        wait(lambda: is_pod_assigned(yp_client, pod_id))
        assert (
            yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
            == node_ids[2]
        )

    def test_strong_hints(self, yp_env):
        yp_client = yp_env.yp_client

        node_count = 3
        node_ids = create_nodes(yp_client, node_count, cpu_total_capacity=100)
        random.shuffle(node_ids)

        pod_set_id = create_pod_set(yp_client)
        contradictory_hints_pod_id = self._create_pod_with_scheduling_hint(
            yp_client, pod_set_id, "true", [(node_id, True) for node_id in node_ids]
        )

        # Contradictory strong hints cannot be satisfied.
        wait(
            lambda: is_error_pod_scheduling_status(
                get_pod_scheduling_status(yp_client, contradictory_hints_pod_id)
            )
        )

        scheduling_hints = yp_client.get_object(
            "pod", contradictory_hints_pod_id, selectors=["/spec/scheduling/hints"]
        )[0]
        scheduling_hint_uuids = [hint["uuid"] for hint in scheduling_hints]

        yp_client.add_scheduling_hint(
            contradictory_hints_pod_id,
            scheduling_hints[-1]["node_id"],
            scheduling_hints[-1]["strong"],
        )
        for uuid in scheduling_hint_uuids[:-1]:
            yp_client.remove_scheduling_hint(contradictory_hints_pod_id, uuid)

        scheduling_hints = yp_client.get_object(
            "pod", contradictory_hints_pod_id, selectors=["/spec/scheduling/hints"]
        )[0]
        assert len(scheduling_hints) == 2
        assert scheduling_hints[0]["node_id"] == node_ids[-1]

        # Two strong hints with one node_id still can be satisfied.
        wait(lambda: is_pod_assigned(yp_client, contradictory_hints_pod_id))
        assert (
            yp_client.get_object(
                "pod", contradictory_hints_pod_id, selectors=["/status/scheduling/node_id"]
            )[0]
            == node_ids[-1]
        )

    def test_cannot_add_hint_with_nonexisting_node(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1, cpu_total_capacity=100)

        pod_set_id = create_pod_set(yp_client)
        pod_id = yp_client.create_object(
            "pod",
            dict(
                meta=dict(pod_set_id=pod_set_id),
                spec=dict(resource_requests=(dict(vcpu_guarantee=100, vcpu_limit=100))),
            ),
        )

        for strong in [True, False]:
            with pytest.raises(YtResponseError):
                yp_client.add_scheduling_hint(pod_id, "nonexisting", strong)

    def test_strong_hint_with_node_filter(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = create_nodes(yp_client, 3, cpu_total_capacity=100)
        random.shuffle(node_ids)
        yp_client.update_objects(
            [
                {
                    "object_type": "node",
                    "object_id": node_id,
                    "set_updates": [{"path": "/labels/filter_value", "value": index + 1}],
                }
                for index, node_id in enumerate(node_ids)
            ]
        )

        pod_set_id = create_pod_set(yp_client)

        pod_id_1 = self._create_pod_with_scheduling_hint(
            yp_client, pod_set_id, "[/labels/filter_value]=1", [(node_ids[0], True)]
        )
        pod_id_2 = self._create_pod_with_scheduling_hint(
            yp_client, pod_set_id, "[/labels/filter_value]>2", [(node_ids[1], True)]
        )

        wait(lambda: is_pod_assigned(yp_client, pod_id_1))
        assert (
            yp_client.get_object("pod", pod_id_1, selectors=["/status/scheduling/node_id"])[0]
            == node_ids[0]
        )

        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id_2)))

    def test_weak_hints_with_node_filter(self, yp_env):
        yp_client = yp_env.yp_client

        small_node_ids = create_nodes(yp_client, 2, cpu_total_capacity=100)
        big_node_id = create_nodes(yp_client, 1, cpu_total_capacity=200)[0]

        random.shuffle(small_node_ids)
        yp_client.update_objects(
            [
                {
                    "object_type": "node",
                    "object_id": node_id,
                    "set_updates": [{"path": "/labels/filter_value", "value": index + 1}],
                }
                for index, node_id in enumerate(small_node_ids)
            ]
        )

        pod_set_id = create_pod_set(yp_client)

        pod_id_1 = self._create_pod_with_scheduling_hint(
            yp_client, pod_set_id, "[/labels/filter_value]=1", [(small_node_ids[0], False)]
        )
        pod_id_2 = self._create_pod_with_scheduling_hint(
            yp_client, pod_set_id, "[/labels/filter_value]=2", [(big_node_id, False)]
        )

        wait(lambda: are_pods_assigned(yp_client, [pod_id_1, pod_id_2]))
        assert (
            yp_client.get_object("pod", pod_id_1, selectors=["/status/scheduling/node_id"])[0]
            == small_node_ids[0]
        )
        assert (
            yp_client.get_object("pod", pod_id_2, selectors=["/status/scheduling/node_id"])[0]
            == small_node_ids[1]
        )

    def test_remove_node_after_setting_hint(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = create_nodes(yp_client, 2, cpu_total_capacity=200)
        pod_set_id = create_pod_set(yp_client)

        pod_id_1 = self._create_pod_with_scheduling_hint(
            yp_client, pod_set_id, "true", [(node_ids[1], True)], False
        )
        pod_id_2 = self._create_pod_with_scheduling_hint(
            yp_client, pod_set_id, "true", [(node_ids[1], False)], False
        )

        yp_client.remove_object("node", node_ids[1])

        for pod_id in [pod_id_1, pod_id_2]:
            yp_client.update_object(
                "pod", pod_id, set_updates=[dict(path="/spec/enable_scheduling", value=True)]
            )

        wait(lambda: are_pods_touched_by_scheduler(yp_client, [pod_id_1, pod_id_2]))
        assert is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id_1))
        assert is_pod_assigned(yp_client, pod_id_2)

    def test_cannot_change_scheduling_hints_manually(self, yp_env):
        yp_client = yp_env.yp_client

        node_ids = create_nodes(yp_client, 2, cpu_total_capacity=100)
        pod_set_id = create_pod_set(yp_client)
        pod_id = self._create_pod_with_scheduling_hint(
            yp_client, pod_set_id, "true", [(node_ids[0], False), (node_ids[1], True)], False
        )

        for relative_path, value in [
            ("/scheduling", {}),
            ("/scheduling/hints", {}),
            ("/scheduling/hints/0", {}),
        ]:
            path = "/spec" + relative_path

            with pytest.raises(YtResponseError):
                yp_client.update_object("pod", pod_id, set_updates=[dict(path=path, value=value)])

            with pytest.raises(YtResponseError):
                yp_client.update_object("pod", pod_id, remove_updates=[dict(path=path)])

        for relative_path, value in [
            ("uuid", "other-uuid"),
            ("creation_time", {}),
            ("creation_time/seconds", 12),
            ("creation_time/nanos", 123456789),
            ("strong", True),
            ("node_id", node_ids[1]),
        ]:
            path = "/spec/scheduling/hints/0/" + relative_path

            with pytest.raises(YtResponseError):
                yp_client.update_object("pod", pod_id, set_updates=[dict(path=path, value=value)])

            with pytest.raises(YtResponseError):
                yp_client.update_object("pod", pod_id, remove_updates=[dict(path=path)])

        scheduling_hints = yp_client.get_object("pod", pod_id, ["/spec/scheduling/hints"])[0]

        indices_buckets = [
            [1, 0],
            [0],
            [0, 1, 1],
        ]

        for indices in indices_buckets:
            with pytest.raises(YtResponseError):
                yp_client.update_object(
                    "pod",
                    pod_id,
                    set_updates=[
                        dict(
                            path="/spec/scheduling/hints",
                            value=[scheduling_hints[index] for index in indices],
                        )
                    ],
                )

        yp_client.update_object(
            "pod",
            pod_id,
            set_updates=[
                dict(
                    path="/spec/scheduling/hints", value=[scheduling_hints[0], scheduling_hints[1]]
                )
            ],
        )
