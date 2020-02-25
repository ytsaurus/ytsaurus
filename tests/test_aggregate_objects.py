from yp.common import YtResponseError

import pytest


@pytest.mark.usefixtures("yp_env")
class TestAggregateObjects(object):
    def test_bad_aggregation_function(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.aggregate_objects(
                "pod",
                group_by=["[/meta/pod_set_id]"],
                aggregators=["bad_func([/meta/creation_time])"],
            )

    def test_bad_aggregator(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.aggregate_objects(
                "pod", group_by=["[/meta/pod_set_id]"], aggregators=["avg([/bad_obj])"]
            )

    def test_column_order(self, yp_env):
        yp_client = yp_env.yp_client
        pod_set_id = yp_client.create_object("pod_set")
        memory_limits = [i * 2 ** 30 for i in range(1, 4)]
        vcpu_guarantees = [i * 100 for i in range(1, 4)]
        for memory_limit, vcpu_guarantee in zip(memory_limits, vcpu_guarantees):
            yp_client.create_object(
                "pod",
                attributes={
                    "meta": {"pod_set_id": pod_set_id,},
                    "spec": {
                        "resource_requests": {
                            "memory_limit": memory_limit,
                            "vcpu_guarantee": vcpu_guarantee,
                        },
                    },
                },
            )
        result = yp_client.aggregate_objects(
            "pod",
            group_by=["[/meta/pod_set_id]"],
            aggregators=[
                "max(int64([/spec/resource_requests/memory_limit]))",
                "avg(int64([/spec/resource_requests/vcpu_guarantee]))",
            ],
            filter='[/meta/pod_set_id] = "{}"'.format(pod_set_id),
        )

        assert len(result) == 1
        assert result[0] == [
            pod_set_id,
            max(memory_limits),
            float(sum(vcpu_guarantees)) / len(vcpu_guarantees),
        ]

    def test_bad_group_by(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.aggregate_objects(
                "pod",
                group_by=["[/meta/typo]"],
                aggregators=["max(int64([/spec/resource_requests/memory_limit]))"],
            )

    def test_empty_group_by(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(Exception):
            yp_client.aggregate_objects(
                "pod",
                group_by=[],
                aggregators=["max(int64([/spec/resource_requests/memory_limit]))"],
            )

    def test_only_keys(self, yp_env):
        yp_client = yp_env.yp_client
        pod_set_id = yp_client.create_object("pod_set")
        for _ in range(3):
            yp_client.create_object("pod", attributes={"meta": {"pod_set_id": pod_set_id,},})

        result = yp_client.aggregate_objects(
            "pod",
            group_by=["[/meta/pod_set_id]"],
            filter='[/meta/pod_set_id] = "{}"'.format(pod_set_id),
        )

        assert result == [[pod_set_id]]

    def test_secrets_not_allowed_in_aggregators(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.aggregate_objects(
                "pod",
                group_by=["[/meta/pod_set_id]"],
                aggregators=["sum(int64([/spec/secrets/0/secret_id]))"],
            )

    def test_secrets_not_allowed_in_filter(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.aggregate_objects(
                "pod",
                group_by=["[/meta/pod_set_id]"],
                filter='[/spec/secrets/0/secret_id] = "my_secret"',
            )

    def test_secrets_not_allowed_in_group_by(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            yp_client.aggregate_objects(
                "pod", group_by=["[/spec/secrets/0/secret_id]"],
            )

    def test_evaluator(self, yp_env):
        yp_client = yp_env.yp_client
        with pytest.raises(YtResponseError):
            # raises 'Attribute /status/used cannot be queried'
            yp_client.aggregate_objects(
                "resource",
                group_by=["[/meta/node_id]"],
                aggregators=["[/status/used/memory/capacity]"],
                filter='is_prefix("memory", [/meta/id])',
            )

    def test_annotations_in_filter(self, yp_env):
        yp_client = yp_env.yp_client
        pod_set_id = yp_client.create_object("pod_set")
        yp_client.update_object(
            "pod_set", pod_set_id, set_updates=[{"path": "/annotations/a", "value": "b"}]
        )
        with pytest.raises(YtResponseError):
            yp_client.aggregate_objects(
                "pod_set", group_by=["[/meta/id]"], filter='[/annotations/a] = "b"',
            )

    def test_annotations_in_aggregator(self, yp_env):
        yp_client = yp_env.yp_client
        pod_set_id = yp_client.create_object("pod_set")
        yp_client.update_object(
            "pod_set", pod_set_id, set_updates=[{"path": "/annotations/a", "value": 5}]
        )
        with pytest.raises(YtResponseError):
            yp_client.aggregate_objects(
                "pod_set", group_by=["[/meta/id]"], aggregators=["sum(int64([/annotations/a]))"],
            )

    def test_annotations_in_group_by(self, yp_env):
        yp_client = yp_env.yp_client
        pod_set_id = yp_client.create_object("pod_set")
        yp_client.update_object(
            "pod_set", pod_set_id, set_updates=[{"path": "/annotations/a", "value": 5}]
        )
        with pytest.raises(YtResponseError):
            yp_client.aggregate_objects(
                "pod_set",
                group_by=["int64([/annotations/a])"],
                aggregators=["avg(int64([/meta/creation_time]))"],
            )
