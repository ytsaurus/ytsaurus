from .conftest import (
    are_pods_assigned,
    assert_over_time,
    create_nodes,
    is_pod_assigned,
    wait,
)

from yp.common import YpAuthorizationError, YtResponseError

import pytest


@pytest.mark.userfixtures("yp_env")
class TestNodeSegmentPodConstraints(object):
    def _update_default_node_segment_vcpu_constraint(self, yp_client, multiplier, additive):
        yp_client.update_object(
            "node_segment",
            "default",
            set_updates=[
                {
                    "path": "/spec/pod_constraints",
                    "value": {
                        "vcpu_guarantee_to_limit_ratio": {
                            "multiplier": multiplier,
                            "additive": additive,
                        }
                    },
                }
            ],
        )

    def test_vcpu_guarantee_to_limit_ratio(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1, cpu_total_capacity=10000)

        pod_set_id = yp_client.create_object(
            "pod_set",
            {
                "spec": {
                    "node_segment_id": "default",
                    "violate_node_segment_constraints": {"vcpu_guarantee_to_limit_ratio": False},
                }
            },
        )

        def _violate_node_segment_vcpu_constraint(violate):
            yp_client.update_object(
                "pod_set",
                pod_set_id,
                [
                    {
                        "path": "/spec/violate_node_segment_constraints/vcpu_guarantee_to_limit_ratio",
                        "value": violate,
                    }
                ],
            )

        def _create_pod(vcpu_guarantee, vcpu_limit):
            return yp_client.create_object(
                "pod",
                {
                    "meta": {"pod_set_id": pod_set_id},
                    "spec": {
                        "enable_scheduling": True,
                        "resource_requests": {
                            "vcpu_guarantee": vcpu_guarantee,
                            "vcpu_limit": vcpu_limit,
                            "memory_guarantee": 128 * 1024 * 1024,
                            "memory_limit": 128 * 1024 * 1024,
                        },
                    },
                },
            )

        def _update_vcpu_guarantee_and_limit(pod_id, guarantee, limit):
            yp_client.update_object(
                "pod",
                pod_id,
                set_updates=[
                    {"path": "/spec/resource_requests/vcpu_guarantee", "value": guarantee,},
                    {"path": "/spec/resource_requests/vcpu_limit", "value": limit,},
                ],
            )

        pod_ids = [_create_pod(100, 100), _create_pod(100, 1000)]
        wait(lambda: are_pods_assigned(yp_client, pod_ids))

        self._update_default_node_segment_vcpu_constraint(yp_client, 1.0, 100)

        pod_id = _create_pod(100, 200)
        wait(lambda: is_pod_assigned(yp_client, pod_id))
        with pytest.raises(YtResponseError):
            _update_vcpu_guarantee_and_limit(pod_id, 100, 1000)

        _violate_node_segment_vcpu_constraint(True)
        _update_vcpu_guarantee_and_limit(pod_id, 100, 201)
        assert is_pod_assigned(yp_client, pod_id)

        _violate_node_segment_vcpu_constraint(False)
        pod_id_within_limits = _create_pod(150, 250)
        with pytest.raises(YtResponseError):
            _create_pod(150, 251)
        with pytest.raises(YtResponseError):
            _update_vcpu_guarantee_and_limit(pod_id_within_limits, 149, 250)

        wait(lambda: is_pod_assigned(yp_client, pod_id_within_limits))

        self._update_default_node_segment_vcpu_constraint(yp_client, 2.0, 150)
        assert is_pod_assigned(yp_client, pod_id)

        pod_id_within_limits_2 = _create_pod(150, 450)
        with pytest.raises(YtResponseError):
            _create_pod(150, 451)

        wait(lambda: is_pod_assigned(yp_client, pod_id_within_limits_2))

        self._update_default_node_segment_vcpu_constraint(yp_client, 1.0, 0)

        _update_vcpu_guarantee_and_limit(pod_id, 100, 201)
        _update_vcpu_guarantee_and_limit(pod_id_within_limits_2, 150, 450)

        assert_over_time(
            lambda: are_pods_assigned(
                yp_client, [pod_id, pod_id_within_limits, pod_id_within_limits_2]
            )
        )

        self._update_default_node_segment_vcpu_constraint(yp_client, 1.0, 0)

        yp_client.update_object(
            "node_segment",
            "default",
            remove_updates=[
                {"path": "/spec/pod_constraints/vcpu_guarantee_to_limit_ratio/additive"}
            ],
        )
        with pytest.raises(YtResponseError):
            _update_vcpu_guarantee_and_limit(pod_id, 150, 151)
        _update_vcpu_guarantee_and_limit(pod_id, 150, 150)

        yp_client.update_object(
            "node_segment",
            "default",
            remove_updates=[{"path": "/spec/pod_constraints/vcpu_guarantee_to_limit_ratio"}],
        )
        _update_vcpu_guarantee_and_limit(pod_id, 150, 301)
        assert_over_time(lambda: is_pod_assigned(yp_client, pod_id))

    def test_vcpu_guarantee_to_limit_ratio_validation(self, yp_env):
        yp_client = yp_env.yp_client

        min_multiplier, max_multiplier = 1.0, 1000.0
        min_additive = 0
        pod_vcpu_ratios = [
            (min_multiplier - 0.1, 100),
            (max_multiplier + 0.01, 0),
            (min_multiplier, min_additive - 1),
            (2.0 * min_multiplier, min_additive - 100),
            (min_multiplier, 1000001),
            (0.0, 100),
        ]

        with pytest.raises(YtResponseError):
            self._update_default_node_segment_vcpu_constraint(yp_client, 0.0, 100)

        for multiplier, additive in pod_vcpu_ratios:
            with pytest.raises(YtResponseError):
                yp_client.create_object(
                    "node_segment",
                    {
                        "spec": {
                            "node_filter": "",
                            "pod_constraints": {
                                "vcpu_guarantee_to_limit_ratio": {
                                    "multiplier": multiplier,
                                    "additive": additive,
                                }
                            },
                        },
                    },
                )
            with pytest.raises(YtResponseError):
                self._update_default_node_segment_vcpu_constraint(yp_client, multiplier, additive)

        self._update_default_node_segment_vcpu_constraint(yp_client, max_multiplier, min_additive)
        self._update_default_node_segment_vcpu_constraint(yp_client, min_multiplier, min_additive)
        self._update_default_node_segment_vcpu_constraint(yp_client, min_multiplier, 1000000)

        # We can't remove multiplier without removing "/spec/pod_constraints/vcpu_guarantee_to_limit_ratio",
        # because when "../vcpu_guarantee_to_limit_ratio" exists, multiplier is considered as 0.0, but it can't be less than 1.0.
        with pytest.raises(YtResponseError):
            yp_client.update_object(
                "node_segment",
                "default",
                remove_updates=[
                    {"path": "/spec/pod_constraints/vcpu_guarantee_to_limit_ratio/multiplier"}
                ],
            )

    def test_vcpu_guarantee_to_limit_ratio_acls(self, yp_env):
        yp_client_root = yp_env.yp_client

        node_segment_id = yp_client_root.get_object("node_segment", "default", ["/meta/id"])[0]

        username = "simple_user"

        yp_client_root.create_object("user", attributes={"meta": {"id": username}})
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": username}) as yp_client_simple_user:

            def _create_pod_set(yp_client, violate):
                return yp_client.create_object(
                    "pod_set",
                    attributes={
                        "spec": {
                            "node_segment_id": "default",
                            "violate_node_segment_constraints": {
                                "vcpu_guarantee_to_limit_ratio": violate
                            },
                        },
                    },
                )

            def _update_pod_set_violation(yp_client, violate):
                yp_client.update_object(
                    "pod_set",
                    pod_set_id,
                    [
                        {
                            "path": "/spec/violate_node_segment_constraints",
                            "value": {"vcpu_guarantee_to_limit_ratio": violate},
                        }
                    ],
                )

            with pytest.raises(YpAuthorizationError):
                _create_pod_set(yp_client_simple_user, True)

            pod_set_id = _create_pod_set(yp_client_simple_user, False)
            with pytest.raises(YpAuthorizationError):
                _update_pod_set_violation(yp_client_simple_user, True)

            _update_pod_set_violation(yp_client_root, True)
            _update_pod_set_violation(yp_client_simple_user, True)
            _update_pod_set_violation(yp_client_simple_user, False)

            yp_client_root.update_object(
                "node_segment",
                node_segment_id,
                set_updates=[
                    {
                        "path": "/meta/acl",
                        "value": [
                            {
                                "action": "allow",
                                "permissions": ["use"],
                                "subjects": [username],
                                "attributes": [
                                    "/access/pod_constraints/violate/vcpu_guarantee_to_limit_ratio"
                                ],
                            }
                        ],
                    }
                ],
            )
            yp_env.sync_access_control()

            _update_pod_set_violation(yp_client_simple_user, True)
            _update_pod_set_violation(yp_client_simple_user, False)
