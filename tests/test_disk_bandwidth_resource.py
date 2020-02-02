from .conftest import (
    DEFAULT_ACCOUNT_ID,
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
    get_pod_scheduling_status,
    is_assigned_pod_scheduling_status,
    is_error_pod_scheduling_status,
)

from yp.common import wait, YtResponseError

import pytest


@pytest.mark.usefixtures("yp_env")
class TestDiskBandwidthResource(object):
    def _get_pod_disk_volume_allocation(self, yp_client, pod_id):
        pod_disk_volume_allocations = yp_client.get_object(
            "pod",
            pod_id,
            selectors=["/status/disk_volume_allocations"]
        )[0]
        assert len(pod_disk_volume_allocations) == 1
        return pod_disk_volume_allocations[0]

    def test(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1)

        pod_set_id = create_pod_set(yp_client)

        pod_capacity = 10 ** 9
        pod_bandwidth_guarantee = 10 ** 8
        pod_storage_class = "hdd"

        pod_spec = dict(
            enable_scheduling=True,
            disk_volume_requests=[
                dict(
                    id="disk1",
                    storage_class=pod_storage_class,
                    quota_policy=dict(
                        capacity=pod_capacity,
                        bandwidth_guarantee=pod_bandwidth_guarantee,
                    ),
                ),
            ],
        )

        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec)

        # Test. There's no node with configured bandwidth resource.
        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

        read_bandwidth_factor = 0.5
        write_bandwidth_factor = 1.33
        read_operation_rate_divisor = 10.0 ** 6

        def create_node(total_bandwidth):
            node_ids = create_nodes(yp_client, 1, disk_specs=[dict(
                total_bandwidth=total_bandwidth,
                read_bandwidth_factor=read_bandwidth_factor,
                write_bandwidth_factor=write_bandwidth_factor,
                read_operation_rate_divisor=read_operation_rate_divisor,
                # Parameter #write_operation_rate_divisor is missed intentionally.
            )])
            assert len(node_ids) == 1
            return node_ids[0]

        node_id = create_node(int(2.5 * pod_bandwidth_guarantee))

        # Test. There's at least one node with available bandwidth resource.
        wait(lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

        # Test. Bandwidth / operation rate guarantees are calculated correctly.
        # Test. Bandwidth / operation rate limits are missed because pod bandwidth limit parameter is missed.
        volume_allocation = self._get_pod_disk_volume_allocation(yp_client, pod_id)
        assert volume_allocation["read_bandwidth_guarantee"] == int(pod_bandwidth_guarantee * read_bandwidth_factor)
        assert "read_bandwidth_limit" not in volume_allocation
        assert volume_allocation["write_bandwidth_guarantee"] == int(pod_bandwidth_guarantee * write_bandwidth_factor)
        assert "write_bandwidth_limit" not in volume_allocation
        assert volume_allocation["read_operation_rate_guarantee"] == int(pod_bandwidth_guarantee / read_operation_rate_divisor)
        assert "read_operation_rate_limit" not in volume_allocation
        assert volume_allocation["write_operation_rate_guarantee"] == 0
        assert "write_operation_rate_limit" not in volume_allocation

        # Test. Bandwidth / operation rate limits are calculated correctly.
        pod_bandwidth_limit = pod_bandwidth_guarantee * 4

        pod_spec2 = dict(
            enable_scheduling=True,
            disk_volume_requests=[
                dict(
                    id="disk1",
                    storage_class=pod_storage_class,
                    quota_policy=dict(
                        capacity=pod_capacity,
                        bandwidth_guarantee=pod_bandwidth_guarantee,
                        bandwidth_limit=pod_bandwidth_limit,
                    ),
                ),
            ],
        )

        pod_id2 = create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec2)

        wait(lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id2)))

        volume_allocation2 = self._get_pod_disk_volume_allocation(yp_client, pod_id2)
        assert volume_allocation2["read_bandwidth_limit"] == int(pod_bandwidth_limit * read_bandwidth_factor)
        assert volume_allocation2["write_bandwidth_limit"] == int(pod_bandwidth_limit * write_bandwidth_factor)
        assert volume_allocation2["read_operation_rate_limit"] == int(pod_bandwidth_limit / read_operation_rate_divisor)
        assert volume_allocation2["write_operation_rate_limit"] == 0

        # Test. Bandwidth accounting is correct.
        account_disk_totals = yp_client.get_object(
            "account",
            DEFAULT_ACCOUNT_ID,
            selectors=["/status/resource_usage/per_segment/default/disk_per_storage_class/{}".format(pod_storage_class)],
        )[0]

        assert account_disk_totals["bandwidth"] == 2 * pod_bandwidth_guarantee

        # Test. Exclusive disk policy.
        pod_spec3 = dict(
            enable_scheduling=True,
            disk_volume_requests=[
                dict(
                    id="disk1",
                    storage_class=pod_storage_class,
                    exclusive_policy=dict(
                        min_capacity=pod_capacity,
                        min_bandwidth=pod_bandwidth_guarantee,
                    ),
                ),
            ],
        )
        exclusive_pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec3)

        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, exclusive_pod_id)))
        create_node(pod_bandwidth_guarantee)
        wait(lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, exclusive_pod_id)))

        exclusive_volume_allocation = self._get_pod_disk_volume_allocation(yp_client, exclusive_pod_id)
        assert exclusive_volume_allocation["read_bandwidth_guarantee"] == int(pod_bandwidth_guarantee * read_bandwidth_factor)
        assert "read_bandwidth_limit" not in exclusive_volume_allocation
        assert exclusive_volume_allocation["write_bandwidth_guarantee"] == int(pod_bandwidth_guarantee * write_bandwidth_factor)
        assert "write_bandwidth_limit" not in exclusive_volume_allocation
        assert exclusive_volume_allocation["read_operation_rate_guarantee"] == int(pod_bandwidth_guarantee / read_operation_rate_divisor)
        assert "read_operation_rate_limit" not in exclusive_volume_allocation
        assert exclusive_volume_allocation["write_operation_rate_guarantee"] == 0
        assert "write_operation_rate_limit" not in exclusive_volume_allocation

        # Test. All nodes with configured bandwidth resource have less resource than needed.
        pod_id3 = create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec)
        wait(lambda: is_error_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id3)))

        # Test. Node disk resource status must be updated.
        resource_status_response = yp_client.select_objects(
            "resource",
            selectors=["/status"],
            filter="[/meta/node_id] = \"{}\" and [/meta/kind] = \"disk\"".format(node_id)
        )
        assert len(resource_status_response) == 1
        resource_status = resource_status_response[0][0]

        scheduled_pod_ids = set()
        for scheduled_allocation in resource_status["scheduled_allocations"]:
            scheduled_pod_id = scheduled_allocation["pod_id"]
            assert scheduled_pod_id not in scheduled_pod_ids
            assert scheduled_allocation["disk"]["bandwidth"] == pod_bandwidth_guarantee
            scheduled_pod_ids.add(scheduled_pod_id)
        assert set([pod_id, pod_id2]) == scheduled_pod_ids

        assert 2 * pod_bandwidth_guarantee == resource_status["used"]["disk"]["bandwidth"]

    def test_factors_and_divisors_validation(self, yp_env):
        yp_client = yp_env.yp_client

        def get_disk_specs(parameter_name, value):
            disk_spec = {
                "total_bandwidth": 1,
            }
            disk_spec[parameter_name] = value
            return [disk_spec]

        for bad_value in (1e-9, -100.0, 0.0, -1e-9, 1e10):
            for parameter_name in ("read_operation_rate_divisor",
                                   "write_operation_rate_divisor",
                                   "read_bandwidth_factor",
                                   "write_bandwidth_factor"):
                with pytest.raises(YtResponseError) as error:
                    create_nodes(yp_client, 1, disk_specs=get_disk_specs(parameter_name, bad_value))
                create_nodes(yp_client, 1, disk_specs=get_disk_specs(parameter_name, 1.0))

    def test_update_limits_without_rescheduling(self, yp_env):
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1, disk_specs=[dict(
            read_bandwidth_factor=1.0,
            write_bandwidth_factor=1.0,
            read_operation_rate_divisor=1.0,
            write_operation_rate_divisor=1.0,
        )])

        pod_set_id = create_pod_set(yp_client)

        base_quota_policy = dict(capacity=10 ** 9)
        pod_spec = dict(
            enable_scheduling=True,
            disk_volume_requests=[
                dict(
                    id="disk1",
                    storage_class="hdd",
                    quota_policy=base_quota_policy,
                ),
            ],
        )

        def validate_disk_volume_allocation_limits(pod_id, bandwidth_limit):
            disk_volume_allocation = self._get_pod_disk_volume_allocation(yp_client, pod_id)
            field_names = (
                "read_bandwidth_limit",
                "write_bandwidth_limit",
                "read_operation_rate_limit",
                "write_operation_rate_limit",
            )
            for field_name in field_names:
                assert disk_volume_allocation.get(field_name, None) == bandwidth_limit

        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, pod_spec)
        wait(lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

        validate_disk_volume_allocation_limits(pod_id, None)
        for bandwidth_limit in (100, 200, None, 300):
            quota_policy = base_quota_policy.copy()
            if bandwidth_limit is not None:
                quota_policy["bandwidth_limit"] = bandwidth_limit
            yp_client.update_object(
                "pod",
                pod_id,
                set_updates=[
                    dict(
                        path="/spec/disk_volume_requests/0/quota_policy",
                        value=quota_policy,
                    ),
                ],
            )
            validate_disk_volume_allocation_limits(pod_id, bandwidth_limit)
