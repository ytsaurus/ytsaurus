from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
)
from yt_commands import (
    authors, print_debug, wait, wait_no_assert,
    create, ls, get, set, exists, create_pool, create_pool_tree,
    write_table, run_test_vanilla, update_scheduler_config)

from yt_helpers import profiler_factory, read_structured_log, write_log_barrier

from yt.common import YtError

import pytest

import os
import time

##################################################################


def get_by_composite_key(item, composite_key, default=None):
    current_item = item
    for part in composite_key:
        if part not in current_item:
            return default
        else:
            current_item = current_item[part]
    return current_item

##################################################################


class TestResourceMetering(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 5
    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "resource_metering": {
                "default_abc_id": 42,
            },
            "resource_metering_period": 1000,
        }
    }
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "resource_limits": {"user_slots": 3, "cpu": 3}
            }
        }
    }

    @classmethod
    def setup_class(cls):
        super(TestResourceMetering, cls).setup_class()
        set("//sys/@cluster_name", "my_cluster")

    def _extract_metering_records_from_log(self, last=True, schema=None):
        """ Returns dict from metering key to last record with this key. """
        scheduler_log_file = os.path.join(self.path_to_run, "logs/scheduler-0.json.log")
        scheduler_address = ls("//sys/scheduler/instances")[0]
        scheduler_barrier = write_log_barrier(scheduler_address)

        events = read_structured_log(scheduler_log_file, to_barrier=scheduler_barrier,
                                     row_filter=lambda e: "event_type" not in e)

        reports = {}
        for entry in events:
            if "abc_id" not in entry:
                continue

            if schema is not None and entry["schema"] != schema:
                continue

            key = (
                entry["abc_id"],
                entry["labels"]["pool_tree"],
                entry["labels"]["pool"],
            )
            if last:
                reports[key] = (entry["tags"], entry["usage"])
            else:
                if key not in reports:
                    reports[key] = []
                reports[key].append((entry["tags"], entry["usage"]))

        return reports

    def _validate_metering_records(self, root_key, desired_metering_data, event_key_to_last_record, precision=None):
        if root_key not in event_key_to_last_record:
            assert False, "Root key is missing"

        for key, desired_data in desired_metering_data.items():
            for resource_key, desired_value in desired_data.items():
                tags, usage = event_key_to_last_record.get(key, ({}, {}))
                observed_value = get_by_composite_key(tags, resource_key.split("/"), default=0)
                if isinstance(desired_value, int):
                    observed_value = int(observed_value)
                is_equal = False
                if precision is None:
                    is_equal = observed_value == desired_value
                else:
                    is_equal = abs(observed_value - desired_value) < precision
                assert is_equal, "Value mismatch (abc_key: {}, resource_key: {}, observed: {}, desired: {})"\
                    .format(key, resource_key, observed_value, desired_value)

    @authors("ignat")
    def test_resource_metering_log(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])

        create_pool_tree("yggdrasil", wait_for_orchid=False)
        set("//sys/pool_trees/default/@config/nodes_filter", "!other")
        set("//sys/pool_trees/yggdrasil/@config/nodes_filter", "other")

        nodes = ls("//sys/cluster_nodes")
        for node in nodes[:4]:
            set("//sys/cluster_nodes/" + node + "/@user_tags/end", "other")

        create_pool(
            "abcless",
            pool_tree="yggdrasil",
            attributes={
                "strong_guarantee_resources": {"cpu": 4},
            },
            wait_for_orchid=False,
        )

        create_pool(
            "pixies",
            pool_tree="yggdrasil",
            attributes={
                "strong_guarantee_resources": {"cpu": 3},
                "abc": {"id": 1, "slug": "pixies", "name": "Pixies"},
            },
            wait_for_orchid=False,
        )

        create_pool(
            "francis",
            pool_tree="yggdrasil",
            attributes={
                "strong_guarantee_resources": {"cpu": 1},
                "abc": {"id": 2, "slug": "francis", "name": "Francis"},
            },
            parent_name="pixies",
            wait_for_orchid=False,
        )

        create_pool(
            "misirlou",
            pool_tree="yggdrasil",
            parent_name="pixies",
            # Intentionally wait for pool creation.
            wait_for_orchid=True,
        )

        create_pool(
            "nidhogg",
            pool_tree="yggdrasil",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "burst",
                    "resource_flow": {"cpu": 5},
                    "burst_guarantee_resources": {"cpu": 6},
                },
                "abc": {"id": 3, "slug": "nidhogg", "name": "Nidhogg"},
            },
            # Intentionally wait for pool creation.
            wait_for_orchid=True,
        )

        op1 = run_test_vanilla("sleep 1000", job_count=2, spec={"pool": "francis", "pool_trees": ["yggdrasil"]})
        op2 = run_test_vanilla("sleep 1000", job_count=1, spec={"pool": "nidhogg", "pool_trees": ["yggdrasil"]})
        op3 = run_test_vanilla("sleep 1000", job_count=1, spec={"pool": "misirlou", "pool_trees": ["yggdrasil"]})
        op4 = run_test_vanilla("sleep 1000", job_count=1, spec={"pool": "abcless", "pool_trees": ["yggdrasil"]})

        wait(lambda: op1.get_job_count("running") == 2)
        wait(lambda: op2.get_job_count("running") == 1)
        wait(lambda: op3.get_job_count("running") == 1)
        wait(lambda: op4.get_job_count("running") == 1)

        root_key = (42, "yggdrasil", "<Root>")

        desired_metering_data = {
            root_key: {
                "strong_guarantee_resources/cpu": 4,
                "resource_flow/cpu": 0,
                "burst_guarantee_resources/cpu": 0,
                # Aggregated from op4 in abcless pool.
                "allocated_resources/cpu": 1,
            },
            (1, "yggdrasil", "pixies"): {
                "strong_guarantee_resources/cpu": 2,
                "resource_flow/cpu": 0,
                "burst_guarantee_resources/cpu": 0,
                # Aggregated from op3 in misilrou pool.
                "allocated_resources/cpu": 1},
            (2, "yggdrasil", "francis"): {
                "strong_guarantee_resources/cpu": 1,
                "resource_flow/cpu": 0,
                "burst_guarantee_resources/cpu": 0,
                "allocated_resources/cpu": 2,
            },
            (3, "yggdrasil", "nidhogg"): {
                "strong_guarantee_resources/cpu": 0,
                "resource_flow/cpu": 5,
                "burst_guarantee_resources/cpu": 6,
                "allocated_resources/cpu": 1,
            },
        }

        @wait_no_assert
        def check_structured():
            event_key_to_last_record = self._extract_metering_records_from_log()
            return self._validate_metering_records(root_key, desired_metering_data, event_key_to_last_record)

    @authors("ignat")
    def test_metering_tags(self):
        set("//sys/pool_trees/default/@config/metering_tags", {"my_tag": "my_value"})
        wait(lambda: get("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/config/metering_tags"))

        # Check that metering tag cannot be specified without abc attribute.
        with pytest.raises(YtError):
            create_pool(
                "my_pool",
                pool_tree="default",
                attributes={
                    "metering_tags": {"pool_tag": "pool_value"},
                },
                wait_for_orchid=False,
            )

        create_pool(
            "my_pool",
            pool_tree="default",
            attributes={
                "strong_guarantee_resources": {"cpu": 4},
                "abc": {"id": 1, "slug": "my", "name": "MyService"},
                "metering_tags": {"pool_tag": "pool_value"},
            },
            wait_for_orchid=False,
        )

        root_key = (42, "default", "<Root>")

        desired_metering_data = {
            root_key: {
                "strong_guarantee_resources/cpu": 0,
                "resource_flow/cpu": 0,
                "burst_guarantee_resources/cpu": 0,
                "allocated_resources/cpu": 0,
                "my_tag": "my_value",
            },
            (1, "default", "my_pool"): {
                "strong_guarantee_resources/cpu": 4,
                "resource_flow/cpu": 0,
                "burst_guarantee_resources/cpu": 0,
                "allocated_resources/cpu": 0,
                "my_tag": "my_value",
                "pool_tag": "pool_value",
            },
        }

        @wait_no_assert
        def check_structured():
            event_key_to_last_record = self._extract_metering_records_from_log()
            return self._validate_metering_records(root_key, desired_metering_data, event_key_to_last_record)

    @authors("ignat")
    def test_resource_metering_at_root(self):
        create_pool(
            "pool_with_abc",
            pool_tree="default",
            attributes={
                "strong_guarantee_resources": {"cpu": 4},
                "abc": {"id": 1, "slug": "my1", "name": "MyService1"},
            },
            wait_for_orchid=False,
        )

        create_pool(
            "pool_without_abc",
            pool_tree="default",
            attributes={
                "strong_guarantee_resources": {"cpu": 2},
            },
            wait_for_orchid=False,
        )

        create_pool(
            "pool_with_abc_at_children",
            pool_tree="default",
            attributes={
                "strong_guarantee_resources": {"cpu": 2},
                "integral_guarantees": {
                    "guarantee_type": "none",
                    "resource_flow": {"cpu": 5},
                },
            },
            wait_for_orchid=False,
        )

        create_pool(
            "strong_guarantees_pool",
            parent_name="pool_with_abc_at_children",
            pool_tree="default",
            attributes={
                "strong_guarantee_resources": {"cpu": 1},
                "abc": {"id": 2, "slug": "my2", "name": "MyService2"},
            },
            wait_for_orchid=False,
        )

        create_pool(
            "integral_guarantees_pool",
            parent_name="pool_with_abc_at_children",
            pool_tree="default",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 2},
                },
                "abc": {"id": 3, "slug": "my3", "name": "MyService3"},
            },
            wait_for_orchid=False,
        )

        root_key = (42, "default", "<Root>")

        desired_metering_data = {
            root_key: {
                "strong_guarantee_resources/cpu": 3,  # 2 from pool_without_abc, and 1 from pool_with_abc_at_children
                "resource_flow/cpu": 0,  # none integral resources are not summed to <Root>
                "burst_guarantee_resources/cpu": 0,
                "allocated_resources/cpu": 0,
            },
            (1, "default", "pool_with_abc"): {
                "strong_guarantee_resources/cpu": 4,
                "resource_flow/cpu": 0,
                "burst_guarantee_resources/cpu": 0,
                "allocated_resources/cpu": 0,
            },
            (2, "default", "strong_guarantees_pool"): {
                "strong_guarantee_resources/cpu": 1,
                "resource_flow/cpu": 0,
                "burst_guarantee_resources/cpu": 0,
                "allocated_resources/cpu": 0,
            },
            (3, "default", "integral_guarantees_pool"): {
                "strong_guarantee_resources/cpu": 0,
                "resource_flow/cpu": 2,
                "burst_guarantee_resources/cpu": 0,
                "allocated_resources/cpu": 0,
            },
        }

        @wait_no_assert
        def check_structured():
            event_key_to_last_record = self._extract_metering_records_from_log()
            return self._validate_metering_records(root_key, desired_metering_data, event_key_to_last_record)

    @authors("ignat")
    def test_metering_with_revive(self):
        set("//sys/pool_trees/default/@config/metering_tags", {"my_tag": "my_value"})
        wait(lambda: get("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/config/metering_tags"))

        create_pool(
            "my_pool",
            pool_tree="default",
            attributes={
                "strong_guarantee_resources": {"cpu": 4},
                "abc": {"id": 1, "slug": "my", "name": "MyService"},
                "metering_tags": {"pool_tag": "pool_value"},
            },
            wait_for_orchid=False,
        )

        root_key = (42, "default", "<Root>")

        desired_metering_data = {
            root_key: {
                "strong_guarantee_resources/cpu": 0,
                "resource_flow/cpu": 0,
                "burst_guarantee_resources/cpu": 0,
                "allocated_resources/cpu": 0,
                "my_tag": "my_value",
            },
            (1, "default", "my_pool"): {
                "strong_guarantee_resources/cpu": 4,
                "resource_flow/cpu": 0,
                "burst_guarantee_resources/cpu": 0,
                "allocated_resources/cpu": 0,
                "my_tag": "my_value",
                "pool_tag": "pool_value",
            },
        }

        @wait_no_assert
        def check_expected_tags():
            event_key_to_last_record = self._extract_metering_records_from_log()
            return self._validate_metering_records(root_key, desired_metering_data, event_key_to_last_record)

        wait(lambda: exists("//sys/scheduler/@last_metering_log_time"))

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            time.sleep(8)

        def check_expected_usage():
            event_key_to_records = self._extract_metering_records_from_log(last=False)
            has_long_record = False
            for tags, usage in event_key_to_records[root_key]:
                print_debug(tags, usage)
                # Record could be split by hour bound, therefore we check half of the slept period.
                if usage["quantity"] > 3900:
                    has_long_record = True
            return has_long_record

        wait(check_expected_usage)

    @authors("ignat")
    def test_metering_profiling(self):
        create_pool(
            "my_pool",
            pool_tree="default",
            attributes={
                "strong_guarantee_resources": {"cpu": 4},
                "abc": {"id": 1, "slug": "my", "name": "MyService"},
                "metering_tags": {"pool_tag": "pool_value"},
            },
            wait_for_orchid=False,
        )

        metering_count_sensor = profiler_factory().at_scheduler().counter("scheduler/metering/record_count")

        wait(lambda: metering_count_sensor.get_delta() > 0)

    @authors("ignat")
    def test_separate_schema_for_allocation(self):
        # NB(eshcherbin): Increase metering period to ensure accumulated usage value is averaged over the period.
        update_scheduler_config("resource_metering_period", 2000)
        update_scheduler_config("resource_metering/enable_separate_schema_for_allocation", True)
        set("//sys/pool_trees/default/@config/accumulated_resource_usage_update_period", 100)

        create_pool(
            "my_pool",
            pool_tree="default",
            attributes={
                "strong_guarantee_resources": {"cpu": 4},
                "abc": {"id": 1, "slug": "my", "name": "MyService"},
            },
            wait_for_orchid=False,
        )

        op = run_test_vanilla("sleep 2000", job_count=1, spec={"pool": "my_pool"})
        wait(lambda: op.get_job_count("running") == 1)

        root_key = (42, "default", "<Root>")

        desired_guarantees_metering_data = {
            root_key: {
                "strong_guarantee_resources/cpu": 0,
                "resource_flow/cpu": 0,
                "burst_guarantee_resources/cpu": 0,
            },
            (1, "default", "my_pool"): {
                "strong_guarantee_resources/cpu": 4,
                "resource_flow/cpu": 0,
                "burst_guarantee_resources/cpu": 0,
            },
        }

        desired_allocation_metering_data = {
            root_key: {
                "allocated_resources/cpu": 0,
            },
            (1, "default", "my_pool"): {
                "allocated_resources/cpu": 1.0,
            },
        }

        @wait_no_assert
        def check_expected_guarantee_records():
            event_key_to_last_record = self._extract_metering_records_from_log(schema="yt.scheduler.pools.compute_guarantee.v1")
            return self._validate_metering_records(root_key, desired_guarantees_metering_data, event_key_to_last_record)

        @wait_no_assert
        def check_expected_allocation_records():
            event_key_to_last_record = self._extract_metering_records_from_log(schema="yt.scheduler.pools.compute_allocation.v1")
            # Update period equal 100ms, metering period is 1000ms, so we expected the error to be less than or equal to 10%.
            self._validate_metering_records(root_key, desired_allocation_metering_data, event_key_to_last_record, precision=0.15)
