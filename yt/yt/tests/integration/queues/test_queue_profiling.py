import time

import pytest

from yt_queue_agent_test_base import TestQueueAgentBase, QueueAgentOrchid

from yt_commands import (
    print_debug,
    authors,
    wait,
    create,
    insert_rows,
    select_rows,
    register_queue_consumer,
    advance_consumer,
)

from yt_helpers import profiler_factory


##################################################################

TIME_METRIC_NON_FLAP_MULTIPLIER = 1.5
NONE_TAG = "none"


def get_mapping():
    return list(select_rows("* from [//sys/queue_agents/queue_agent_object_mapping]"))


def get_profiler():
    wait(lambda: len(get_mapping()) > 0)
    mapping = get_mapping()

    print_debug(f"{mapping=}")

    original_host = mapping[0]["host"]
    return profiler_factory().at_queue_agent(original_host)


class TestQueueAgentConsumerProfiling(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    @authors("panesher")
    def test_profiling(self):
        orchid = QueueAgentOrchid()

        queue = self.create_queue_path()
        self._create_queue(queue, mount=True)

        consumer_path = self.create_consumer_path()
        create("queue_consumer", consumer_path)

        register_queue_consumer(queue, consumer_path, vital=True)

        self._wait_for_component_passes()

        consumer_orchid = orchid.get_consumer_orchid(f"primary:{consumer_path}")
        queue_orchid = orchid.get_queue_orchid(f"primary:{queue}")

        profiler = get_profiler()

        def get_gauge_summary_metric(name):
            return int(profiler.summary(f"queue_agent/consumer_partition/{name}").get_all()[0]["value"])

        def get_counter_metric(name):
            return profiler.counter(f"queue_agent/consumer_partition/{name}").get_delta()

        def get_gauge_time_summary_metric(name):
            return profiler.summary(f"queue_agent/consumer_partition/{name}").get_all()[0]["value"]

        def get_closest_bins_gauge_histogram_metric(name):
            bins = profiler.histogram(f"queue_agent/consumer_partition/{name}").get_all()[0]["value"]

            for i, bin in enumerate(bins):
                if bin["count"] > 0:
                    assert bin["count"] == 1
                    if i > 0:
                        return bins[i - 1]["bound"] / 1000, bin["bound"] / 1000
                    return 0, bin["bound"] / 1000

            raise Exception("No bins with count > 0")

        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        wait(lambda: get_gauge_summary_metric("lag_data_weight") == 0, ignore_exceptions=True)
        wait(lambda: get_gauge_summary_metric("lag_rows") == 0, ignore_exceptions=True)
        wait(lambda: get_gauge_summary_metric("offset") == 0, ignore_exceptions=True)
        wait(lambda: get_counter_metric("rows_consumed") == 0, ignore_exceptions=True)
        wait(lambda: get_counter_metric("data_weight_consumed") == 0, ignore_exceptions=True)

        time_before_insert = time.time()

        insert_rows(queue, [{"data": "foo", "$tablet_index": 0}] * 3)
        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        time_after_insert = time.time()

        def check_histogram_metric(name):
            expected_lower_bound = time.time() - time_after_insert
            lower_bin, upper_bin = get_closest_bins_gauge_histogram_metric(name)
            expected_upper_bound = time.time() - time_before_insert + 1
            print_debug(
                f"""Checking histogram. {lower_bin=}, {upper_bin=},
                expected: [{expected_lower_bound}, {expected_upper_bound}]"""
            )
            return 0 < lower_bin <= expected_upper_bound * TIME_METRIC_NON_FLAP_MULTIPLIER

        def check_lag_time(name):
            expected_lower_bound = time.time() - time_after_insert
            lag_time = get_gauge_time_summary_metric(name)
            expected_upper_bound = time.time() - time_before_insert
            print_debug(
                f"""Checking lag time. {lag_time=},
                expected: [{expected_lower_bound}, {expected_upper_bound}]"""
            )
            return (
                0 < get_gauge_time_summary_metric("lag_time") <= expected_upper_bound * TIME_METRIC_NON_FLAP_MULTIPLIER
            )

        wait(lambda: get_gauge_summary_metric("lag_data_weight") == 0)  # We haven't started consuming yet
        wait(lambda: get_gauge_summary_metric("lag_rows") == 3)
        wait(lambda: get_gauge_summary_metric("offset") == 0)
        wait(lambda: get_counter_metric("rows_consumed") == 0)
        wait(lambda: get_counter_metric("data_weight_consumed") == 0)

        wait(lambda: check_lag_time("lag_time"), ignore_exceptions=True)
        wait(lambda: check_histogram_metric("lag_time_histogram"), ignore_exceptions=True)

        advance_consumer(consumer_path, queue, partition_index=0, old_offset=None, new_offset=1)
        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        wait(lambda: get_gauge_summary_metric("lag_data_weight") == 40)
        wait(lambda: get_gauge_summary_metric("lag_rows") == 2)
        wait(lambda: get_gauge_summary_metric("offset") == 1)

        wait(lambda: check_lag_time("lag_time"))
        wait(lambda: check_histogram_metric("lag_time_histogram"))

        # We could see less than expected here if we had more than 1 pass
        wait(lambda: get_counter_metric("rows_consumed") <= 1)
        wait(lambda: get_counter_metric("data_weight_consumed") <= 20)

        advance_consumer(consumer_path, queue, partition_index=0, old_offset=1, new_offset=3)
        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        wait(lambda: get_gauge_summary_metric("lag_data_weight") == 0)
        wait(lambda: get_gauge_summary_metric("lag_rows") == 0)
        wait(lambda: get_gauge_summary_metric("offset") == 3)

        # We could see less than expected here if we had more than 1 pass
        wait(lambda: get_counter_metric("rows_consumed") <= 2)
        wait(lambda: get_counter_metric("data_weight_consumed") <= 40)
        wait(lambda: get_gauge_time_summary_metric("lag_time") == 0.0)
        wait(lambda: get_closest_bins_gauge_histogram_metric("lag_time_histogram")[0] == 0.0)

    @authors("panesher")
    @pytest.mark.parametrize(
        "queue_tag", ["queue_tag_1", ""],
    )
    @pytest.mark.parametrize(
        "consumer_tag", ["consumer_tag_1", ""],
    )
    def test_profiling_tags(self, queue_tag, consumer_tag):
        orchid = QueueAgentOrchid()

        queue = self.create_queue_path()
        if queue_tag:
            self._create_queue(queue, mount=True, queue_profiling_tag=queue_tag)
        else:
            self._create_queue(queue, mount=True)
        insert_rows(queue, [{"data": "foo", "$tablet_index": 0}] * 3)

        consumer_path = self.create_consumer_path()
        create(
            "queue_consumer",
            consumer_path,
            attributes=(
                {
                    "queue_consumer_profiling_tag": consumer_tag,
                }
                if consumer_tag
                else {}
            ),
        )

        register_queue_consumer(queue, consumer_path, vital=True)

        self._wait_for_component_passes()

        consumer_orchid = orchid.get_consumer_orchid(f"primary:{consumer_path}")
        queue_orchid = orchid.get_queue_orchid(f"primary:{queue}")

        profiler = get_profiler()

        self._wait_for_component_passes()
        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        wait(lambda: int(profiler.gauge('queue_agent/consumer_partition/lag_rows', fixed_tags={
            "queue_tag": queue_tag or NONE_TAG,
            "consumer_tag": consumer_tag or NONE_TAG,
        }).get_all()[0]["value"]) == 3, ignore_exceptions=True)

        lag_rows = profiler.gauge('queue_agent/consumer_partition/lag_rows', fixed_tags={
            "queue_tag": queue_tag or NONE_TAG,
            "consumer_tag": consumer_tag or NONE_TAG,
        }).get_all()[0]
        assert lag_rows["tags"]["queue_tag"] == queue_tag or NONE_TAG
        assert lag_rows["tags"]["consumer_tag"] == consumer_tag or NONE_TAG


class TestQueueAgentQueueProfiling(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    @authors("panesher")
    @pytest.mark.parametrize(
        "queue_tag", ["queue_tag_1", ""],
    )
    def test_profiling_tag(self, queue_tag):
        queue = self.create_queue_path()
        if queue_tag:
            self._create_queue(queue, mount=True, queue_profiling_tag=queue_tag)
        else:
            self._create_queue(queue, mount=True)

        self._wait_for_component_passes()
        profiler = get_profiler()

        wait(lambda: int(profiler.gauge('queue_agent/queue/partitions', fixed_tags={
            "queue_tag": queue_tag or NONE_TAG,
        }).get_all()[0]["value"]) == 1, ignore_exceptions=True)
        partitions = profiler.gauge('queue_agent/queue/partitions', fixed_tags={
            "queue_tag": queue_tag or NONE_TAG,
        }).get_all()[0]
        assert partitions["tags"]["queue_tag"] == queue_tag or NONE_TAG
