import time

import pytest

from yt_queue_agent_test_base import TestQueueAgentBase, QueueAgentOrchid, GenericObjectPath

from yt_commands import (
    print_debug,
    authors,
    wait,
    create,
    insert_rows,
    register_queue_consumer,
    advance_consumer,
    ls,
    set as yt_set,
    get,
    abort_transaction,
)

from yt_helpers import profiler_factory
from yt.test_helpers.profiler import Profiler


##################################################################


TIME_METRIC_NON_FLAP_MULTIPLIER = 1.5
NONE_TAG = "none"


def get_profiler() -> Profiler:
    hosts = ls('//sys/queue_agents/instances')
    assert len(hosts) == 1
    return profiler_factory().at_queue_agent(hosts[0])


def get_profilers() -> dict[str, Profiler]:
    hosts = set(ls('//sys/queue_agents/instances'))
    return {host: profiler_factory().at_queue_agent(host) for host in hosts}


##################################################################


class ConsumerMetricsHelper:
    def __init__(self, profiler: Profiler, tags: dict[str, str] | None = None):
        self.profiler = profiler
        self.tags = tags

    def _get_gauge_summary_metric(self, name):
        return int(self.profiler.summary(f"queue_agent/consumer_partition/{name}", fixed_tags=self.tags).get_all()[0]["value"])

    def _get_counter_metric(self, name):
        return self.profiler.counter(f"queue_agent/consumer_partition/{name}", tags=self.tags).get_delta()

    def _get_gauge_time_summary_metric(self, name):
        return self.profiler.summary(f"queue_agent/consumer_partition/{name}", fixed_tags=self.tags).get_all()[0]["value"]

    def _get_closest_bins_gauge_histogram_metric(self, name):
        bins = self.profiler.histogram(f"queue_agent/consumer_partition/{name}", fixed_tags=self.tags).get_all()[0]["value"]

        for i, bin in enumerate(bins):
            if bin["count"] > 0:
                assert bin["count"] == 1
                if i > 0:
                    return bins[i - 1]["bound"] / 1000, bin["bound"] / 1000
                return 0, bin["bound"] / 1000

        raise Exception("No bins with count > 0")

    def get_lag_data_weight(self):
        return self._get_gauge_summary_metric("lag_data_weight")

    def get_lag_rows(self):
        return self._get_gauge_summary_metric("lag_rows")

    def get_offset(self):
        return self._get_gauge_summary_metric("offset")

    def get_rows_consumed(self):
        return self._get_counter_metric("rows_consumed")

    def get_data_weight_consumed(self):
        return self._get_counter_metric("data_weight_consumed")

    def get_lag_time(self):
        return self._get_gauge_time_summary_metric("lag_time")

    def get_lag_time_histogram(self):
        return self._get_closest_bins_gauge_histogram_metric("lag_time_histogram")

    def check_lag_time(self, time_before_insert, time_after_insert):
        expected_lower_bound = time.time() - time_after_insert
        lag_time = self.get_lag_time()
        expected_upper_bound = time.time() - time_before_insert
        print_debug(
            f"""Checking lag time. {lag_time=},
            expected: [{expected_lower_bound}, {expected_upper_bound}]"""
        )
        return 0 < lag_time <= expected_upper_bound * TIME_METRIC_NON_FLAP_MULTIPLIER

    def check_histogram_metric(self, time_before_insert, time_after_insert):
        expected_lower_bound = time.time() - time_after_insert
        lower_bin, upper_bin = self.get_lag_time_histogram()
        expected_upper_bound = time.time() - time_before_insert + 1
        print_debug(
            f"""Checking histogram. {lower_bin=}, {upper_bin=},
            expected: [{expected_lower_bound}, {expected_upper_bound}]"""
        )
        return 0 < lower_bin <= expected_upper_bound * TIME_METRIC_NON_FLAP_MULTIPLIER


##################################################################


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

        consumer_metrics = ConsumerMetricsHelper(get_profiler(), tags={
            "consumer_path": consumer_path,
            "consumer_cluster": "primary",
        })

        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        wait(lambda: consumer_metrics.get_lag_data_weight() == 0, ignore_exceptions=True)
        wait(lambda: consumer_metrics.get_lag_rows() == 0, ignore_exceptions=True)
        wait(lambda: consumer_metrics.get_offset() == 0, ignore_exceptions=True)
        wait(lambda: consumer_metrics.get_rows_consumed() == 0, ignore_exceptions=True)
        wait(lambda: consumer_metrics.get_data_weight_consumed() == 0, ignore_exceptions=True)

        time_before_insert = time.time()

        insert_rows(queue, [{"data": "foo", "$tablet_index": 0}] * 3)
        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        time_after_insert = time.time()

        wait(lambda: consumer_metrics.get_lag_data_weight() == 0)  # We haven't started consuming yet
        wait(lambda: consumer_metrics.get_lag_rows() == 3)
        wait(lambda: consumer_metrics.get_offset() == 0)
        wait(lambda: consumer_metrics.get_rows_consumed() == 0)
        wait(lambda: consumer_metrics.get_data_weight_consumed() == 0)

        wait(lambda: consumer_metrics.check_lag_time(time_before_insert, time_after_insert), ignore_exceptions=True)
        wait(lambda: consumer_metrics.check_histogram_metric(time_before_insert, time_after_insert), ignore_exceptions=True)

        advance_consumer(consumer_path, queue, partition_index=0, old_offset=None, new_offset=1)
        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        wait(lambda: consumer_metrics.get_lag_data_weight() == 40)
        wait(lambda: consumer_metrics.get_lag_rows() == 2)
        wait(lambda: consumer_metrics.get_offset() == 1)

        wait(lambda: consumer_metrics.check_lag_time(time_before_insert, time_after_insert))
        wait(lambda: consumer_metrics.check_histogram_metric(time_before_insert, time_after_insert))

        # We could see less than expected here if we had more than 1 pass
        wait(lambda: consumer_metrics.get_rows_consumed() <= 1)
        wait(lambda: consumer_metrics.get_data_weight_consumed() <= 20)

        advance_consumer(consumer_path, queue, partition_index=0, old_offset=1, new_offset=3)
        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        wait(lambda: consumer_metrics.get_lag_data_weight() == 0)
        wait(lambda: consumer_metrics.get_lag_rows() == 0)
        wait(lambda: consumer_metrics.get_offset() == 3)

        # We could see less than expected here if we had more than 1 pass
        wait(lambda: consumer_metrics.get_rows_consumed() <= 2)
        wait(lambda: consumer_metrics.get_data_weight_consumed() <= 40)
        wait(lambda: consumer_metrics.get_lag_time() == 0.0)
        wait(lambda: consumer_metrics.get_lag_time_histogram()[0] == 0.0)

    @authors("panesher")
    @pytest.mark.parametrize(
        "queue_tag",
        ["queue_tag_1", ""],
    )
    @pytest.mark.parametrize(
        "consumer_tag",
        ["consumer_tag_1", ""],
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

        def get_metric() -> list:
            x = profiler.gauge(
                'queue_agent/consumer_partition/lag_rows',
                fixed_tags={
                    "queue_tag": queue_tag or NONE_TAG,
                    "consumer_tag": consumer_tag or NONE_TAG,
                },
            ).get_all()
            print_debug(x)
            return x

        wait(get_metric, ignore_exceptions=True)

        lag_rows = get_metric()[0]
        assert lag_rows["tags"]["queue_tag"] == queue_tag or NONE_TAG
        assert lag_rows["tags"]["consumer_tag"] == consumer_tag or NONE_TAG

        queue_tag = "new_queue_tag"
        yt_set(f"{queue}/@queue_profiling_tag", queue_tag)

        self._wait_for_component_passes()
        wait(get_metric, ignore_exceptions=True)

        lag_rows = get_metric()[0]
        assert lag_rows["tags"]["queue_tag"] == queue_tag
        assert lag_rows["tags"]["consumer_tag"] == consumer_tag or NONE_TAG

        consumer_tag = "new_consumer_tag"
        yt_set(f"{consumer_path}/@queue_consumer_profiling_tag", consumer_tag)

        self._wait_for_component_passes()
        wait(get_metric, ignore_exceptions=True)

        lag_rows = get_metric()[0]
        assert lag_rows["tags"]["queue_tag"] == queue_tag
        assert lag_rows["tags"]["consumer_tag"] == consumer_tag

    @authors("panesher")
    def test_multi_consumer_profiling(self):
        orchid = QueueAgentOrchid()

        multi_consumer_path = self.create_consumer_path()
        self._create_consumer(multi_consumer_path, multi_consumer=True, queue_agent_stage="production")

        self._wait_for_component_passes()

        multi_consumer_orchid = orchid.get_multi_consumer_orchid(f"primary:{multi_consumer_path}")
        multi_consumer_orchid.wait_fresh_pass()

        profiler = get_profiler()

        def get_consumers_gauge():
            return profiler.gauge(
                "queue_agent/multi_consumer/consumers",
                fixed_tags={
                    "multi_consumer_cluster": "primary",
                    "multi_consumer_path": multi_consumer_path,
                },
            ).get_all()

        wait(lambda: int(get_consumers_gauge()[0]["value"]) == 0, ignore_exceptions=True)

        queue = self.create_queue_path()
        self._create_queue(queue, mount=True)

        time_before_insert = time.time()
        insert_rows(queue, [{"data": "foo", "$tablet_index": 0}] * 3)
        time_after_insert = time.time()

        names = ["consumer_1", "consumer_2", "consumer_3"]
        refs: list[GenericObjectPath] = []
        for name in names:
            consumer_ref = self._create_registered_consumer(multi_consumer_path, queue, vital=True, consumer_name=name)
            self._advance_consumer(consumer_ref, queue, partition_index=0, offset=0)
            refs.append(consumer_ref)

        multi_consumer_orchid.wait_fresh_pass()

        wait(lambda: int(get_consumers_gauge()[0]["value"]) == len(names), ignore_exceptions=True)

        consumer_orchids = [orchid.get_consumer_orchid(ref) for ref in refs]
        for consumer_orchid in consumer_orchids:
            consumer_orchid.wait_fresh_pass()

        consumer_metrics = [ConsumerMetricsHelper(get_profiler(), tags={
            "consumer_path": ref.get_path(),
            "consumer_cluster": ref.get_cluster(),
            "consumer_name": ref.get_queue_consumer_name(),
        }) for ref in refs]

        def check_metrics_for_not_started_consumer(consumer_metric: ConsumerMetricsHelper, ignore_exceptions: bool = False):
            wait(lambda: consumer_metric.get_lag_data_weight() == 0, ignore_exceptions=ignore_exceptions)
            wait(lambda: consumer_metric.get_lag_rows() == 3, ignore_exceptions=ignore_exceptions)
            wait(lambda: consumer_metric.get_offset() == 0, ignore_exceptions=ignore_exceptions)
            wait(lambda: consumer_metric.get_rows_consumed() == 0, ignore_exceptions=ignore_exceptions)
            wait(lambda: consumer_metric.get_data_weight_consumed() == 0, ignore_exceptions=ignore_exceptions)
            wait(lambda: consumer_metric.check_lag_time(time_before_insert, time_after_insert), ignore_exceptions=ignore_exceptions)
            wait(lambda: consumer_metric.check_histogram_metric(time_before_insert, time_after_insert), ignore_exceptions=ignore_exceptions)

        for consumer_metric in consumer_metrics:
            check_metrics_for_not_started_consumer(consumer_metric, ignore_exceptions=True)

        advance_consumer(refs[0], queue, partition_index=0, old_offset=0, new_offset=1)
        consumer_orchids[0].wait_fresh_pass()
        wait(lambda: consumer_metrics[0].get_lag_data_weight() == 40)
        wait(lambda: consumer_metrics[0].get_lag_rows() == 2)
        wait(lambda: consumer_metrics[0].get_offset() == 1)
        wait(lambda: consumer_metrics[0].check_lag_time(time_before_insert, time_after_insert))
        wait(lambda: consumer_metrics[0].check_histogram_metric(time_before_insert, time_after_insert))

        for consumer_metric in consumer_metrics[1:]:
            check_metrics_for_not_started_consumer(consumer_metric)


class TestQueueAgentQueueProfiling(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    @authors("panesher")
    @pytest.mark.parametrize(
        "queue_tag",
        ["queue_tag_1", ""],
    )
    def test_profiling_tag(self, queue_tag):
        queue = self.create_queue_path()
        if queue_tag:
            self._create_queue(queue, mount=True, queue_profiling_tag=queue_tag)
        else:
            self._create_queue(queue, mount=True)

        self._wait_for_component_passes()
        profiler = get_profiler()

        def get_metric() -> list:
            return profiler.gauge(
                'queue_agent/queue/partitions',
                fixed_tags={
                    "queue_tag": queue_tag or NONE_TAG,
                },
            ).get_all()

        wait(get_metric, ignore_exceptions=True)
        partitions = get_metric()[0]
        assert partitions["tags"]["queue_tag"] == queue_tag or NONE_TAG

        queue_tag = "new_queue_tag"
        yt_set(f"{queue}/@queue_profiling_tag", queue_tag)

        self._wait_for_component_passes()
        wait(get_metric, ignore_exceptions=True)

        partitions = get_metric()[0]
        assert partitions["tags"]["queue_tag"] == queue_tag


class TestQueueAgentPassProfiling(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    def _check_pass(
        self, profiler: Profiler, prefix: str, start_time: float, tags: dict[str, str] | None = None
    ) -> None:
        wait(lambda: profiler.counter(f"{prefix}/pass/errors", tags=tags).get_delta() == 0, ignore_exceptions=True)
        wait(
            lambda: 0 < profiler.gauge(f"{prefix}/pass/index", fixed_tags=tags).get_all()[0]["value"],
            ignore_exceptions=True,
        )
        wait(
            lambda: start_time
            < profiler.gauge(f"{prefix}/pass/start_time", fixed_tags=tags).get_all()[0]["value"]
            < time.time() + 5,
            ignore_exceptions=True,
        )
        wait(
            lambda: 0 < profiler.gauge(f"{prefix}/pass/duration", fixed_tags=tags).get_all()[0]["value"] < 10,
            ignore_exceptions=True,
        )

    @authors("panesher")
    def test_basic_pass_profiling(self):
        pass_prefixes = [
            "queue_agent/cypress_synchronizer",
            "queue_agent/queue_agent_sharding_manager",
            "queue_agent",
        ]

        start_time = time.time() - 5  # fault tolerance

        self._wait_for_component_passes()

        profiler = get_profiler()

        for pass_prefix in pass_prefixes:
            self._check_pass(profiler, pass_prefix, start_time)

    @authors("panesher")
    def test_queue_pass_profiling(self):
        queue_pass_prefixes = [
            "queue_agent/queue/controller",
            "queue_agent/queue/static_export",
        ]

        start_time = time.time() - 5  # fault tolerance
        queue = self.create_queue_path()
        self._create_queue(
            queue,
            mount=True,
            static_export_config={
                "default": {
                    "export_directory": "//tmp/export",
                    "export_period": 1000,
                },
            },
        )

        self._wait_for_component_passes()

        orchid = QueueAgentOrchid()
        queue_orchid = orchid.get_queue_orchid(f"primary:{queue}")
        queue_orchid.wait_fresh_pass()

        profiler = get_profiler()

        for pass_prefix in queue_pass_prefixes:
            tags = {
                "queue_path": queue,
                "queue_cluster": "primary",
            }
            self._check_pass(profiler, pass_prefix, start_time, tags)
            duration_summary = profiler.summary(f'{pass_prefix}/pass/duration', fixed_tags={
                "queue_cluster": "primary",
                "leading": "true",
            })
            assert 0 < duration_summary.get_max() < 10

    @authors("panesher")
    def test_consumer_pass_profiling(self):
        consumer_pass_prefixes = [
            "queue_agent/consumer/controller",
        ]

        start_time = time.time() - 5  # fault tolerance
        queue = self.create_queue_path()
        self._create_queue(queue, mount=True)

        consumer_path = self.create_consumer_path()
        create("queue_consumer", consumer_path)

        register_queue_consumer(queue, consumer_path, vital=True)

        self._wait_for_component_passes()

        orchid = QueueAgentOrchid()
        consumer_orchid = orchid.get_consumer_orchid(f"primary:{consumer_path}")
        queue_orchid = orchid.get_queue_orchid(f"primary:{queue}")
        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        profiler = get_profiler()

        for pass_prefix in consumer_pass_prefixes:
            tags = {
                "consumer_path": consumer_path,
                "consumer_cluster": "primary",
            }
            self._check_pass(profiler, pass_prefix, start_time, tags)
            duration_summary = profiler.summary(f'{pass_prefix}/pass/duration', fixed_tags={
                "consumer_cluster": "primary",
                "leading": "true",
            })
            assert 0 < duration_summary.get_max() < 10


class TestBasicPassesMultiHosts(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
        "election_manager": {
            "lock_acquisition_period": 100,
        },
    }

    DELTA_QUEUE_AGENT_CONFIG = {
        "election_manager": {
            "transaction_ping_period": 100,
            "lock_acquisition_period": 100,
        },
    }

    NUM_QUEUE_AGENTS_PRIMARY = 3

    @authors("panesher")
    def test_basic_pass_profiling_counts(self):
        pass_prefixes_to_count = {
            "queue_agent/cypress_synchronizer": 1,
            "queue_agent/queue_agent_sharding_manager": 1,
            "queue_agent": self.NUM_QUEUE_AGENTS_PRIMARY,
        }

        self._wait_for_component_passes()

        profilers = get_profilers()
        assert len(profilers) == self.NUM_QUEUE_AGENTS_PRIMARY

        def get_metric_count(profiler: Profiler, pass_prefix: str) -> int:
            count = len(profiler.gauge(f"{pass_prefix}/pass/duration").get_all())
            assert count <= 1, f"{pass_prefix} has more than one pass"
            return count

        def get_profilers_count(pass_prefix: str):
            # NB(panesher): using duration here since it resets to zero if the agent loses leadership.
            return sum(get_metric_count(profiler, pass_prefix) for profiler in profilers.values())

        for pass_prefix, expected_count in pass_prefixes_to_count.items():
            wait(lambda: get_profilers_count(pass_prefix) == expected_count, ignore_exceptions=True)

        def get_cypress_synchronizer_host() -> str:
            hosts = list(
                host
                for host, profiler in profilers.items()
                if get_metric_count(profiler, "queue_agent/cypress_synchronizer")
            )
            return hosts[0] if len(hosts) == 1 else ""

        old_host = get_cypress_synchronizer_host()

        def get_current_lock():
            locks = list(filter(lambda x: x["state"] == "acquired", get("//sys/queue_agents/leader_lock/@locks")))
            assert len(locks) == 1
            return locks[0]

        abort_transaction(get_current_lock()["transaction_id"])

        def check_new():
            new_host = get_cypress_synchronizer_host()
            trx = get_current_lock()["transaction_id"]
            trx_host = get(f"//sys/transactions/{trx}/@host")
            return new_host and new_host != old_host and trx_host == new_host

        wait(check_new)


class TestLeadingPassesMultiHosts(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
        "queue_agent_sharding_manager": {
            "pass_period": 10 ** 15,
        },
    }

    NUM_QUEUE_AGENTS_PRIMARY = 2

    @authors("panesher")
    def test_leading_pass_profiling(self):
        queue = self.create_queue_path()
        self._create_queue(queue, mount=True)

        consumer_path = self.create_consumer_path()
        create("queue_consumer", consumer_path)

        register_queue_consumer(queue, consumer_path, vital=True)

        profilers = get_profilers()
        hosts = list(profilers.keys())

        # put queue and consumer in different hosts
        insert_rows("//sys/queue_agents/queue_agent_object_mapping", [
            {
                "object": f"primary:{queue}",
                "host": hosts[0],
            },
            {
                "object": f"primary:{consumer_path}",
                "host": hosts[1],
            },
        ])

        print_debug(f"{profilers=}")

        metric = "queue_agent/{}/controller/pass/duration"

        wait(lambda: profilers[hosts[0]].gauge(metric.format("queue")).get_all()[0]["tags"]["leading"] == "true", ignore_exceptions=True)
        wait(lambda: profilers[hosts[0]].gauge(metric.format("consumer")).get_all()[0]["tags"]["leading"] == "false", ignore_exceptions=True)
        wait(lambda: profilers[hosts[1]].gauge(metric.format("queue")).get_all()[0]["tags"]["leading"] == "false", ignore_exceptions=True)
        wait(lambda: profilers[hosts[1]].gauge(metric.format("consumer")).get_all()[0]["tags"]["leading"] == "true", ignore_exceptions=True)
