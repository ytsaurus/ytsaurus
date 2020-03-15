from yt_commands import get, YtError, print_debug

from yt.packages import requests
import time
import pytest
import tarfile
import os
from io import BytesIO
from collections import defaultdict


# This class resembles a metric exported via Orchid and provides effective means
# for tracking YT profiling information.
#
# How to create: use Metric.at_xxx() static method to create a metric for component xxx.
#
# Aggregation: you can choose how to aggregate the metric data, e.g. compute the maximum value or
#              delta between the current and the initial values.
#
# Tags: there is a limited possibility to filter and distribute samples into bucket by tags.
#       with_tags: use this argument to filter out the samples which don't have certain tags
#                  with certain values (with_tags={"tag_a": "value_a", "tag_b": "value_b"}).
#       by_tags: use this argument to distribute the samples into buckets
#                by certain tags' values (by_tags=["tag_a", "tag_b"]).
#                Use metric.get("value_a", "value_b") to get the needed bucket.
#
# Debug output: metric.get(verbose=True) prints some debug info (path, aggr[, tags] and value).
#
# Usage:
# (1) Create a Metric object for every metric you want to track.
# (2) Do some computations.
# (3) Use metric.get() to get the aggregated value for the metric's samples, which were reported
#     since the metric's creation in (1).
# (4) Use metric.update() to keep the metric up to date.
#
# Recommendation: in most cases you should check the metric's value by using
#                 wait(lambda: metric.update().get(verbose=True) == expected)
#
# Example:
# > metric = Metric.at_scheduler("scheduler/pools/metrics/custom")
# > op1 = map(...)
# > metric.update()
# > op2 = map(...)
# > print_debug(metric.get())  # Doesn't include op2's changes.
# > print_debug(metric.update().get()) # Updated, now includes op2's changes.
class Metric(object):
    AGGREGATION_METHOD_TO_DEFAULT_FACTORY = {
        "none": list,          # No aggregation, just collects all samples as a list of (time, value) tuples sorted by time.
        "delta": int,          # The difference between the last value and the init value (the last value prior to initialization).
        "sum": int,            # The sum of all values.
        "max": lambda: None,   # The maximum value.
        "last": lambda: None,  # The last value.
    }

    # Default DequeuePeriod in profile manager is 100 ms, here we take 300 ms just in case.
    FROM_TIME_GAP = 300000  # mcs

    def __init__(self, path, with_tags=None, grouped_by_tags=None, aggr_method="delta"):
        assert aggr_method in Metric.AGGREGATION_METHOD_TO_DEFAULT_FACTORY

        self.path = path
        self.aggr_method = aggr_method
        self.with_tags = with_tags
        self.grouped_by_tags = grouped_by_tags
        self.last_update_samples = []
        # TODO(eshcherbin): This is used for flap diagnostics. Remove when TestSchedulerProfilingOnOperationFinished is fixed.
        self.custom_diagnostics_enabled = path.split("/")[-1] in ["metric_completed", "metric_failed"]

        # Need time in mcs.
        self.start_time = int(time.time() * 1e6)
        start_samples = self._read_from_orchid()

        default_factory = Metric.AGGREGATION_METHOD_TO_DEFAULT_FACTORY[self.aggr_method]
        if self.grouped_by_tags is None:
            self.data = default_factory()
            if self.aggr_method == "delta":
                self.state = {"start_value": start_samples[-1]["value"] if start_samples else 0,
                              "last_sample_time": start_samples[-1]["time"] if start_samples else 0}
            elif self.aggr_method == "last":
                self.state = {"last_sample_time": 0}
            else:
                self.state = None

            if start_samples:
                self.start_time = max(self.start_time, start_samples[-1]["time"])
        else:
            self.data = defaultdict(default_factory)
            if self.aggr_method == "delta":
                self.state = defaultdict(lambda: {"start_value": 0,
                                                  "last_sample_time": 0})
            elif self.aggr_method == "last":
                self.state = defaultdict(lambda: {"last_sample_time": 0})
            else:
                self.state = defaultdict(lambda: None)

            for tag_values, samples in start_samples.iteritems():
                if self.aggr_method == "delta" and samples:
                    self.state[tag_values]["start_value"] = samples[-1]["value"]
                    self.state[tag_values]["last_sample_time"] = samples[-1]["time"]

                if samples:
                    self.start_time = max(self.start_time, samples[-1]["time"])

        self.last_update_time = self.start_time

    # NB(eshcherbin): **kwargs is used only for the `verbose` argument.
    def get(self, *tags, **kwargs):
        verbose = kwargs["verbose"] if "verbose" in kwargs else False

        if not tags:
            assert self.grouped_by_tags is None
            if verbose:
                print_debug("Metric \"{}\" (aggr={}): {}".format(
                    self.path,
                    self.aggr_method,
                    self.data))
            return self.data
        else:
            assert self.grouped_by_tags is not None
            if verbose:
                print_debug("Metric \"{}\" (aggr={}, tags={}): {}".format(
                    self.path,
                    self.aggr_method,
                    tags,
                    self.data[tags]))
            return self.data[tags]

    def __getitem__(self, item):
        if isinstance(item, tuple):
            return self.get(*item)
        else:
            return self.get(item)

    def update(self):
        # Need time in mcs.
        update_time = int(time.time() * 1e6)
        new_samples = self._read_from_orchid(from_time=max(self.last_update_time - Metric.FROM_TIME_GAP,
                                                           self.start_time))

        if isinstance(new_samples, list):
            self.data, self.state = Metric._update_data(
                self.data,
                self.state,
                new_samples,
                self.aggr_method)

            if new_samples:
                update_time = max(update_time, new_samples[-1]["time"])
        else:
            for tags, samples in new_samples.iteritems():
                self.data[tags], self.state[tags] = Metric._update_data(
                    self.data[tags],
                    self.state[tags],
                    samples,
                    self.aggr_method)

                if samples:
                    update_time = max(update_time, samples[-1]["time"])

        self.last_update_time = update_time
        return self

    @staticmethod
    def at_scheduler(path, *args, **kwargs):
        return Metric("//sys/scheduler/orchid/profiling/" + path, *args, **kwargs)

    @staticmethod
    def at_node(node, path, *args, **kwargs):
        return Metric("//sys/cluster_nodes/{0}/orchid/profiling/{1}".format(node, path), *args, **kwargs)

    @staticmethod
    def at_tablet_node(node, path, *args, **kwargs):
        tablets = get(node + "/@tablets")
        address = get("#%s/@peers/0/address" % tablets[0]["cell_id"])
        return Metric("//sys/cluster_nodes/{0}/orchid/profiling/tablet_node/{1}".format(address, path), *args, **kwargs)

    @staticmethod
    def at_proxy(proxy, path, *args, **kwargs):
        return Metric("//sys/proxies/{0}/orchid/profiling/{1}".format(proxy, path), *args, **kwargs)

    @staticmethod
    def _update_data(data, state, new_samples, aggr):
        new_state = state

        if aggr == "none":
            new_data = sorted(data + [(sample["time"], sample["value"]) for sample in new_samples]) \
                if new_samples \
                else data
        elif aggr == "delta":
            new_data = (new_samples[-1]["value"] - state["start_value"]) \
                if new_samples and new_samples[-1]["time"] >= state["last_sample_time"] \
                else data
            if new_samples:
                new_state["last_sample_time"] = max(new_state["last_sample_time"], new_samples[-1]["time"])
        elif aggr == "sum":
            new_data = data + sum(sample["value"] for sample in new_samples)
        elif aggr == "max":
            max_sample = max(sample["value"] for sample in new_samples) if new_samples else None

            if max_sample is None:
                new_data = data
            elif data is None:
                new_data = max_sample
            else:
                new_data = max(data, max_sample)
        elif aggr == "last":
            new_data = new_samples[-1]["value"] \
                if new_samples and new_samples[-1]["time"] >= state["last_sample_time"] \
                else data
            if new_samples:
                new_state["last_sample_time"] = max(new_state["last_sample_time"], new_samples[-1]["time"])
        else:
            raise Exception("Trying to update metric data with unknown aggregator (Aggr: \"{}\")".format(aggr))

        return new_data, new_state

    # NB(eshcherbin): **kwargs is used only for `from_time` argument.
    def _read_from_orchid(self, **kwargs):
        # TODO(eshcherbin): This is used for flap diagnostics. Remove when TestSchedulerProfilingOnOperationFinished is fixed.
        if self.custom_diagnostics_enabled and "from_time" in kwargs:
            original_from_time = kwargs["from_time"]
            kwargs["from_time"] = self.start_time - Metric.FROM_TIME_GAP

        try:
            data = get(self.path, verbose=False, **kwargs)
        except YtError:
            data = []

        # TODO(eshcherbin): This is used for flap diagnostics. Remove when TestSchedulerProfilingOnOperationFinished is fixed.
        if self.custom_diagnostics_enabled and "from_time" in kwargs:
            for sample in data:
                if sample["time"] < self.start_time and sample["value"] > 0:
                    print_debug("Metric \"{}\", non-negative sample before start retrieved (time={}, value={})"
                                .format(self.path, sample["time"], sample["value"]))
            data = [sample for sample in data if sample["time"] > original_from_time]
            kwargs["from_time"] = original_from_time

        # Keep last_update_samples up to date.
        from_time = kwargs["from_time"] if "from_time" in kwargs else 0
        self.last_update_samples = [sample for sample in self.last_update_samples if sample["time"] > from_time]

        # Filter out samples that were already seen before. They will be here because of FROM_TIME_GAP.
        data = [sample for sample in data if sample not in self.last_update_samples]
        data = sorted(data, key=lambda x: x["time"])
        self.last_update_samples = sorted(self.last_update_samples + data, key=lambda x: x["time"])

        if self.with_tags is not None:
            def check_tags(sample):
                for tag_name, tag_value in self.with_tags.iteritems():
                    if tag_name not in sample["tags"] or sample["tags"][tag_name] != tag_value:
                        return False
                return True

            data = filter(check_tags, data)

        if self.grouped_by_tags is not None:
            data_by_tags = defaultdict(list)
            for sample in data:
                tag_values = tuple(sample["tags"][tag_name] if tag_name in sample["tags"] else None
                                   for tag_name in self.grouped_by_tags)
                # If a tag from `grouped_by_tags` is missing, skip the sample.
                if None in tag_values:
                    continue
                data_by_tags[tag_values].append(sample)

            return data_by_tags
        return data


def from_sandbox(resource_id):
    print_debug("Fetching resource {0}".format(resource_id))
    sandbox_url = "https://proxy.sandbox.yandex-team.ru/{0}".format(resource_id)
    response = requests.get(sandbox_url)
    if not response:
        if response.status_code == 404:
            pytest.fail("Resource #{0} not found".format(resource_id))
        else:
            pytest.skip("Unable to get sandbox resource {0}, status_code {1}".format(resource_id, response.status_code))
    print_debug("")
    file_obj = BytesIO(response.content)
    tar = tarfile.open(fileobj=file_obj)
    tar.list()
    tar.extractall()
