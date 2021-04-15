from yt_commands import get, set, ls, create_pool_tree, print_debug
from yt.test_helpers import wait

from yt.packages import requests
import decimal
import struct
import time
from datetime import datetime
from dateutil import parser
from dateutil.tz import tzlocal
import pytest
import tarfile
import os
from io import BytesIO
from collections import defaultdict

MAX_DECIMAL_PRECISION = 35


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
# NB(eshcherbin): This helper is deprecated. Use Profiler below instead.
class Metric(object):
    AGGREGATION_METHOD_TO_DEFAULT_FACTORY = {
        "none": list,  # No aggregation, just collects all samples as a list of (time, value) tuples sorted by time.
        "delta": int,  # The difference between the last value and the init value (the last value prior to initialization).
        "sum": int,  # The sum of all values.
        "max": lambda: None,  # The maximum value.
        "last": lambda: None,  # The last value.
    }

    # Default DequeuePeriod in profile manager is 100 ms, here we take 300 ms just in case.
    FROM_TIME_GAP = 1300000  # mcs

    def __init__(self, path, with_tags=None, grouped_by_tags=None, aggr_method="delta"):
        assert aggr_method in Metric.AGGREGATION_METHOD_TO_DEFAULT_FACTORY

        self.path = path
        self.aggr_method = aggr_method
        self.with_tags = with_tags
        self.grouped_by_tags = grouped_by_tags
        self.last_update_samples = []

        # Need time in mcs.
        self.start_time = int(time.time() * 1e6)
        start_samples = self._read_from_orchid()

        default_factory = Metric.AGGREGATION_METHOD_TO_DEFAULT_FACTORY[self.aggr_method]
        if self.grouped_by_tags is None:
            self.data = default_factory()
            if self.aggr_method == "delta":
                self.state = {
                    "start_value": start_samples[-1]["value"] if start_samples else 0,
                    "last_sample_time": start_samples[-1]["time"] if start_samples else 0,
                }
            elif self.aggr_method == "last":
                self.state = {"last_sample_time": 0}
            else:
                self.state = None

            if start_samples:
                self.start_time = max(self.start_time, start_samples[-1]["time"])
        else:
            self.data = defaultdict(default_factory)
            if self.aggr_method == "delta":
                self.state = defaultdict(lambda: {"start_value": 0, "last_sample_time": 0})
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
                print_debug('Metric "{}" (aggr={}): {}'.format(self.path, self.aggr_method, self.data))
            return self.data
        else:
            assert self.grouped_by_tags is not None
            if verbose:
                print_debug(
                    'Metric "{}" (aggr={}, tags={}): {}'.format(self.path, self.aggr_method, tags, self.data[tags])
                )
            return self.data[tags]

    def __getitem__(self, item):
        if isinstance(item, tuple):
            return self.get(*item)
        else:
            return self.get(item)

    def update(self, from_time=None):
        # Need time in mcs.
        update_time = int(time.time() * 1e6)

        if from_time is None:
            from_time = max(self.last_update_time - Metric.FROM_TIME_GAP, self.start_time)
        new_samples = self._read_from_orchid(from_time=from_time)

        if isinstance(new_samples, list):
            self.data, self.state = Metric._update_data(self.data, self.state, new_samples, self.aggr_method)

            if new_samples:
                update_time = max(update_time, new_samples[-1]["time"])
        else:
            for tags, samples in new_samples.iteritems():
                self.data[tags], self.state[tags] = Metric._update_data(
                    self.data[tags], self.state[tags], samples, self.aggr_method
                )

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
    def at_master(path, master_num=0, *args, **kwargs):
        primary_masters = [key for key in get("//sys/primary_masters")]
        return Metric(
            "//sys/primary_masters/{0}/orchid/profiling/{1}".format(primary_masters[master_num], path), *args, **kwargs
        )

    @staticmethod
    def at_proxy(proxy, path, *args, **kwargs):
        return Metric("//sys/proxies/{0}/orchid/profiling/{1}".format(proxy, path), *args, **kwargs)

    @staticmethod
    def _update_data(data, state, new_samples, aggr):
        new_state = state

        if aggr == "none":
            new_data = (
                sorted(data + [(sample["time"], sample["value"]) for sample in new_samples]) if new_samples else data
            )
        elif aggr == "delta":
            new_data = (
                (new_samples[-1]["value"] - state["start_value"])
                if new_samples and new_samples[-1]["time"] >= state["last_sample_time"]
                else data
            )
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
            new_data = (
                new_samples[-1]["value"]
                if new_samples and new_samples[-1]["time"] >= state["last_sample_time"]
                else data
            )
            if new_samples:
                new_state["last_sample_time"] = max(new_state["last_sample_time"], new_samples[-1]["time"])
        else:
            raise Exception('Trying to update metric data with unknown aggregator (Aggr: "{}")'.format(aggr))

        return new_data, new_state

    # NB(eshcherbin): **kwargs is used only for `from_time` argument.
    def _read_from_orchid(self, **kwargs):
        data = get(self.path, default=[], verbose=False, **kwargs)

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
                tag_values = tuple(
                    sample["tags"][tag_name] if tag_name in sample["tags"] else None
                    for tag_name in self.grouped_by_tags
                )
                # If a tag from `grouped_by_tags` is missing, skip the sample.
                if None in tag_values:
                    continue
                data_by_tags[tag_values].append(sample)

            return data_by_tags
        return data


# This class provides an interface for the Solomon exporter component which manages all the profiling data
# of our server components. For every sensor and for every possible set of tags Solomon exporter keeps
# an aggregated value in the same manner as it would be seen in Solomon. A set of tags and the corresponding
# aggregated value is called a projection. Note that only the most recent projections are stored, so
# it is impossible to get any historical data.
#
# How to start:
# (1) Create a Profiler object for the needed component via one of the static Profiler.at_xxx() methods.
# (2) Read the most recent value of a sensor using profiler.get(name, tags).
#
# Use gauge(), counter(), summary() and histogram() methods to create a convenient wrapper for
# the corresponding type of sensor.
#
# Example:
# > profiler = Profiler.at_scheduler(fixed_tags={"tree": "default"})
# > total_time_completed_parent_counter = profiler.counter("scheduler/pools/metrics/total_time_completed", {"pool": "parent"})
# > ...
# > wait(lambda: total_time_completed_parent_counter.get_delta() > 0)
#
# Troubleshooting and debugging:
# (*) You can list all the sensors via the profiler.list() method.
# (*) Be sure to specify the _exact_ set of tags for the wanted projection. If a projection could not be found,
#     check for any required tags you might be missing. It is super useful to call sensor.get_all() method to list
#     all the available projections for the sensor.
class Profiler(object):
    def __init__(self, path, fixed_tags={}, namespace="yt"):
        self.path = path
        self.namespace = namespace
        self.fixed_tags = fixed_tags

    @staticmethod
    def at_scheduler(**kwargs):
        return Profiler("//sys/scheduler/orchid/sensors", **kwargs)

    @staticmethod
    def at_node(node, *args, **kwargs):
        return Profiler("//sys/cluster_nodes/{0}/orchid/sensors".format(node), *args, **kwargs)

    @staticmethod
    def at_tablet_node(table, tablet_cell_bundle="default", fixed_tags={}):
        fixed_tags["tablet_cell_bundle"] = tablet_cell_bundle

        tablets = get(table + "/@tablets")
        assert len(tablets) == 1
        address = get("#{0}/@peers/0/address".format(tablets[0]["cell_id"]))
        return Profiler(
            "//sys/cluster_nodes/{0}/orchid/sensors".format(address),
            namespace="yt/tablet_node",
            fixed_tags=fixed_tags)

    @staticmethod
    def at_master(master_index=0, **kwargs):
        primary_masters = [key for key in get("//sys/primary_masters")]
        return Profiler(
            "//sys/primary_masters/{0}/orchid/sensors".format(primary_masters[master_index]),
            **kwargs
        )

    @staticmethod
    def at_proxy(proxy, **kwargs):
        return Profiler("//sys/proxies/{0}/orchid/sensors".format(proxy), **kwargs)

    @staticmethod
    def at_rpc_proxy(proxy, *args, **kwargs):
        return Profiler("//sys/rpc_proxies/{0}/orchid/sensors".format(proxy), *args, **kwargs)

    def with_tags(self, tags):
        return Profiler(self.path, fixed_tags=dict(self.fixed_tags, **tags), namespace=self.namespace)

    def list(self, **kwargs):
        return ls(self.path, **kwargs)

    def get(self, name, tags, verbose=True, verbose_value_name="value", postprocessor=lambda value: value, **kwargs):
        if "default" not in kwargs:
            kwargs["default"] = None

        tags = dict(self.fixed_tags, **tags)
        value = get(self.path, name="{}/{}".format(self.namespace, name), tags=tags, verbose=True, **kwargs)
        if value is not None:
            value = postprocessor(value)
        if verbose:
            print_debug("{} of sensor \"{}\" with tags {}: {}".format(verbose_value_name.capitalize(), name, tags, value))

        return value

    def get_all(self, name, tags, **kwargs):
        return self.get(name, tags, verbose_value_name="projections", read_all_projections=True, default=[], **kwargs)

    class _Sensor(object):
        def __init__(self, profiler, name, fixed_tags):
            self.profiler = profiler
            self.name = name
            self.fixed_tags = fixed_tags

        def get(self, tags={}, **kwargs):
            return self.profiler.get(self.name, dict(self.fixed_tags, **tags), **kwargs)

        # NB(eshcherbin): This method is used mostly for debugging purposes.
        def get_all(self, tags={}, **kwargs):
            return self.profiler.get_all(self.name, dict(self.fixed_tags, **tags), **kwargs)

    class Gauge(_Sensor):
        def get(self, *args, **kwargs):
            return super(Profiler.Gauge, self).get(*args, postprocessor=float, **kwargs)

    class Counter(_Sensor):
        def __init__(self, profiler, name, tags, driver=None):
            super(Profiler.Counter, self).__init__(profiler, name, fixed_tags=tags)
            self.start_value = self.get(default=0, driver=driver)

        def get_delta(self, **kwargs):
            return self.get(
                verbose_value_name="delta",
                postprocessor=lambda value: int(value) - self.start_value,
                default=0,
                **kwargs
            )

    class Summary(_Sensor):
        def get_summary(self, *args, **kwargs):
            return self.get(
                export_summary_as_max=False,
                verbose_value_name="summary",
                *args,
                **kwargs
            )

        def get_max(self, *args, **kwargs):
            return self.get(
                export_summary_as_max=True,
                verbose_value_name="max",
                *args,
                **kwargs
            )

        def _get_summary_part(self, part, *args, **kwargs):
            return self.get(
                export_summary_as_max=False,
                verbose_value_name=part,
                postprocessor=lambda summary: summary[part],
                *args,
                **kwargs
            )

        def get_min(self, *args, **kwargs):
            return self._get_summary_part("min", *args, **kwargs)

        def get_last(self, *args, **kwargs):
            return self._get_summary_part("last", *args, **kwargs)

        def get_count(self, *args, **kwargs):
            return self._get_summary_part("count", *args, **kwargs)

    class Histogram(_Sensor):
        def get_bins(self, *args, **kwargs):
            return self.get(
                verbose_value_name="bins",
                default=[],
                *args,
                **kwargs
            )

    def gauge(self, name, fixed_tags={}):
        return Profiler.Gauge(self, name, fixed_tags)

    # FIXME(eshcherbin): This is very ad-hoc but RPC proxy tests with counters just can't live without it.
    def counter(self, name, tags={}, driver=None):
        return Profiler.Counter(self, name, tags, driver=driver)

    def summary(self, name, fixed_tags={}):
        return Profiler.Summary(self, name, fixed_tags)

    def histogram(self, name, fixed_tags={}):
        return Profiler.Histogram(self, name, fixed_tags)


# NB(eshcherbin): Custom driver is used in tests where default driver uses RPC. Options are not supported in RPC yet.
def get_job_count_profiling(tree="default", driver=None):
    job_count = {"state": defaultdict(int), "abort_reason": defaultdict(int)}
    profiler = Profiler.at_scheduler(fixed_tags={"tree": tree})

    start_time = datetime.now()

    # Enable verbose for debugging.
    for projection in profiler.get_all("scheduler/jobs/running_job_count", {}, verbose=False, driver=driver):
        if ("state" not in projection["tags"]) or ("job_type" in projection["tags"]):
            continue
        job_count["state"][projection["tags"]["state"]] = int(projection["value"])

    job_count["state"]["completed"] = int(profiler.get("scheduler/jobs/completed_job_count", tags={}, default=0, verbose=False, driver=driver))

    for projection in profiler.get_all("scheduler/jobs/aborted_job_count", tags={}, verbose=False, driver=driver):
        if "job_type" in projection["tags"]:
            continue
        if "abort_reason" in projection["tags"]:
            job_count["abort_reason"][projection["tags"]["abort_reason"]] = int(projection["value"])
        else:
            job_count["state"]["aborted"] = int(projection["value"])

    duration = (datetime.now() - start_time).total_seconds()

    # Enable it for debugging.
    print_debug("job_counters (took {} seconds to calculate): {}".format(duration, job_count))

    return job_count


def parse_yt_time(time):
    return parser.parse(time)


def get_current_time():
    return datetime.now(tzlocal())


def create_custom_pool_tree_with_one_node(pool_tree):
    tag = pool_tree
    node = ls("//sys/cluster_nodes")[0]
    set("//sys/cluster_nodes/" + node + "/@user_tags/end", tag)
    set("//sys/pool_trees/default/@config/nodes_filter", "!" + tag)
    create_pool_tree(pool_tree, config={"nodes_filter": tag})
    wait(lambda: tag in get("//sys/scheduler/orchid/scheduler/nodes/{}/tags".format(node)))
    wait(lambda: pool_tree in ls("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"))
    return node


def filter_tests(name_pred=None):
    """Leave only tests with names satisfying name_pred"""

    def decorate_class(cls):
        for method_name in list(dir(cls)):
            if method_name.startswith("test_") and not name_pred(method_name):
                setattr(cls, method_name, None)
        return cls

    return decorate_class


def skip_if_no_descending(env):
    if env.get_component_version("ytserver-master").abi <= (20, 3):
        pytest.skip("Masters do not support descending yet")
    if env.get_component_version("ytserver-controller-agent").abi <= (20, 3):
        pytest.skip("Controller agents do not support descending yet")
    if env.get_component_version("ytserver-job-proxy").abi <= (20, 3):
        pytest.skip("Job proxies do not support descending yet")
    if env.get_component_version("ytserver-http-proxy").abi <= (20, 3):
        pytest.skip("Http proxies do not support descending yet")
    if env.get_component_version("ytserver-proxy").abi <= (20, 3):
        pytest.skip("Rpc proxies do not support descending yet")
