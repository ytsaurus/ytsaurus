from __future__ import print_function

from yt.wrapper.client import create_client_with_command_params
from yt.wrapper.format import YsonFormat

from yt.common import YtResponseError

from collections import defaultdict
from datetime import datetime
import sys


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
# > total_time_completed_parent_counter =
# >     profiler.counter("scheduler/pools/metrics/total_time_completed", {"pool": "parent"})
# > ...
# > wait(lambda: total_time_completed_parent_counter.get_delta() > 0)
#
# Troubleshooting and debugging:
# (*) You can list all the sensors via the profiler.list() method.
# (*) Be sure to specify the _exact_ set of tags for the wanted projection. If a projection could not be found,
#     check for any required tags you might be missing. It is super useful to call sensor.get_all() method to list
#     all the available projections for the sensor.
class Profiler(object):
    def __init__(self, yt_client, path, fixed_tags=None, namespace="yt"):
        if fixed_tags is None:
            fixed_tags = {}
        self.yt_client = yt_client
        self.path = path
        self.namespace = namespace
        self.fixed_tags = fixed_tags

    @staticmethod
    def at_scheduler(yt_client, **kwargs):
        return Profiler(yt_client, "//sys/scheduler/orchid/sensors", **kwargs)

    @staticmethod
    def at_node(yt_client, node, *args, **kwargs):
        return Profiler(yt_client, "//sys/cluster_nodes/{0}/orchid/sensors".format(node), *args, **kwargs)

    @staticmethod
    def at_job_proxy(yt_client, node, *args, **kwargs):
        return Profiler(yt_client, "//sys/cluster_nodes/{0}/orchid/job_proxy_sensors".format(node), *args, **kwargs)

    @staticmethod
    def at_tablet_node(yt_client, table, tablet_cell_bundle="default", fixed_tags=None):
        if fixed_tags is None:
            fixed_tags = {}
        fixed_tags["tablet_cell_bundle"] = tablet_cell_bundle

        tablets = yt_client.get(table + "/@tablets")
        assert len(tablets) == 1
        address = yt_client.get("#{0}/@peers/0/address".format(tablets[0]["cell_id"]))
        return Profiler(
            yt_client,
            "//sys/cluster_nodes/{0}/orchid/sensors".format(address),
            namespace="yt/tablet_node",
            fixed_tags=fixed_tags
        )

    @staticmethod
    def at_master(yt_client, master_index=0, **kwargs):
        primary_masters = [key for key in yt_client.get("//sys/primary_masters")]
        return Profiler(
            yt_client,
            "//sys/primary_masters/{0}/orchid/sensors".format(primary_masters[master_index]),
            **kwargs
        )

    @staticmethod
    def at_proxy(yt_client, proxy, **kwargs):
        return Profiler(yt_client, "//sys/proxies/{0}/orchid/sensors".format(proxy), **kwargs)

    @staticmethod
    def at_rpc_proxy(yt_client, proxy, *args, **kwargs):
        return Profiler(yt_client, "//sys/rpc_proxies/{0}/orchid/sensors".format(proxy), *args, **kwargs)

    def with_tags(self, tags):
        return Profiler(self.yt_client, self.path, fixed_tags=dict(self.fixed_tags, **tags), namespace=self.namespace)

    def list(self, **kwargs):
        return self.yt_client.list(self.path, format=YsonFormat(), **kwargs)

    def get(self, name, tags, verbose=True, verbose_value_name="value", postprocessor=lambda value: value, default=None, **kwargs):
        tags = dict(self.fixed_tags, **tags)

        if "export_summary_as_max" in kwargs:
            export_summary_as_max = kwargs["export_summary_as_max"]
            del kwargs["export_summary_as_max"]
        else:
            export_summary_as_max = True

        if "read_all_projections" in kwargs:
            read_all_projections = kwargs["read_all_projections"]
            del kwargs["read_all_projections"]
        else:
            read_all_projections = False

        client = create_client_with_command_params(
            self.yt_client,
            tags=tags,
            name="{}/{}".format(self.namespace, name),
            export_summary_as_max=export_summary_as_max,
            read_all_projections=read_all_projections
        )

        try:
            value = client.get(self.path, **kwargs)
        except YtResponseError as err:
            if err.is_resolve_error():
                value = default
        if value is not None:
            value = postprocessor(value)
        if verbose:
            print("{} of sensor \"{}\" with tags {}: {}"
                  .format(verbose_value_name.capitalize(), name, tags, value), file=sys.stderr)

        return value

    def get_all(self, name, tags, **kwargs):
        return self.get(name, tags, verbose_value_name="projections", read_all_projections=True, default=[], **kwargs)

    class _Sensor(object):
        def __init__(self, profiler, name, fixed_tags):
            self.profiler = profiler
            self.name = name
            self.fixed_tags = fixed_tags

        def get(self, tags=None, **kwargs):
            if tags is None:
                tags = {}
            return self.profiler.get(self.name, dict(self.fixed_tags, **tags), **kwargs)

        # NB(eshcherbin): This method is used mostly for debugging purposes.
        def get_all(self, tags=None, **kwargs):
            if tags is None:
                tags = {}
            return self.profiler.get_all(self.name, dict(self.fixed_tags, **tags), **kwargs)

    class Gauge(_Sensor):
        def get(self, *args, **kwargs):
            return super(Profiler.Gauge, self).get(*args, postprocessor=float, **kwargs)

    class Counter(_Sensor):
        def __init__(self, profiler, name, tags):
            super(Profiler.Counter, self).__init__(profiler, name, fixed_tags=tags)
            self.start_value = self.get(default=0)

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

    def gauge(self, name, fixed_tags=None):
        if fixed_tags is None:
            fixed_tags = {}
        return Profiler.Gauge(self, name, fixed_tags)

    def counter(self, name, tags=None):
        if tags is None:
            tags = {}
        return Profiler.Counter(self, name, tags)

    def summary(self, name, fixed_tags=None):
        if fixed_tags is None:
            fixed_tags = {}
        return Profiler.Summary(self, name, fixed_tags)

    def histogram(self, name, fixed_tags=None):
        if fixed_tags is None:
            fixed_tags = {}
        return Profiler.Histogram(self, name, fixed_tags)


def get_job_count_profiling(yt_client, tree="default"):
    job_count = {"state": defaultdict(int), "abort_reason": defaultdict(int)}
    profiler = Profiler.at_scheduler(yt_client, fixed_tags={"tree": tree})

    start_time = datetime.now()

    # Enable verbose for debugging.
    for projection in profiler.get_all("scheduler/jobs/running_job_count", {}, verbose=False):
        if ("state" not in projection["tags"]) or ("job_type" in projection["tags"]):
            continue
        job_count["state"][projection["tags"]["state"]] = int(projection["value"])

    job_count["state"]["completed"] = int(
        profiler.get(
            "scheduler/jobs/completed_job_count",
            tags={},
            default=0,
            verbose=False
            )
        )

    for projection in profiler.get_all("scheduler/jobs/aborted_job_count", tags={}, verbose=False):
        if "job_type" in projection["tags"]:
            continue
        if "abort_reason" in projection["tags"]:
            job_count["abort_reason"][projection["tags"]["abort_reason"]] = int(projection["value"])
        else:
            job_count["state"]["aborted"] = int(projection["value"])

    duration = (datetime.now() - start_time).total_seconds()

    # Enable it for debugging.
    print("job_counters (took {} seconds to calculate): {}".format(duration, job_count), file=sys.stderr)

    return job_count
