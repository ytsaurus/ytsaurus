from yt_commands import get, get_driver, set, ls, create_pool_tree, print_debug, master_exit_read_only
from yt.test_helpers import wait
from yt.test_helpers.profiler import ProfilerFactory

import yt.yson as yson

from yt.wrapper import YtClient

import builtins
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from dateutil import parser
from dateutil.tz import tzlocal
from fnmatch import fnmatchcase

import inspect
import json
import os.path
import platform
import pytest
import re


class JobCountProfiler:
    def __init__(self, state, tags=None, **kwargs):
        assert state in ["started", "failed", "aborted", "completed"]
        path = "controller_agent/jobs/{}_job_count".format(state)
        self._counters = [
            profiler_factory().at_controller_agent(agent, fixed_tags=tags, **kwargs).counter(path)
            for agent in ls("//sys/controller_agents/instances")
        ]

    def get_job_count_delta(self, **kwargs):
        return sum(counter.get_delta(verbose=True, **kwargs) for counter in self._counters)

    def get(self, **kwargs):
        return sum(counter.get(verbose=True, **kwargs) or 0 for counter in self._counters)


def profiler_factory():
    yt_client = YtClient(config={
        "enable_token": False,
        "backend": "native",
        "driver_config": get_driver().get_config(),
    })
    return ProfilerFactory(yt_client)


def get_job_count_profiling(tree="default"):
    job_count = {"state": defaultdict(int), "abort_reason": defaultdict(int)}
    scheduler_profiler = profiler_factory().at_scheduler(fixed_tags={"tree": tree})

    start_time = datetime.now()

    # Enable verbose for debugging.
    for projection in scheduler_profiler.get_all("scheduler/allocations/running_allocation_count", {}, verbose=False):
        if ("state" not in projection["tags"]) or ("job_type" in projection["tags"]):
            continue
        job_count["state"][projection["tags"]["state"]] = int(projection["value"])

    for controller_agent in ls("//sys/controller_agents/instances"):
        controller_agent_profiler = profiler_factory().at_controller_agent(controller_agent, fixed_tags={"tree": tree})

        job_count["state"]["completed"] += int(
            controller_agent_profiler.get(
                "controller_agent/jobs/completed_job_count",
                tags={},
                default=0,
                verbose=False,
            )
        )

        for projection in controller_agent_profiler.get_all("controller_agent/jobs/aborted_job_count", tags={}, verbose=False):
            if "job_type" in projection["tags"]:
                continue
            if "abort_reason" in projection["tags"]:
                job_count["abort_reason"][projection["tags"]["abort_reason"]] += int(projection["value"])
            else:
                job_count["state"]["aborted"] += int(projection["value"])

    duration = (datetime.now() - start_time).total_seconds()

    # Enable it for debugging.
    print_debug("Job counters (took {} seconds to calculate): {}".format(duration, job_count))

    return job_count


def parse_yt_time(time):
    return parser.parse(time)


def get_current_time():
    return datetime.now(tzlocal())


def create_custom_pool_tree_with_one_node(pool_tree):
    tag = pool_tree
    node = ls("//sys/cluster_nodes")[0]
    set("//sys/cluster_nodes/" + node + "/@user_tags/end", tag)
    set("//sys/pool_trees/default/@config/node_tag_filter", "!" + tag)
    create_pool_tree(pool_tree, config={"node_tag_filter": tag})
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


def _get_chunk_owner_master_cell_profilers(cypress_path):
    cell_tag = get(cypress_path + "/@external_cell_tag", default=None)
    if cell_tag is None:
        return [profiler_factory().at_primary_master(address)
                for address in ls("//sys/primary_masters")]
    else:
        return [profiler_factory().at_secondary_master(cell_tag, address)
                for address in ls("//sys/secondary_masters/{0}".format(cell_tag))]


def get_chunk_owner_master_cell_counters(cypress_path, sensor_path, *args, **kwargs):
    return [profiler.counter(sensor_path, *args, **kwargs) for profiler in _get_chunk_owner_master_cell_profilers(cypress_path)]


def get_chunk_owner_master_cell_gauges(cypress_path, sensor_path, *args, **kwargs):
    return [profiler.gauge(sensor_path, *args, **kwargs) for profiler in _get_chunk_owner_master_cell_profilers(cypress_path)]


def skip_if_old(env, version_at_least, message):
    def get_message(name):
        return "{} {}".format(name, message)

    if env.get_component_version("ytserver-master").abi < version_at_least:
        pytest.skip(get_message("Masters"))
    if env.get_component_version("ytserver-controller-agent").abi < version_at_least:
        pytest.skip(get_message("Controller agents"))
    if env.get_component_version("ytserver-job-proxy").abi < version_at_least:
        pytest.skip(get_message("Job proxies"))
    if env.get_component_version("ytserver-http-proxy").abi < version_at_least:
        pytest.skip(get_message("HTTP proxies"))
    if env.get_component_version("ytserver-proxy").abi < version_at_least:
        pytest.skip(get_message("RPC proxies"))


def skip_if_component_old(env, version_at_least, component, message="too old"):
    """
    Example components: master, controller-agent, scheduler, job-proxy, http-proxy, proxy
    """
    if env.get_component_version("ytserver-" + component).abi < version_at_least:
        pytest.skip(component + " " + message)


# COMPAT(pogorelov)
def skip_if_delivery_fenced_pipe_writer_not_supported(use_new_delivery_fenced_connection):
    kernel_version = platform.release()
    major_kernel_version = tuple(int(part) for part in kernel_version.split(".")[0:2])
    print_debug("platform release:", major_kernel_version)

    if major_kernel_version >= (5, 4) and major_kernel_version < (5, 15) and use_new_delivery_fenced_connection:
        pytest.xfail("New delivery fenced pipe writer is not supported since linux kernel 5.4 and before 5.15")

    if major_kernel_version >= (5, 15) and not use_new_delivery_fenced_connection:
        pytest.xfail("Old delivery fenced pipe writer is not supported on kernels newer than 5.15")


def write_log_barrier(address, category="Barrier", driver=None, cluster_name="primary"):
    if driver is None:
        driver = get_driver(cluster=cluster_name)
    return driver.write_log_barrier(address=address, category=category)


def read_structured_log(path, from_barrier=None, to_barrier=None, format=None, category_filter=None, row_filter=None):
    if format is None:
        if path.endswith(".json.log"):
            format = "json"
        elif path.endswith(".yson.log"):
            format = "yson"
        else:
            assert False, "Cannot determine structured log format from file name"
    assert format in {"json", "yson"}

    has_start_barrier = from_barrier is None
    lines = []

    def json_generator(path):
        with open(path, "r") as fd:
            for line in fd:
                try:
                    parsed_line = json.loads(line)
                    yield parsed_line
                except ValueError:
                    continue

    def yson_generator(path):
        return yson.load(open(path, "rb"), yson_type="list_fragment")

    generator = json_generator if format == "json" else yson_generator

    for parsed_line in generator(path):
        if parsed_line.get("system_event_kind") == "barrier":
            barrier_id = parsed_line["barrier_id"]
            if barrier_id == from_barrier:
                assert not has_start_barrier, "Start barrier appeared twice"
                has_start_barrier = True
                continue
            if barrier_id == to_barrier:
                assert has_start_barrier, "End barrier appeared before start barrier"
                return lines

        if not has_start_barrier:
            continue
        if category_filter is not None and parsed_line.get("category") not in category_filter:
            continue
        if row_filter is not None and not row_filter(parsed_line):
            continue
        lines.append(parsed_line)

    assert to_barrier is None, "End barrier not found"
    return lines


def read_structured_log_single_entry(path, row_filter, from_barrier=None, to_barrier=None):
    lines = read_structured_log(path, from_barrier=from_barrier, to_barrier=to_barrier, row_filter=row_filter)
    if len(lines) != 1:
        source = inspect.getsource(row_filter)
        raise RuntimeError(f"Found {len(lines)} lines satisfying filter: {source}")
    return lines[0]


def wait_no_peers_in_read_only(driver=None, secondary_cell_tags=None):
    def no_peers_in_read_only(monitoring_prefix, master_list):
        for master in master_list:
            monitoring = get(
                "{}/{}/orchid/monitoring/hydra".format(monitoring_prefix, master),
                default=None,
                suppress_transaction_coordinator_sync=True,
                suppress_upstream_sync=True,
                driver=driver,
            )
            if monitoring is None or monitoring["read_only"]:
                return False

        return True

    primary = ls(
        "//sys/primary_masters",
        suppress_transaction_coordinator_sync=True,
        suppress_upstream_sync=True,
        driver=driver,
    )
    wait(lambda: no_peers_in_read_only("//sys/primary_masters", primary))

    secondary_masters = get(
        "//sys/secondary_masters",
        suppress_transaction_coordinator_sync=True,
        suppress_upstream_sync=True,
        driver=driver,
    )
    if secondary_cell_tags is None:
        secondary_cell_tags = secondary_masters.keys()
    for cell_tag in secondary_cell_tags:
        addresses = list(secondary_masters[cell_tag].keys())
        wait(lambda: no_peers_in_read_only("//sys/secondary_masters/{}".format(cell_tag), addresses))


def master_exit_read_only_sync(driver=None):
    master_exit_read_only(driver=driver)
    wait_no_peers_in_read_only(driver=driver)


def wait_until_unlocked(path, driver=None):
    wait(lambda: get(path + "/@lock_count", driver=driver) == 0)


def parse_version(vstring):
    pattern = r'^(\d+)\.(\d+)\.(\d+).*'
    match = re.match(pattern, vstring)
    if not match:
        raise ValueError("invalid version number '%s'" % vstring)
    (major, minor, patch) = match.group(1, 2, 3)
    return tuple(map(int, [major, minor, patch]))


def is_uring_supported():
    if platform.system() != "Linux":
        return False
    try:
        return parse_version(platform.release()) >= (5, 4, 0)
    except Exception:
        pass

    return False


def is_uring_disabled():
    proc_file = "/proc/sys/kernel/io_uring_perm"
    if not os.path.exists(proc_file):
        return False
    with open(proc_file, "r") as myfile:
        return myfile.read().strip() == '0'


def wait_and_get_controller_incarnation(agent: str):
    """
    :param agent: Controller agent's address.
    """
    incarnation_id = None

    def check():
        nonlocal incarnation_id
        incarnation_id = get(
            f"//sys/controller_agents/instances/{agent}/orchid/controller_agent/incarnation_id",
            default=None
        )
        return incarnation_id is not None

    wait(check)
    return incarnation_id


class MissingField:
    pass


MissingFieldSentinelValue = MissingField()


@dataclass
class ObjectDiff:
    diff_left: any
    diff_right: any

    def __post_init__(self):
        if isinstance(self.diff_left, dict) and isinstance(self.diff_right, dict):
            assert self.diff_left.keys() == self.diff_right.keys(), "Dict diff should have the same keys, since missing values are represented by MissingFieldSentinelValue"

    def __repr__(self):
        return str(self)

    def __str__(self):
        return format(self, "")

    def __format__(self, format_spec):
        left_name = "left"
        right_name = "right"

        if format_spec:
            left_name, right_name = format_spec.split(",")

        return self.get_diff_message(left_name=left_name, right_name=right_name)

    def __bool__(self):
        return self.diff_left != self.diff_right

    def get_diff_message(self, left_name="left", right_name="right"):
        return self._get_diff_message(self.diff_left, self.diff_right, left_name, right_name, top_level_diff=True)

    @classmethod
    def _get_diff_message(cls, diff_left, diff_right, left_name, right_name, top_level_diff):
        if not isinstance(diff_left, dict) or not isinstance(diff_right, dict):
            return {f"{left_name}_value": diff_left, f"{right_name}_value": diff_right}

        left_unique_fields = {key for key in diff_right.keys() if key is MissingFieldSentinelValue}
        right_unique_fields = {key for key in diff_left.keys() if key is MissingFieldSentinelValue}

        # XXX(apachee): Good enough for one line diff message, but this might (or should) be improved.

        diff_obj = {}

        if left_unique_fields:
            diff_obj[f"{left_name}_unique_fields"] = left_unique_fields
        if right_unique_fields:
            diff_obj[f"{right_name}_unique_fields"] = right_unique_fields

        common_fields = diff_left.keys() - left_unique_fields - right_unique_fields

        diff_obj["recursive_diff"] = {key: cls._get_diff_message(diff_left[key], diff_right[key], left_name, right_name, top_level_diff=False) for key in common_fields}

        return str(diff_obj)


def calculate_object_diff(obj1, obj2) -> ObjectDiff:
    """
    Recursively calculate the difference between two objects.

    Returns:
        ObjectDiff: object difference representation. Casting to bool will return True if objects are different, False if they are equal.
    """

    if not isinstance(obj1, dict) or not isinstance(obj2, dict):
        return ObjectDiff(diff_left=obj1, diff_right=obj2)
    assert isinstance(obj1, dict) and isinstance(obj2, dict)

    keys = builtins.set(obj1.keys()) | builtins.set(obj2.keys())
    diff_left = {}
    diff_right = {}
    for key in keys:
        diff = calculate_object_diff(obj1.get(key, MissingFieldSentinelValue), obj2.get(key, MissingFieldSentinelValue))
        diff_l = diff.diff_left
        diff_r = diff.diff_right
        assert diff_l is not MissingFieldSentinelValue or diff_r is not MissingFieldSentinelValue, f"Incorrectly calculating diff of key {key!r}, which is absent in both objects"
        if diff_l != diff_r:
            diff_left[key] = diff_l
            diff_right[key] = diff_r
    return ObjectDiff(diff_left=diff_left, diff_right=diff_right)


def validate_operation_statistics_descriptions(statistics):
    descriptions = get("//sys/scheduler/orchid/scheduler/supported_features/operation_statistics_descriptions")
    pattern_statistics = [statistic_path for statistic_path in descriptions if "*" in statistic_path]

    def _walk_statistics(statistics, base_path=""):
        # TODO(khlebnikov): Handle v1 aggregates.
        for name, value in statistics.items():
            path = f"{base_path}/{name}" if base_path else name
            if isinstance(value, dict):
                if "count" in value:
                    yield None, path, value
                else:
                    yield from _walk_statistics(value, path)
            elif isinstance(value, list):
                for group in value:
                    yield group["tags"], path, group["summary"]

    reported = builtins.set()
    for _, path, _ in _walk_statistics(statistics):
        reported.add(path)

    def _check_statistic(path):
        if path.startswith("custom/"):
            return True
        if path in descriptions:
            return True
        for pattern in pattern_statistics:
            if fnmatchcase(path, pattern):
                return True
        return False

    undescribed = builtins.set()
    for path in reported:
        if path.startswith("custom/"):
            continue
        if not _check_statistic(path):
            undescribed.add(path)

    assert not undescribed, f"Undescribed operation statistics: {undescribed}"
