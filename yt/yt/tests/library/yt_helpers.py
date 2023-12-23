from yt_commands import get, get_driver, set, ls, create_pool_tree, print_debug, master_exit_read_only
from yt.test_helpers import wait
from yt.test_helpers.profiler import ProfilerFactory

import yt.yson as yson

from yt.wrapper import YtClient

from collections import defaultdict
from datetime import datetime
from dateutil import parser
from dateutil.tz import tzlocal
import pytest
import json
import inspect

MAX_DECIMAL_PRECISION = 35


class JobCountProfiler:
    def __init__(self, state, tags=None, **kwargs):
        assert state in ["started", "failed", "aborted", "completed"]
        path = "controller_agent/jobs/{}_job_count".format(state)
        self._counters = [
            profiler_factory().at_controller_agent(agent, fixed_tags=tags, **kwargs).counter(path)
            for agent in ls("//sys/controller_agents/instances")
        ]

    def get_job_count_delta(self, **kwargs):
        delta = 0
        for counter in self._counters:
            delta += counter.get_delta(verbose=True, **kwargs)

        return delta


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


def skip_if_no_descending(env):
    skip_if_old(env, (21, 1), "do not support descending yet")


def skip_if_renaming_disabled(env):
    skip_if_old(env, (22, 2), "do not support column renaming")


def skip_if_renaming_not_differentiated(env):
    skip_if_old(env, (23, 1), "not differentiated renaming in static vs dynamic")


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


def wait_no_peers_in_read_only(driver=None):
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
    for cell_tag in secondary_masters:
        addresses = list(secondary_masters[cell_tag].keys())
        wait(lambda: no_peers_in_read_only("//sys/secondary_masters/{}".format(cell_tag), addresses))


def master_exit_read_only_sync(driver=None):
    master_exit_read_only(driver=driver)
    wait_no_peers_in_read_only(driver=driver)
