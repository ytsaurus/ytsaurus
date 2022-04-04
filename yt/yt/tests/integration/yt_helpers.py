from yt_commands import get, get_driver, set, ls, create_pool_tree, print_debug
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

MAX_DECIMAL_PRECISION = 35


class JobCountProfiler:
    def __init__(self, state, tags=None):
        assert state in ["started", "failed", "aborted", "completed"]
        path = "controller_agent/jobs/{}_job_count".format(state)
        self._counters = [
            profiler_factory().at_controller_agent(agent, fixed_tags=tags).counter(path)
            for agent in ls("//sys/controller_agents/instances")
        ]

    def get_job_count_delta(self, tags=None):
        delta = 0
        for counter in self._counters:
            delta += counter.get_delta(tags=tags)

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
    for projection in scheduler_profiler.get_all("scheduler/jobs/running_job_count", {}, verbose=False):
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


def write_log_barrier(address, category="Barrier", driver=None):
    if driver is None:
        driver = get_driver()
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

    with open(path, "r") as fd:
        for line in fd:
            try:
                if format == "json":
                    parsed_line = json.loads(line)
                elif format == "yson":
                    parsed_line = yson.loads(line)
            except ValueError:
                continue

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
