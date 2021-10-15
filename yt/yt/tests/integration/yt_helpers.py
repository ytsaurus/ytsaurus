from yt_commands import get, get_driver, set, ls, create_pool_tree
from yt.test_helpers import wait

import yt.yson as yson

from datetime import datetime
from dateutil import parser
from dateutil.tz import tzlocal
import pytest
import json

MAX_DECIMAL_PRECISION = 35


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


def write_log_barrier(address, category):
    return get_driver().write_log_barrier(address=address, category=category)


def read_structured_log(path, from_barrier=None, to_barrier=None, format=None, category_filter=None, filter=None):
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
            if filter is not None and not filter(parsed_line):
                continue
            lines.append(parsed_line)

    assert to_barrier is None, "End barrier not found"
    return lines
