from __future__ import print_function

import os
import sys

from collections import Counter


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def get_process_count(cls, index):
    master_count = cls.get_param("NUM_MASTERS", index)
    secondary_cell_count = cls.get_param("NUM_SECONDARY_MASTER_CELLS", index)
    master_cache_count = cls.get_param("NUM_MASTER_CACHES", index)
    node_count = cls.get_param("NUM_NODES", index)
    chaos_node_count = cls.get_param("NUM_CHAOS_NODES", index)
    scheduler_count = cls.get_param("NUM_SCHEDULERS", index)
    controller_agent_count = cls.get_param("NUM_CONTROLLER_AGENTS", index)

    http_proxy_count = (
        cls.get_param("NUM_HTTP_PROXIES", index) if cls.get_param("ENABLE_HTTP_PROXY", index) else 0)

    rpc_proxy_count = (
        cls.get_param("NUM_RPC_PROXIES", index) if cls.get_param("ENABLE_RPC_PROXY", index) else 0)

    if controller_agent_count is None:
        controller_agent_count = scheduler_count

    job_proxy_count = node_count if scheduler_count > 0 else 0

    return (2 * master_count * (secondary_cell_count + 1) +
            scheduler_count + controller_agent_count + master_cache_count +
            (node_count + job_proxy_count + chaos_node_count + 1) // 2 +
            (http_proxy_count + rpc_proxy_count + 1) // 2)


def get_total_process_count(cls):
    count = get_process_count(cls, 0)
    for index in range(1, cls.NUM_REMOTE_CLUSTERS + 1):
        count += get_process_count(cls, index)
    return count


def get_test_category(process_count):
    if process_count <= 8:
        return "SMALL"
    elif process_count <= 16:
        return "MEDIUM"
    elif process_count <= 24:
        return "LARGE"
    else:
        return "XLARGE"


def pytest_collection_modifyitems(items, config):
    test_category = os.getenv("YT_TEST_FILTER", None)
    counts_by_process_count = Counter()
    counts_by_category = Counter()

    if test_category is not None:
        filtered_items = []
        deselected_items = []
        for item in items:
            process_count = get_total_process_count(item.cls)
            counts_by_process_count[process_count] += 1
            counts_by_category[get_test_category(process_count)] += 1
            if test_category == get_test_category(process_count):
                filtered_items.append(item)
            else:
                deselected_items.append(item)

        config.hook.pytest_deselected(items=deselected_items)
        items[:] = filtered_items

    # eprint("counts_by_process_count: {}".format(sorted(counts_by_process_count.items())))
    # eprint("counts_by_category: {}".format(sorted(counts_by_category.items())))
