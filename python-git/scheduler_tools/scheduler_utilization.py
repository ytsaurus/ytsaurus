from __future__ import print_function

from six import iteritems
from yt.wrapper import YtClient

GiB = 1024 ** 3
TiB = 1024 * GiB
PiB = 1024 * TiB


class _ResourceVector(object):
    def __init__(self, cpu, memory):
        self.cpu = cpu
        self.memory = memory

    @staticmethod
    def from_dict(resource_vector_dict):
        memory_key = "user_memory" if "user_memory" in resource_vector_dict else "memory"
        return _ResourceVector(float(resource_vector_dict["cpu"]), int(resource_vector_dict[memory_key]))

    def __getitem__(self, resource_name):
        if resource_name == "cpu":
            return self.cpu
        elif resource_name in ("memory", "user_memory"):
            return self.memory
        else:
            raise ValueError("Unexpected resource name: {}".format(resource_name))

    def __add__(self, other):
        return _ResourceVector(self.cpu + other.cpu, self.memory + other.memory)

    def __radd__(self, other):
        if other == 0:
            return self
        else:
            return self.__add__(other)

    def __sub__(self, other):
        return _ResourceVector(self.cpu - other.cpu, self.memory - other.memory)


def _compmin(v1, v2):
    return _ResourceVector(min(v1.cpu, v2.cpu), min(v1.memory, v2.memory))


class _NodeState(object):
    def __init__(self, resource_limits, resource_usage):
        self.resource_limits = resource_limits
        self.resource_usage = _compmin(resource_usage, resource_limits)
        self.free_resources = self.resource_limits - self.resource_usage

    @staticmethod
    def from_dict(node_attributes):
        return _NodeState(_ResourceVector.from_dict(node_attributes["resource_limits"]),
                          _ResourceVector.from_dict(node_attributes["resource_usage"]))


class ResourceStats(object):
    """Resource utilization statistics."""

    def __init__(self):
        # Number of invalid nodes.
        self.invalid_node_count = 0.0
        # Total limit of the resource on invalid nodes.
        self.limit_on_invalid_nodes = 0.0
        # Total limit of the resource on valid nodes.
        self.available_limit = 0.0

        # Total used amount of the resource.
        self.used = 0.0
        # Total amount of the resource which is unlikely to be ever used due to bad cpu-memory proportion on the node.
        self.not_schedulable = 0.0
        # Total free and schedulable amount of the resource.
        self.free = 0.0
        # Invariant: self.available_limit = self.used + self.not_schedulable + self.free.

        # Total amount of the resource which can be utilized without moving or aborting jobs.
        self.allocatable = 0.0
        # Total amount of the resource which is not utilized because of the skewed workload.
        self.unclaimed = 0.0
        # Total amount of the resource which is not utilized due to fragmentation.
        self.fragmented = 0.0
        # Invariant: self.free = self.allocatable + self.unclaimed + self.fragmented.

    def as_dict(self):
        return self.__dict__


def _is_online(node_attributes):
    if "state" in node_attributes:
        return node_attributes["state"] == "online"
    if "master_state" in node_attributes and "scheduler_state" in node_attributes:
        return node_attributes["master_state"] and node_attributes["scheduler_state"]
    return False


def _is_valid_node(node_attributes):
    return (all(attribute in node_attributes for attribute in ("resource_limits", "resource_usage"))
            and _is_online(node_attributes)
            and node_attributes["resource_limits"]["user_slots"] > 0)


def _get_resource_stats(nodes, resource, min_memory_per_cpu=1 * GiB, max_memory_per_cpu=8 * GiB):
    abs_error = 1e-7

    def max_hole_filling(resource_vector):
        cpu = min(resource_vector.cpu, float(resource_vector.memory) / min_memory_per_cpu)
        memory = min(resource_vector.memory, int(round(resource_vector.cpu * max_memory_per_cpu)))
        return _ResourceVector(cpu, memory)

    def create_mega_node(node_states):
        return _NodeState(sum([node_state.resource_limits for node_state in node_states]),
                          sum(node_state.resource_usage for node_state in node_states))

    def create_fixed_node_state(node_state):
        fixed_cpu_limit = max(node_state.resource_usage.cpu, max_hole_filling(node_state.resource_limits).cpu)
        fixed_memory_limit = max(node_state.resource_usage.memory, max_hole_filling(node_state.resource_limits).memory)
        fixed_resource_limits = _ResourceVector(fixed_cpu_limit, fixed_memory_limit)
        return _NodeState(fixed_resource_limits, node_state.resource_usage)

    stats = ResourceStats()

    stats.invalid_node_count = len([node_address
                                    for node_address, node_attributes in iteritems(nodes)
                                    if not _is_valid_node(node_attributes)])
    stats.limit_on_invalid_nodes = sum(_ResourceVector.from_dict(node_attributes["resource_limits"])[resource]
                                       for node_address, node_attributes in iteritems(nodes)
                                       if not _is_valid_node(node_attributes) and "resource_limits" in node_attributes)

    valid_node_states = [_NodeState.from_dict(node_attributes)
                         for node_address, node_attributes in iteritems(nodes)
                         if _is_valid_node(node_attributes)]

    if not valid_node_states:
        return stats

    fixed_node_states = [create_fixed_node_state(node_state) for node_state in valid_node_states]

    stats.available_limit = sum(node_state.resource_limits[resource] for node_state in valid_node_states)
    stats.used = sum(node_state.resource_usage[resource] for node_state in valid_node_states)
    stats.not_schedulable = (sum(node_state.free_resources[resource] for node_state in valid_node_states)
                             - sum(node_state.free_resources[resource] for node_state in fixed_node_states))
    stats.free = sum(node_state.free_resources[resource] for node_state in fixed_node_states)

    if abs(stats.available_limit - (stats.free + stats.used + stats.not_schedulable)) >= abs_error:
        raise AssertionError("Invariant is broken or absolute error is too large. "
                             "abs(stats.available_limit - (stats.free + stats.used + stats.not_schedulable)) == {}. "
                             "Expected less than {}.  "
                             "stats.available_limit = {}, "
                             "stats.free = {}, "
                             "stats.used = {}, "
                             "stats.not_schedulable = {}."
                             .format(abs(stats.available_limit - (stats.free + stats.used + stats.not_schedulable)),
                                     abs_error,
                                     stats.available_limit,
                                     stats.free,
                                     stats.used,
                                     stats.not_schedulable))

    stats.allocatable = sum(max_hole_filling(node_state.free_resources)[resource] for node_state in fixed_node_states)
    stats.unclaimed = stats.free - max_hole_filling(create_mega_node(fixed_node_states).free_resources)[resource]
    stats.fragmented = stats.free - stats.allocatable - stats.unclaimed

    if abs(stats.free - (stats.allocatable + stats.unclaimed + stats.fragmented)) >= abs_error:
        raise AssertionError("Invariant is broken or absolute error is too large. "
                             "abs(stats.free - (stats.allocatable + stats.unclaimed + stats.fragmented)) == {}. "
                             "Expected less than {}.  "
                             "stats.free = {}, "
                             "stats.allocatable = {}, "
                             "stats.unclaimed = {}, "
                             "stats.fragmented = {}."
                             .format(abs(stats.free - (stats.allocatable + stats.unclaimed + stats.fragmented)),
                                     abs_error,
                                     stats.free,
                                     stats.allocatable,
                                     stats.unclaimed,
                                     stats.fragmented))

    return stats


def get_cpu_stats(nodes):
    """Extracts statistics about cpu utilization on the cluster.

    :param dict nodes: map from node addresses to node attributes.
        Can be obtained by calling `get_all_cluster_nodes <yt.scheduler_utilization.get_all_cluster_nodes>`
        or `get_tree_nodes <yt.scheduler_utilization.get_tree_nodes>`.
    :type nodes: dict[str, dict]
    :return: an instance of `ResourceStats`.
    """

    return _get_resource_stats(nodes, "cpu")


def get_memory_stats(nodes):
    """Extracts statistics about memory utilization on the cluster.

    :param dict nodes: map from node addresses to node attributes.
        Can be obtained by calling `get_all_cluster_nodes <yt.scheduler_utilization.get_all_cluster_nodes>`
        or `get_tree_nodes <yt.scheduler_utilization.get_tree_nodes>`.
    :type nodes: dict[str, dict]
    :return: an instance of `ResourceStats`.
    """

    return _get_resource_stats(nodes, "memory")


def _format_cpu(cpu, ndigits=1):
    return "{} CPU".format(round(cpu, ndigits=ndigits))


def _format_memory(memory, ndigits=2):
    if memory >= PiB:
        return "{} PiB".format(round(float(memory) / PiB, ndigits=ndigits))
    if memory >= TiB:
        return "{} TiB".format(round(float(memory) / TiB, ndigits=ndigits))
    return "{} GiB".format(round(float(memory) / GiB, ndigits=ndigits))


def _format_resource(resource_name, resource_value):
    if resource_name.lower() == "cpu":
        return _format_cpu(resource_value)
    if resource_name.lower() in ("user_memory", "memory"):
        return _format_memory(resource_value)
    raise ValueError("Unexpected resource_name '{}'.".format(resource_name))


def _format_percents(num, denum, ndigits=1):
    return 0 if denum == 0 else round(float(num) / denum * 100, ndigits=ndigits)


def print_resource_stats(resource_name, stats, file=None):
    def formatr(resource_value):
        return _format_resource(resource_name, resource_value)

    print("Number of invalid nodes: {}".format(stats.invalid_node_count), file=file)
    print("Limit on invalid nodes: {}".format(formatr(stats.limit_on_invalid_nodes)), file=file)
    print("Available for scheduler: {}".format(formatr(stats.available_limit)), file=file)
    print("Not schedulable: {} ({}% of available)".format(
        formatr(stats.not_schedulable), _format_percents(stats.not_schedulable, stats.available_limit)), file=file)
    print("Used: {} ({}% of available)".format(
        formatr(stats.used), _format_percents(stats.used, stats.available_limit)), file=file)
    print("Free: {} ({}% of available)".format(
        formatr(stats.free), _format_percents(stats.free, stats.available_limit)), file=file)
    print("Unclaimed: {} ({}% of free)".format(
        formatr(stats.unclaimed), _format_percents(stats.unclaimed, stats.free)), file=file)
    print("Fragmented: {} ({}% of free)".format(
        formatr(stats.fragmented), _format_percents(stats.fragmented, stats.free)), file=file)
    print("Allocatable: {} ({}% of free)".format(
        formatr(stats.allocatable), _format_percents(stats.allocatable, stats.free)), file=file)


def get_all_cluster_nodes(client, source):
    """Collects information about all nodes in the cluster and their attributes.

    :param client: yt client.
    :type client: YtClient
    :param source: one of `"//sys/cluster_nodes"` or `"orchid"`
    :type source: str
    :return: map from node addresses to node attributes for all nodes in the cluster.
    """
    if source == "//sys/cluster_nodes":
        nodes = client.get("//sys/cluster_nodes", attributes=["resource_usage", "resource_limits", "state"])
        return {node_address: node_object.attributes for node_address, node_object in iteritems(nodes)}
    elif source == "orchid":
        return client.get("//sys/scheduler/orchid/scheduler/nodes")
    else:
        raise ValueError("Unexpected source: {}".format(source))


def get_tree_nodes(client, all_cluster_nodes, pool_tree):
    """Filters `all_cluster_nodes` by leaving only nodes from the pool tree `pool_tree`.

    :param client: yt client.
    :type client: YtClient
    :param dict all_cluster_nodes: map from node addresses to node attributes.
        Can be obtained by calling `get_all_cluster_nodes <yt.scheduler_utilization.get_all_cluster_nodes>`.
    :type all_cluster_nodes: dict[str, dict]
    :param pool_tree: str
    :return: map from node addresses to node attributes for all nodes in the tree.
    """
    cypress_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{}/node_addresses".format(pool_tree)
    tree_node_addresses = set(client.get(cypress_path))
    return {node_address: node_attributes for (node_address, node_attributes) in iteritems(all_cluster_nodes)
            if node_address in tree_node_addresses}
