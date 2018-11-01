from . import yson

import os
import sys

def write_statistics(dict):
    """Writes user statistics to proper file descriptor.
    This function must be called from job.

    :param dict dict: python dictionary with statistics.
    """
    try:
        # File descriptor 5 is reserved for user statistics
        os.write(5, yson.dumps([dict], yson_type="list_fragment"))
    except OSError:
        sys.stderr.write("Failed to write user statistics\n")

def _get_field(path, name):
    data = open(path, "rb").read()
    for line in data.split("\n"):
        if line.startswith(name):
            return int(line.split()[1])

def _get_value(path):
    return int(open(path, "rb").read())

def get_blkio_cgroup_statistics():
    """Returns map with blkio statistics from cgroup."""
    blkio_cgroup_path = os.environ.get("YT_CGROUP_BLKIO")
    if not blkio_cgroup_path:
        return {}

    return {
        "total_amount": _get_field(os.path.join(blkio_cgroup_path, "blkio.io_serviced_recursive"), "Total"),
        "total_bytes": _get_field(os.path.join(blkio_cgroup_path, "blkio.io_service_bytes_recursive"), "Total"),
        "total_time": _get_field(os.path.join(blkio_cgroup_path, "blkio.io_service_time_recursive"), "Total")}

def get_memory_cgroup_statistics():
    """Returns map with memory statistics from cgroups."""
    # TODO(ignat): this env variable does not exist anymore. YT-9716.
    memory_cgroup_path = os.environ.get("YT_CGROUP_MEMORY")
    if not memory_cgroup_path:
        return {}

    stat_path = os.path.join(memory_cgroup_path, "memory.stat")
    return {"usage_in_bytes": _get_field(stat_path, "total_rss") + _get_field(stat_path, "total_mapped_file")}
        #"max_usage_in_bytes": _get_value(os.path.join(memory_cgroup_path, "memory.max_usage_in_bytes"))
