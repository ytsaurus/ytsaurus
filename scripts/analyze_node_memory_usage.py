#!/usr/bin/env python
"""
Prints nice table showing actual & estimated memory usage.
Useful to check whether node memory tracking is accurate and
whether there are lots of small allocations happening.
"""
import yt.wrapper as yt

GB = 1024 * 1024 * 1024


def x(path):
    value = yt.get(path)
    value = float(value[-1]["value"]) / GB
    return value


def main():
    for node in yt.get("//sys/nodes"):
        orchid = "//sys/nodes/%s/orchid/" % node
        actual_memory = x(orchid + "/profiling/resource_tracker/total/memory")
        lfalloc_used_memory = x(orchid + "/profiling/resource_tracker/lf_alloc/current/used")
        lfalloc_mmapped_memory = x(orchid + "/profiling/resource_tracker/lf_alloc/current/mmapped")
        lfalloc_large_blocks = x(orchid + "/profiling/resource_tracker/lf_alloc/current/large_blocks")
        lfalloc_small_blocks = x(orchid + "/profiling/resource_tracker/lf_alloc/current/small_blocks")
        expected_memory = x(orchid + "/profiling/cell_node/memory_usage/total_used")
        print "%-32s | %6.2fGB rusage (%+6.2fGB) | %6.2fGB LF/used (%+6.2fGB) | %6.2fGB LF/mmapped (%+6.2fGB): %6.2fGB Large & %6.2fGB Small | %6.2fGB CellNode" % (
            node,
            actual_memory, actual_memory - expected_memory,
            lfalloc_used_memory, lfalloc_used_memory - expected_memory,
            lfalloc_mmapped_memory, lfalloc_mmapped_memory - expected_memory,
            lfalloc_large_blocks, lfalloc_small_blocks,
            expected_memory)

if __name__ == "__main__":
    main()
