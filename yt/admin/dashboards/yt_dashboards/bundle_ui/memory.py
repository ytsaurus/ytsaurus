# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor, EmptyCell
from yt_dashboard_generator.backends.monitoring import MonitoringTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.specific_tags.tags import DuplicateTag

from .resources import memory_guarantee, container_memory_usage

from ..common.sensors import *

##################################################################


def build_user_memory():
    memory_usage = (lambda category: MultiSensor(
            TabNode("yt.cluster_node.memory_usage.used").value("category", category),
            MonitoringExpr(TabNode("yt.cluster_node.memory_usage.limit")).series_max().alias("Limit"))
        .value("category", category))

    tablet_dynamic_hint = """\
Tablet dynamic memory may be normally used up to 70-80%. In case of memory pressure \
do not try to increase memory limits: it serves as a buffer, and lack of tablet \
dynamic memory almost always means that write throughput is too large.
"""

    return (Rowset()
        .stack(False)
        .top()
        .min(0)
        .row()
            .cell("Memory usage per category (not shown per-host)", MultiSensor(
                    memory_guarantee.alias("Container Memory Guarantee"),
                    container_memory_usage.alias("Container Memory Usage"),
                    MonitoringExpr(TabNode("yt.cluster_node.memory_usage.used")
                        .sensor_stack()
                        .all("category")).alias("{{category}}"))
                # There will be too many sensors for host=*
                .aggr(DuplicateTag(MonitoringTag("host")))
                .top(False))
            .cell("", EmptyCell())
        .row()
            .cell(
                "Tablet dynamic memory",
                memory_usage("tablet_dynamic"),
                description=tablet_dynamic_hint)
            .cell("Tablet static memory", memory_usage("tablet_static"))
        .row()
            .cell("Query memory usage", TabNode("yt.cluster_node.memory_usage.used").value("category", "query"))
            .cell("Tracked memory usage", TabNode("yt.cluster_node.memory_usage.total_used"))
        .row()
            .cell("Process memory usage (rss)", TabNodeMemory("yt.resource_tracker.memory_usage.rss"))
            .cell("Container (cgroup) memory usage", MultiSensor(
                TabNodeMemory("yt.memory.cgroup.rss"),
                MonitoringExpr(TabNodeMemory("yt.memory.cgroup.memory_limit")).series_max().alias("limit")))
        ).owner


def build_reserved_memory():
    TabNodeMemory = TabNode("yt.cluster_node.memory_usage.{}")
    user_categories = "block_cache|lookup_rows_cache|versioned_chunk_meta|tablet_dynamic|tablet_static"

    reserved_limit = (
        MonitoringExpr(TabNodeMemory("total_limit"))
        - MonitoringExpr(TabNodeMemory("limit").value("category", user_categories))
            .series_sum("container")
    ).top_max(1).alias("Limit")

    reserved_usage = (
        MonitoringExpr(TabNodeMemory("used")
            .value("category", "!{}".format(user_categories)))
            .series_sum("container")
            .alias("Usage {{container}}").top_max(10)
    )

    fragmentation_description = """\
Footprint: memory that is not marked with any category but logically \
belongs to the process.
Fragmentation: memory that is not reclaimed by the allocator but is not used \
by the process.
"""

    return (Rowset()
        .stack(False)
        .min(0)
        .row()
            .cell("Reserved memory usage",  MultiSensor(reserved_limit, reserved_usage))
            .cell(
                "Footprint and Fragmentation",
                MultiSensor(
                    MonitoringExpr(TabNodeMemory("used").value("category", "footprint"))
                        .alias("footprint {{container}}"),
                    MonitoringExpr(TabNodeMemory("used").value("category", "alloc_fragmentation"))
                        .alias("fragmentation {{container}}"))
                    .top(1)
                    .stack(True),
                description=fragmentation_description)
        ).owner
