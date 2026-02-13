# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor, EmptyCell
from yt_dashboard_generator.backends.monitoring import MonitoringTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.specific_tags.tags import DuplicateTag
from yt_dashboards.jobs_monitor import BYTES_LABEL

from .resources import memory_guarantee, anon_memory_limit, anon_memory_usage, oom_tracker_threshold

from ..common.sensors import *

##################################################################


def build_user_memory(has_cgroup):
    memory_usage = (lambda category: MultiSensor(
            TabNode("yt.cluster_node.memory_usage.used").value("category", category).host_container_legend_format(),
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
                    anon_memory_limit.alias("Anon Memory Limit"),
                    oom_tracker_threshold.alias("OOM tracker threshold"),
                    anon_memory_usage.alias("Anon Memory Usage"),
                    MonitoringExpr(TabNode("yt.cluster_node.memory_usage.used")
                        .sensor_stack()
                        .all("category")).alias("{{category}}"))
                # There will be too many sensors for host=*
                .aggr(DuplicateTag(MonitoringTag("host")))
                .top(False),
                yaxis_label=BYTES_LABEL)
            .cell("", EmptyCell(), yaxis_label=BYTES_LABEL, skip_cell=not has_cgroup)
        .row()
            .cell(
                "Tablet dynamic memory",
                memory_usage("tablet_dynamic"),
                description=tablet_dynamic_hint,
                yaxis_label=BYTES_LABEL)
            .cell("Tablet static memory", memory_usage("tablet_static"), yaxis_label=BYTES_LABEL)
        .row()
            .cell("Query memory usage", memory_usage("query"), yaxis_label=BYTES_LABEL)
            .cell("Tracked memory usage", TabNode("yt.cluster_node.memory_usage.total_used").host_container_legend_format(), yaxis_label=BYTES_LABEL)
        .row()
            .cell("Process memory usage (rss)", TabNodeMemory("yt.resource_tracker.memory_usage.rss").host_container_legend_format(), yaxis_label=BYTES_LABEL)
            .cell("Container (cgroup) memory usage", MultiSensor(
                TabNodeMemory("yt.memory.cgroup.rss"),
                MonitoringExpr(TabNodeMemory("yt.memory.cgroup.memory_limit")).series_max().alias("limit")),
                yaxis_label=BYTES_LABEL,
                skip_cell=not has_cgroup)
        .row()
            .cell("Row cache size", memory_usage("lookup_rows_cache"), yaxis_label=BYTES_LABEL)
            .cell(
                "Tablet background memory",
                TabNode("yt.cluster_node.memory_usage.used").value("category", "tablet_background").host_container_legend_format(),
                yaxis_label=BYTES_LABEL)
        ).owner.unit("UNIT_BYTES_SI")


def build_reserved_memory():
    TabNodeMemory = TabNode("yt.cluster_node.memory_usage.{}")
    user_categories = "block_cache|lookup_rows_cache|versioned_chunk_meta|tablet_dynamic|tablet_static|query"

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
            .cell("Reserved memory usage",  MultiSensor(reserved_limit, reserved_usage), yaxis_label=BYTES_LABEL)
            .cell(
                "Footprint and Fragmentation",
                MultiSensor(
                    MonitoringExpr(TabNodeMemory("used").value("category", "footprint"))
                        .alias("footprint {{container}}"),
                    MonitoringExpr(TabNodeMemory("used").value("category", "alloc_fragmentation"))
                        .alias("fragmentation {{container}}"))
                    .top(1)
                    .stack(True),
                description=fragmentation_description,
                yaxis_label=BYTES_LABEL)
        ).owner.unit("UNIT_BYTES_SI")
