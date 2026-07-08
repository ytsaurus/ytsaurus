# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import *

try:
    from .constants import COMPACTION_DASHBOARD_DEFAULT_CLUSTER
except ImportError:
    from .yandex_constants import COMPACTION_DASHBOARD_DEFAULT_CLUSTER

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import MultiSensor, Title
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter
from yt_dashboard_generator.backends.monitoring.sensors import (
    MonitoringExpr, DownsamplingAggregation)


def generate1():
    write_dw = MonitoringExpr(NodeTablet("yt.tablet_node.write.data_weight.rate"))

    lsm_dw = MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.out_data_weight.rate")
        .value("activity", "compaction|partitioning")
        .aggr("eden", "reason"))
    comp_dw = lsm_dw.value("activity", "compaction")
    part_dw = lsm_dw.value("activity", "partitioning")

    lsm_dw_hunks = MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.hunks.out_data_weight.rate")
        .value("activity", "compaction|partitioning")
        .aggr("eden", "reason"))
    comp_dw_hunks = lsm_dw_hunks.value("activity", "compaction")
    part_dw_hunks = lsm_dw_hunks.value("activity", "partitioning")

    return [
        (Rowset()
            .aggr("host")
            .row()
                .cell("Write data weight rate", write_dw)
                .cell(
                    "Overlapping store count (average maximum over nodes)",
                    MonitoringExpr(NodeTablet("yt.tablet_node.tablet.overlapping_store_count.max"))
                    .all("host")
                    .series_avg("cluster"))
            .row()
                .cell("LSM data weight rate", lsm_dw)
                .cell("LSM data weight rate (hunks only)", lsm_dw_hunks)
            .row()
                .cell("LSM write amplification", (comp_dw + part_dw).moving_avg("10m") / write_dw.moving_avg("10m"))
                .cell("LSM write amplification (hunks only)", (comp_dw_hunks + part_dw_hunks).moving_avg("10m") / write_dw.moving_avg("10m"))
            .row()
                .cell("LSM out store count rate", NodeTablet("yt.tablet_node.store_compactor.out_store_count.rate")
                    .aggr("eden").all("reason", "activity"))
                .cell("LSM in hunk chunk count rate", NodeTablet("yt.tablet_node.store_compactor.hunks.in_hunk_chunk_count.rate")
                    .aggr("activity", "eden").all("reason", "hunk_compaction_reason"))
            .row()
                .aggr("eden")
                .all("reason")
                .value("activity", "compaction")
                .stack()
                .cell("Compaction reasons (by in data weight)", MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.in_data_weight.rate"))
                    .alias("{{cluster}}, {{reason}}"))
                .cell("Compaction reasons (by out store count)", MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.out_store_count.rate"))
                    .alias("{{cluster}}, {{reason}}"))
            .row()
                .aggr("eden")
                .all("reason")
                .value("activity", "partitioning")
                .stack()
                .cell("Partitioning reasons (by in data weight)", MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.in_data_weight.rate"))
                    .alias("{{cluster}}, {{reason}}"))
                .cell("Partitioning reasons (by out store count)", MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.out_store_count.rate"))
                    .alias("{{cluster}}, {{reason}}"))
            #  .row()
            #      .cell("Compaction in Eden / partitions (by data weight)", MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.out_data_weight.rate"))

        ).owner,
    ]

def generate15():
    lookup_dw = MonitoringExpr(NodeTablet("yt.tablet_node.lookup.data_weight.rate"))
    lookup_unmerged_dw = MonitoringExpr(NodeTablet("yt.tablet_node.lookup.unmerged_data_weight.rate"))

    def _lookup_percentile(p):
        return (MonitoringExpr(NodeTablet("yt.tablet_node.multiread.request_duration.max"))
            .moving_avg("30s")
            .group_by_labels("cluster", f"v -> series_percentile({p}, v)")
            .alias("{{cluster}} p" + str(p))
            .all("host"))

    return [
        Rowset().row(height=2).cell("", Title("Lookup timings", size="TITLE_SIZE_L")).owner,
        (Rowset()
            .row()
                .cell("Lookup data weight rate", lookup_dw)
                .cell("Lookup unmerged data weight rate", lookup_unmerged_dw)
            .row()
                .cell("Lookup data weight: unmerged : merged", lookup_unmerged_dw.moving_avg("5m") / lookup_dw.moving_avg("5m"))
                .cell("multiread.request_duration.max percentiles", MultiSensor(
                    _lookup_percentile(99), _lookup_percentile(90), _lookup_percentile(50)))
        ).owner,
    ]

def generate2():
    return [
        Rowset().row(height=2).cell("", Title("Rotation, split/merge", size="TITLE_SIZE_L")).owner,
        (Rowset()
            .aggr("host")
            .row()
                .cell("Rotation reasons", NodeTablet("yt.tablet_node.store_rotator.rotation_count.rate")
                    .all("reason")
                    .stack())
                .cell(
                    "Flushed store memory size (p50)",
                    MonitoringExpr(NodeTablet("yt.tablet_node.store_rotator.rotated_memory_usage.max"))
                        .aggr("reason")
                        .all("host")
                        .drop_below(1)
                        .group_by_labels("cluster", "v -> series_percentile(50, v)")
                        .alias("{{cluster}} p50")
                        .downsampling_aggregation(DownsamplingAggregation.Last))
                .cell("Partition splits/merges", NodeTablet("yt.tablet_node.partition_balancer.partition_*.rate"))
        ).owner,
    ]


def generate3():
    return [
        Rowset().row(height=2).cell("", Title("Table size", size="TITLE_SIZE_L")).owner,
        (Rowset()
            .aggr("host")
            .row()
                .cell("Data weight", NodeTablet("yt.tablet_node.tablet.data_weight"))
                .cell("Chunk count", NodeTablet("yt.tablet_node.tablet.chunk_count"))
            .row()
                .cell("Tablet count", NodeTablet("yt.tablet_node.tablet.tablet_count"))
                .cell("Hunk chunk count", NodeTablet("yt.tablet_node.tablet.hunk_chunk_count"))
        ).owner,
    ]


def build_per_table_compaction():
    def with_table_path_tag(rowsets: list[Rowset]):
        return [
            (rowset
                .value("table_path", TemplateTag("table_path"))
                .all("tablet_cell_bundle"))
            for rowset in rowsets
        ]

    def with_common_tags(rowsets: list[Rowset]):
        return [
            rowset.aggr("user")
            for rowset in with_table_path_tag(rowsets)
        ]

    d = Dashboard()
    d.set_title("Per-table compaction statistics")
    d.add_parameter("cluster_", "cluster", MonitoringLabelDashboardParameter("yt", "cluster", COMPACTION_DASHBOARD_DEFAULT_CLUSTER))
    d.add_parameter("table_path", "table_path", MonitoringLabelDashboardParameter("yt", "table_path", "*"))

    rowsets = []
    rowsets += with_common_tags(generate1())
    rowsets += with_common_tags(generate2())
    rowsets += with_table_path_tag(generate3())
    rowsets += with_common_tags(generate15())

    for rowset in rowsets:
        d.add(rowset)

    d = d.value("cluster", TemplateTag("cluster_"))
    return d
