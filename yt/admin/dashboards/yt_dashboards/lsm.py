# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import *

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, MultiSensor
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import (
    MonitoringExpr, DownsamplingAggregation)


def generate1():
    write_dw = MonitoringExpr(NodeTablet("yt.tablet_node.write.data_weight.rate"))
    compaction_dw = MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.data_weight.rate").value("method", "compaction"))
    lookup_dw = MonitoringExpr(NodeTablet("yt.tablet_node.lookup.data_weight.rate"))
    lookup_unmerged_dw = MonitoringExpr(NodeTablet("yt.tablet_node.lookup.unmerged_data_weight.rate"))

    def _lookup_percentile(p):
        return (MonitoringExpr(NodeTablet("yt.tablet_node.multiread.request_duration.max"))
            .moving_avg("30s")
            .group_by_labels("cluster", f"v -> series_percentile({p}, v)")
            .alias("{{cluster}} p" + str(p))
            .all("host"))

    return (Rowset()
        .aggr("host")
        .row()
            .cell("Write data weight rate", write_dw)
            .cell("Lookup data weight rate", lookup_dw)
        .row()
            .cell("Write data weight rate", compaction_dw)
            .cell("Lookup unmerged data weight rate", lookup_unmerged_dw)
        .row()
            .cell("LSM write amplification", compaction_dw.moving_avg("10m") / write_dw.moving_avg("10m"))
            .cell("Lookup data weight: unmerged : merged", lookup_unmerged_dw.moving_avg("5m") / lookup_dw.moving_avg("5m"))
        .row()
            .cell("LSM out store count rate", NodeTablet("yt.tablet_node.store_compactor.out_store_count.rate")
                .aggr("activity", "eden", "reason"))
            .cell("multiread.request_duration.max percentiles", MultiSensor(
                _lookup_percentile(99), _lookup_percentile(90), _lookup_percentile(50)))
        .row()
            .cell(
                "Flushed store memory size (p50)",
                MonitoringExpr(NodeTablet("yt.tablet_node.store_rotator.rotated_memory_usage.max"))
                    .aggr("reason")
                    .all("host")
                    .drop_below(1)
                    .group_by_labels("cluster", "v -> series_percentile(50, v)")
                    .alias("{{cluster}} p50")
                    .downsampling_aggregation(DownsamplingAggregation.Last))
            .cell(
                "OSC (average maximum over nodes)",
                MonitoringExpr(NodeTablet("yt.tablet_node.tablet.overlapping_store_count.max"))
                .all("host")
                .series_avg("cluster"))
    ).owner


def generate2():
    return (Rowset()
        .aggr("host")
        .row()
            .cell("Rotation reasons", NodeTablet("yt.tablet_node.store_rotator.rotation_count.rate")
                .all("reason")
                .stack())
            .cell("Compaction reasons", MonitoringExpr(NodeTablet("yt.tablet_node.store_compactor.in_data_weight.rate"))
                .aggr("eden")
                .all("reason")
                .value("activity", "compaction")
                .alias("{{cluster}}, {{reason}}")
                .stack())
            .cell("Partition splits/merges", NodeTablet("yt.tablet_node.partition_balancer.partition_*.rate"))
    ).owner


def generate3():
    return (Rowset()
        .row()
            .cell("Table size (disk space)", Sensor("table_statistics.resource_usage.disk_space")
                .value("service", "table_statistics_monitor"))
    ).owner


def build_per_table_compaction():
    def with_common_tags(rowset: Rowset):
        return (rowset
            .aggr("user")
            .value("table_path", TemplateTag("table_path"))
            .all("tablet_cell_bundle"))

    d = Dashboard()
    d.add(with_common_tags(generate1()))
    d.add(with_common_tags(generate2()))
    d.add(generate3().value("path", TemplateTag("table_path")))
    d = d.value("cluster", TemplateTag("cluster_"))
    return d
