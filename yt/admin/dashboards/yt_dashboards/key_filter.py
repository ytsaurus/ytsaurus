# flake8: noqa

from .common.sensors import NodeTablet

try:
    from .constants import KEY_FILTER_DASHBOARD_DEFAULT_CLUSTER
except ImportError:
    KEY_FILTER_DASHBOARD_DEFAULT_CLUSTER = ""

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.backends.monitoring import (
    MonitoringTag, MonitoringLabelDashboardParameter)
from yt_dashboard_generator.sensor import MultiSensor

def build_key_filter_rowset():
    lookup_row_count = NodeTablet("yt.tablet_node.lookup.{}_count.rate")
    lookup_key_filter = NodeTablet("yt.tablet_node.lookup.key_filter.{}_key_count.rate")
    lookup_data_bytes = NodeTablet("yt.tablet_node.lookup.chunk_reader_statistics.data_bytes_read_from_{}.rate")

    return (Rowset()
        .min(0)
        .row()
            .cell("Lookup row count", MultiSensor(
                MonitoringExpr(lookup_row_count("row")).alias("row count"),
                MonitoringExpr(lookup_row_count("unmerged_row")).alias("unmerged row count"),
                MonitoringExpr(lookup_row_count("missing_row")).alias("missing row count"),
                MonitoringExpr(lookup_row_count("unmerged_missing_row")).alias("unmerged missing row count"),
            ))
            .cell("Lookup missing row count to row count", MultiSensor(
                (MonitoringExpr(lookup_row_count("missing_row")) / MonitoringExpr(lookup_row_count("row")))
                    .alias("missing : row count"),
                (MonitoringExpr(lookup_row_count("unmerged_missing_row")) / MonitoringExpr(lookup_row_count("row")))
                    .alias("unmerged missing : row count"),
            ))
        .row()
            .cell("Lookup data bytes read", MultiSensor(
                MonitoringExpr(lookup_data_bytes("cache")).alias("from cache"),
                MonitoringExpr(lookup_data_bytes("disk")).alias("from disk"),
            ))
            .cell("Lookup data bytes read per row", MultiSensor(
                (MonitoringExpr(lookup_data_bytes("cache")) / MonitoringExpr(lookup_row_count("row"))).alias("from cache"),
                (MonitoringExpr(lookup_data_bytes("disk")) / MonitoringExpr(lookup_row_count("row"))).alias("from disk"),
            ))
        .row()
            .cell("Filtration performance", MultiSensor(
                MonitoringExpr(lookup_key_filter("input")).alias("input key count"),
                MonitoringExpr(lookup_key_filter("filtered_out")).alias("filtered out key count"),
            ))
        ).owner

def build_key_filter():
    d = Dashboard()

    d.set_title("Key Filter")
    d.add_parameter("cluster", "YT cluster", MonitoringLabelDashboardParameter("yt", "cluster", KEY_FILTER_DASHBOARD_DEFAULT_CLUSTER))
    d.add_parameter("table_path", "Table path", MonitoringLabelDashboardParameter("yt", "table_path", "-"))

    d.add(build_key_filter_rowset())

    return (d
        .value(MonitoringTag("cluster"), TemplateTag("cluster"))
        .value(MonitoringTag("table_path"), TemplateTag("table_path"))
        .aggr("user", "tablet_cell_bundle", "table_tag", "host"))
