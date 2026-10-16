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


LAG_TIME_HINT = """\
This sensor represents the sum of maximum tablet lag times over all nodes. \
Only its relative values should be interpreted. Absolute value makes little sense.
"""


def generate():
    return (Rowset()
        .aggr("host")
        .stack(False)
        .row()
            .unit("UNIT_BYTES_SI_PER_SECOND")
            .cell(
                "Write data weight rate (total)",
                NodeTablet("yt.tablet_node.write.data_weight.rate")
                    .aggr("user"))
            .cell(
                "Write data weight rate (top nodes)",
                NodeTablet("yt.tablet_node.write.data_weight.rate")
                    .aggr("user")
                    .all("host")
                    .top())
        .row()
            .unit("UNIT_BYTES_SI_PER_SECOND")
            .cell(
                "Replication data weight rate (total)",
                NodeTablet("yt.tablet_node.replica.replication_data_weight.rate")
                    .all("replica_cluster")
                    .aggr("host"))
            .cell(
                "Replication data weight rate (top nodes)",
                NodeTablet("yt.tablet_node.replica.replication_data_weight.rate")
                    .aggr("replica_cluster")
                    .all("host")
                    .top())
        .row()
            .unit("UNIT_SECONDS")
            .cell(
                "Replication lag time (total)",
                NodeTablet("yt.tablet_node.replica.lag_time.sum")
                    .all("replica_cluster"),
                description=LAG_TIME_HINT)
            .cell(
                "Replication lag time (top nodes)",
                NodeTablet("yt.tablet_node.replica.lag_time.max")
                    .aggr("replica_cluster")
                    .all("host")
                    .top())
        .row()
            .cell(
                "Replication lag row count (total)",
                NodeTablet("yt.tablet_node.replica.lag_row_count")
                    .all("replica_cluster"))
            .cell(
                "Replication lag row count (top nodes)",
                NodeTablet("yt.tablet_node.replica.lag_row_count")
                    .aggr("replica_cluster")
                    .all("host")
                    .top())
        .row()
            .all("replica_cluster")
            .aggr("host")
            .cell(
                "Replication error count",
                NodeTablet("yt.tablet_node.replica.replication_error_count.rate"))
            .cell(
                "Replication network usage",
                NodeTablet("yt.tablet_node.replica.replication_bytes_throttled.rate"))
    ).owner

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


def build_dynamic_table_replication():
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
    d.set_title("Dynamic table replication")
    d.add_parameter("cluster_", "Cluster", MonitoringLabelDashboardParameter("yt", "cluster", COMPACTION_DASHBOARD_DEFAULT_CLUSTER))
    d.add_parameter("table_path", "Table path", MonitoringLabelDashboardParameter("yt", "table_path", "*"))

    rowsets = []
    rowsets += [generate()]

    for rowset in rowsets:
        d.add(rowset
            .all("tablet_cell_bundle")
            .value("table_path", TemplateTag("table_path")))

    d = d.value("cluster", TemplateTag("cluster_"))
    return d
