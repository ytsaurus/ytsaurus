# yt/admin/dashboards/yt_dashboards/data_node_local.py
# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import DatNode, DatNodeLocation, DatNodeCpu, DatNodeMemory, DatNodePorto

try:
    from .constants import (
        DATA_NODE_DASHBOARD_DEFAULT_CLUSTER,
        DATA_NODE_DASHBOARD_DEFAULT_HOST,
    )
except ImportError:
    # Default values if constants are not defined
    DATA_NODE_DASHBOARD_DEFAULT_CLUSTER = "default"
    DATA_NODE_DASHBOARD_DEFAULT_HOST = "dnd-0"

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, MultiSensor, Text, Title
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.taggable import NotEquals
from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr


def _build_location_info(d):
    """Build the location info section of the dashboard."""
    d.add(Rowset()
        .row(height=2).cell("", Title("Location info", size="TITLE_SIZE_L"))
    )
    d.add(Rowset()
        .value("location_type", "store")
        .all("medium", "location_id")
        .legend_format('{{medium}}: {{location_id}}')
        # Read/Write throughput row.
        .row()
            .cell(
                "Read throughput",
                DatNodeLocation("yt.location.read_bytes.rate"),
            )
            .cell(
                "Write throughput",
                DatNodeLocation("yt.location.written_bytes.rate"),
            )
        # Read/Write time row.
        .row()
            .cell(
                "Read time",
                DatNodeLocation("yt.location.read.time.avg"),
            )
            .cell(
                "Write time",
                DatNodeLocation("yt.location.write.time.avg"),
            )
        # Read/Write request count row.
        .row()
            .cell(
                "Read request count",
                DatNodeLocation("yt.location.read.request_count.rate"),
            )
            .cell(
                "Write request count",
                DatNodeLocation("yt.location.write.request_count.rate"),
            )
        # Sync time row.
        .row()
            .cell(
                "Sync time",
                DatNodeLocation("yt.location.sync.time.avg"),
            )
        .row()
            .cell(
                "Out throttler",
                # COMPAT: remove old metric name
                DatNodeLocation("yt.location.disk_throttler.unlimited_out.value.rate|yt.location.disk_throttler.unlimited_out_throttler.value.rate")
            )
            .cell(
                "In throttler",
                DatNodeLocation("yt.location.disk_throttler.unlimited_in.value.rate")
            )
    )


def _build_cpu_usage(d):
    """Build the CPU usage section of the dashboard."""
    # CPU usage title
    d.add(Rowset().row(height=2).cell("", Title("CPU usage", size="TITLE_SIZE_L")))

    d.add(Rowset()
        # CPU location threads and CPU non-store threads row.
        .row()
            .cell(
                "CPU store threads",
                DatNodeCpu("yt.resource_tracker.total_cpu")
                    .value("thread", "*store*")
                    .legend_format('{{thread}}')
            )
            .cell(
                "CPU non-store threads top-10",
                MonitoringExpr(
                    DatNodeCpu("yt.resource_tracker.total_cpu")
                        .value("thread", NotEquals("*store*"))
                        .legend_format('{{thread}}')
                )
                .top_avg(10)
            )
        # CPU usage and wait row.
        .row()
            .aggr("thread")
            .cell(
                "CPU usage and wait",
                MultiSensor(
                    DatNodeCpu("yt.resource_tracker.total_cpu")
                        .legend_format("usage"),
                    DatNodeCpu("yt.resource_tracker.cpu_wait")
                        .legend_format("wait"),
                ),
            )
    )


def _build_memory_network(d, has_porto):
    """Build the memory and network section of the dashboard."""
    # Memory and network title
    d.add(Rowset().row(height=2).cell("", Title("Memory and network", size="TITLE_SIZE_L")))

    # Memory usage and Net rx/tx row
    d.add(Rowset()
        .row()
            .cell(
                "Network throttled reads",
                DatNode("yt.data_node.net_throttled_reads.rate")
                    .aggr("network")
                    .legend_format("throttled reads"),
            )
            .cell(
                "Network rx/tx bytes",
                MultiSensor(
                    DatNodePorto("yt.porto.network.rx_bytes")
                        .value("container_category", "pod")
                        .legend_format("rx"),
                    DatNodePorto("yt.porto.network.tx_bytes")
                        .value("container_category", "pod")
                        .legend_format("tx"),
                ) if has_porto
                else MultiSensor()
            )
        .row()
            .cell(
                "Memory usage",
                DatNodeMemory("yt.resource_tracker.memory_usage.rss")
                    .legend_format("rss"),
            )
    )


def build_data_node_local(has_porto: bool):
    d = Dashboard()
    d.set_title("Data Node Local")
    d.set_description("")

    # Build sections
    _build_location_info(d)
    _build_cpu_usage(d)
    _build_memory_network(d, has_porto)

    # Add parameters
    d.add_parameter(
        "cluster", "YT cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", DATA_NODE_DASHBOARD_DEFAULT_CLUSTER),
        backends=["monitoring"],
    )
    d.add_parameter(
        "cluster", "Cluster",
        GrafanaTextboxDashboardParameter(DATA_NODE_DASHBOARD_DEFAULT_CLUSTER),
        backends=["grafana"],
    )
    d.value("cluster", TemplateTag("cluster"))

    d.add_parameter(
        "host", "Host",
        MonitoringLabelDashboardParameter("yt", "host", DATA_NODE_DASHBOARD_DEFAULT_HOST),
        backends=["monitoring"],
    )
    d.add_parameter(
        "host", "Host",
        GrafanaTextboxDashboardParameter(DATA_NODE_DASHBOARD_DEFAULT_HOST),
        backends=["grafana"],
    )
    d.value("host", TemplateTag("host"))

    # Set serializer options if needed
    d.set_monitoring_serializer_options(dict(default_row_height=7))

    return d
