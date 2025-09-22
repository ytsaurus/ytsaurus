# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import (
    Scheduler, SchedulerPools, NodeMonitor,
    yt_host,
)

try:
    from .constants import (
        CLUSTER_RESOURCES_DASHBOARD_DEFAULT_CLUSTER,
        CLUSTER_RESOURCES_DASHBOARD_DEFAULT_TREE,
    )
except ImportError:
    from .yandex_constants import (
        CLUSTER_RESOURCES_DASHBOARD_DEFAULT_CLUSTER,
        CLUSTER_RESOURCES_DASHBOARD_DEFAULT_TREE,
    )

try:
    from .constants import CLUSTER_RESOURCES_DASHBOARD_DEFAULT_UNDISTRIBUTED_CPU_RANGE
except ImportError:
    try:
        from .yandex_constants import CLUSTER_RESOURCES_DASHBOARD_DEFAULT_UNDISTRIBUTED_CPU_RANGE
    except ImportError:
        CLUSTER_RESOURCES_DASHBOARD_DEFAULT_UNDISTRIBUTED_CPU_RANGE = (-200_000, 200_000)

try:
    from .constants import CLUSTER_RESOURCES_DASHBOARD_DEFAULT_UNDISTRIBUTED_MEMORY_RANGE
except ImportError:
    try:
        from .yandex_constants import CLUSTER_RESOURCES_DASHBOARD_DEFAULT_UNDISTRIBUTED_MEMORY_RANGE
    except ImportError:
        CLUSTER_RESOURCES_DASHBOARD_DEFAULT_UNDISTRIBUTED_MEMORY_RANGE = (-500 * 1000**4, 500 * 1000**4)

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, MultiSensor
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.taggable import SystemFields, NotEquals

from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr

##################################################################

def _build_scheduler_resource_distribution(d):
    def resource_sensors(resource):
        return MultiSensor(
            SchedulerPools(f"yt.scheduler.pools.resource_limits.{resource}")
                .value("pool", "<Root>")
                .legend_format("total_cluster_resources"),
            Scheduler(f"yt.scheduler.distributed_resources.{resource}")
                .legend_format("distributes_resources"),
        )

    def undistributed_resources():
        sensors = [
            Scheduler(f"yt.scheduler.undistributed_resources.cpu")
                .legend_format("cpu")
                .axis(SystemFields.LeftAxis)
                .range(*CLUSTER_RESOURCES_DASHBOARD_DEFAULT_UNDISTRIBUTED_CPU_RANGE, SystemFields.LeftAxis),
            Scheduler(f"yt.scheduler.undistributed_resources.user_memory")
                .legend_format("memory")
                .axis(SystemFields.RightAxis)
                .range(*CLUSTER_RESOURCES_DASHBOARD_DEFAULT_UNDISTRIBUTED_MEMORY_RANGE, SystemFields.RightAxis)
                .unit("UNIT_BYTES_SI", SystemFields.RightAxis),
        ]
        return MultiSensor(*sensors)

    d.add(Rowset()
        .value("tree", TemplateTag("tree"))
        .stack(False)
        .row()
            .cell("CPU distributed vs total", resource_sensors("cpu"), yaxis_label="CPU, cores", display_legend=True)
            .cell(
                "Memory distributed vs total",
                resource_sensors("user_memory").unit("UNIT_BYTES_SI"),
                yaxis_label="Memory, bytes",
                display_legend=True,
            )
        .row()
            .cell(
                "Undistributed resources",
                undistributed_resources(),
                display_legend=True,
                yaxis_label={
                    SystemFields.LeftAxis: "CPU, cores",
                    SystemFields.RightAxis: "Memory, bytes",
                })
    )


def _build_unavailable_resources_on_nodes(d):
    d.add(Rowset()
        .nan_as_zero()
        .stack(True)
        .row()
            .cell(
                "CPU on disabled exec nodes",
                MonitoringExpr(
                    NodeMonitor(f"node.resources.exec.cpu")
                        .legend_format("cpu")
                        .value("host", "none")
                        .value("presented_in_yp", "true")
                        .value("state", "online")
                        .value("flavor", NotEquals("*tablet*|*dat*|*gpu*|*journal*"))
                        .value("disabled_slots_reason", NotEquals("none"))
                )
                .series_sum("disabled_slots_reason")
            )
            .cell(
                "Non-online node count",
                MonitoringExpr(
                    NodeMonitor(f"node.count")
                        .legend_format("node_count")
                        .value("host", "none")
                        .value("presented_in_yp", "true")
                        .value("state", NotEquals("online"))
                        .value("flavor", NotEquals("*cloud*"))
                        .value("disabled_slots_reason", "missing")
                )
                .top_avg(10)
                .alias("{{flavor}} in state {{state}}")
            )
    )


def build_cluster_resources(has_node_monitor):
    d = Dashboard()

    _build_scheduler_resource_distribution(d)
    if has_node_monitor:
        _build_unavailable_resources_on_nodes(d)

    d.set_title("Cluster resources [Autogenerated]")
    d.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", CLUSTER_RESOURCES_DASHBOARD_DEFAULT_CLUSTER),
        backends=["monitoring"],
    )
    d.add_parameter(
        "cluster",
        "Сluster",
        GrafanaTextboxDashboardParameter(CLUSTER_RESOURCES_DASHBOARD_DEFAULT_CLUSTER),
        backends=["grafana"],
    )
    d.add_parameter(
        "tree",
        "Pool tree",
        MonitoringLabelDashboardParameter("yt", "tree", CLUSTER_RESOURCES_DASHBOARD_DEFAULT_TREE),
        backends=["monitoring"],
    )
    d.add_parameter(
        "tree",
        "Pool tree",
        GrafanaTextboxDashboardParameter(CLUSTER_RESOURCES_DASHBOARD_DEFAULT_TREE),
        backends=["grafana"],
    )

    d.set_monitoring_serializer_options(dict(default_row_height=10))

    return d
