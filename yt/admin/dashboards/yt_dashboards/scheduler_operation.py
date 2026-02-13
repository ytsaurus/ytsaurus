# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

try:
    from yt_dashboards.constants import (
        SCHEDULER_DASHBOARD_DEFAULT_CLUSTER,
        SCHEDULER_DASHBOARD_DEFAULT_POOL,
        SCHEDULER_DASHBOARD_DEFAULT_TREE,
    )
except ImportError:
    from yt_dashboards.yandex_constants import (
        SCHEDULER_DASHBOARD_DEFAULT_CLUSTER,
        SCHEDULER_DASHBOARD_DEFAULT_POOL,
        SCHEDULER_DASHBOARD_DEFAULT_TREE,
    )

from .common.sensors import SchedulerOperations

try:
    from .yandex_constants import (
        JOB_STATISTICS_DOCUMENTATION_URL,
    )
except ImportError:
    JOB_STATISTICS_DOCUMENTATION_URL = ""

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, MultiSensor, Text, Title
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter

##################################################################


def _build_resource_usage(d):
    def build_resource_sensor(resource):
        resource_in_config = resource if resource != "user_memory" else "memory"
        return MultiSensor(
            SchedulerOperations(f"yt.scheduler.operations_by_slot.resource_usage.{resource}")
                .nan_as_zero()
                .legend_format("Usage"),
            SchedulerOperations(f"yt.scheduler.operations_by_slot.resource_demand.{resource}")
                .nan_as_zero()
                .legend_format("Demand"),
            SchedulerOperations(f"yt.scheduler.operations_by_slot.specified_resource_limits.{resource_in_config}")
                .legend_format("Configured limit"),
            # Disabled because of trembling, see details in YT-19499.
            # SchedulerOperations(f"yt.scheduler.operations_by_slot.accumulated_resource_usage.{resource}.rate")
            #     .legend_format("Cumulative Usage"),
        )

    d.add(Rowset().row(height=3).cell("", Title("Resources", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .row()
            .stack(False)
            .min(0)
            .cell("CPU", build_resource_sensor("cpu"), yaxis_label="Cores", display_legend=True)
            .cell("Memory", build_resource_sensor("user_memory").unit("UNIT_BYTES_SI"), yaxis_label="Bytes", display_legend=True)
            .cell("GPU", build_resource_sensor("gpu"), yaxis_label="GPUs", display_legend=True)
    )

    DESCRIPTION = """
**Demand**: amount of resources needed to run all waiting and running jobs of the operation.<EOLN>
**Usage**: model resource consumption of the operation from scheduler's point of view. Represents the number of resources that are reserved for the operation. Actual resource consumption may differ.<EOLN>
**Configured Limit**: maximum amount of resources given to the operation that is specified in its specification and can be changed by updating runtime parameters.
"""
    d.add(Rowset().row(height=5).cell("", Text(DESCRIPTION)))


def _build_cluster_share(d):
    d.add(Rowset().row(height=3).cell("", Title("Detailed Resource Distribution", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell(
                "Dominant Shares",
                MultiSensor(
                    SchedulerOperations("yt.scheduler.operations_by_slot.dominant_usage_share")
                        .legend_format("Usage"),
                    SchedulerOperations("yt.scheduler.operations_by_slot.dominant_demand_share")
                        .legend_format("Demand"),
                    SchedulerOperations("yt.scheduler.operations_by_slot.dominant_fair_share.total")
                        .legend_format("Fair share"),
                ),
                yaxis_label="Cluster share",
                display_legend=True,
            )
            .cell(
                "Fair Share per Resource",
                MultiSensor(
                    SchedulerOperations("yt.scheduler.operations_by_slot.fair_share.total.cpu")
                        .legend_format("CPU"),
                    SchedulerOperations("yt.scheduler.operations_by_slot.fair_share.total.memory")
                        .legend_format("Memory"),
                    SchedulerOperations("yt.scheduler.operations_by_slot.fair_share.total.gpu")
                        .legend_format("GPU"),
                ),
                yaxis_label="Cluster share",
                display_legend=True,
            )
            .cell(
                "Dominant Fair Share per Type",
                MultiSensor(
                    SchedulerOperations("yt.scheduler.operations_by_slot.dominant_fair_share.strong_guarantee")
                        .legend_format("Strong guarantee"),
                    SchedulerOperations("yt.scheduler.operations_by_slot.dominant_fair_share.integral_guarantee")
                        .legend_format("Integral guarantee"),
                    SchedulerOperations("yt.scheduler.operations_by_slot.dominant_fair_share.weight_proportional")
                        .legend_format("Weight proportional"),
                ).stack(True),
                yaxis_label="Cluster share",
                display_legend=True,
            )
        )

    DESCRIPTION = """
**Usage**, **Demand**, **Fair share**: share of the cluster that is used, demanded or should be given to the pool. For example, 1.0 or 100% corresponds to the total amount of resources in the cluster.<EOLN>
**Fair Share** can be split into three parts: strong guarantee, integral guarantee and free resources, which are distributed between pools in proportion to their weights.<EOLN>
"""
    d.add(Rowset().row(height=4).cell("", Text(DESCRIPTION)))


def _build_job_metrics(d, os_documentation):
    d.add(Rowset().row(height=2).cell("", Title("Job metrics", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell("Local Disk IO",
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.user_job_io_total.rate")
                    .legend_format("User job IO rate")
                    .unit("UNIT_IO_OPERATIONS_PER_SECOND"),
                yaxis_label="IO operations/sec",
            )
            .cell("CPU", MultiSensor(
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.user_job_cpu_system.rate")
                    .legend_format("User job system cpu"),
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.user_job_cpu_user.rate")
                    .legend_format("User job user cpu"),
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.job_proxy_cpu_system.rate")
                    .legend_format("Job proxy system cpu"),
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.job_proxy_cpu_user.rate")
                    .legend_format("Job proxy user cpu"),
            )
                .stack(True),
                yaxis_label="Cores")
            .cell("Memory", MultiSensor(
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.user_job_memory_mb.rate")
                    .legend_format("User job memory"),
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.job_proxy_memory_mb.rate")
                    .legend_format("Job proxy memory"),
            )
                .unit("UNIT_MEBIBYTES")
                .stack(True))
            # TODO(eshcherbin): Add expressions and divide first two sensors by 1000.
            .cell("GPU", MultiSensor(
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.gpu_load.rate")
                    .query_transformation("{query} / 1000.0")
                    .legend_format("Load"),
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.gpu_utilization_gpu.rate")
                    .query_transformation("{query} / 1000.0")
                    .legend_format("Utilization"),
                SchedulerOperations("yt.scheduler.operations_by_slot.resource_usage.gpu")
                    .legend_format("Usage"),
            ),
                yaxis_label="Sum of device fractions")
    )

    DESCRIPTION = """
**Local Disk IO**: total amount of I/O operations per second (IOPS) of all user jobs in the operation.<EOLN>
**CPU**: total detailed CPU usage statistics of all user jobs and their job proxies in the operation.<EOLN>
**Memory**: total detailed RAM usage of user job and job proxy.<EOLN>
**GPU**: total detailed GPU utilization statistics of all user jobs in the operation.

Consult [documentation]({}) for more information on the corresponding job statistics.
""".format(
        "https://ytsaurus.tech/docs/en/user-guide/problems/jobstatistics"
        if os_documentation
        else JOB_STATISTICS_DOCUMENTATION_URL)

    d.add(Rowset().row(height=5).cell("", Text(DESCRIPTION.strip())))


def build_scheduler_operation(has_porto=True, os_documentation=False):
    d = Dashboard()
    d.set_title("Scheduler Operation [Autogenerated]")
    _build_resource_usage(d)
    _build_cluster_share(d)
    if has_porto:
        _build_job_metrics(d, os_documentation)

    d.value("tree", TemplateTag("tree"))
    d.value("pool", TemplateTag("pool"))
    d.value("slot_index", TemplateTag("slot_index"))

    d.add_parameter(
        "cluster", "YT cluster", MonitoringLabelDashboardParameter("yt", "cluster", SCHEDULER_DASHBOARD_DEFAULT_CLUSTER)
    )
    d.add_parameter("cluster", "Cluster", GrafanaTextboxDashboardParameter(SCHEDULER_DASHBOARD_DEFAULT_CLUSTER))

    d.add_parameter("tree", "Tree", MonitoringLabelDashboardParameter("yt", "tree", SCHEDULER_DASHBOARD_DEFAULT_TREE))
    d.add_parameter("tree", "Tree", GrafanaTextboxDashboardParameter(SCHEDULER_DASHBOARD_DEFAULT_TREE))

    d.add_parameter("pool", "Pool", MonitoringLabelDashboardParameter("yt", "pool", SCHEDULER_DASHBOARD_DEFAULT_POOL))
    d.add_parameter("pool", "Pool", GrafanaTextboxDashboardParameter(SCHEDULER_DASHBOARD_DEFAULT_POOL))

    d.add_parameter("slot_index", "Slot index", MonitoringLabelDashboardParameter("yt", "slot_index", "-"))
    d.add_parameter("slot_index", "Slot index", GrafanaTextboxDashboardParameter("0"))

    d.set_monitoring_serializer_options(dict(default_row_height=9))

    return d
