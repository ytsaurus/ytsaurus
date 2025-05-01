# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import SchedulerOperations

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, MultiSensor, Text, Title
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter, MonitoringExpr

##################################################################

def _build_resource_usage(d):
    def resource_sensors(resource):
        return MultiSensor(
            SchedulerOperations(f"yt.scheduler.operations_by_slot.resource_usage.{resource}")
                .legend_format("usage"),
            SchedulerOperations(f"yt.scheduler.operations_by_slot.resource_demand.{resource}")
                .legend_format("demand"),
            # Disabled because of trembling, see details in YT-19499.
            # SchedulerOperations(f"yt.scheduler.operations_by_slot.accumulated_resource_usage.{resource}.rate")
            #     .legend_format("cumulative_usage"),
        )

    d.add(Rowset().row(height=2).cell("", Title("Usage and demand", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell("CPU: usage, demand", resource_sensors("cpu"), yaxis_label="CPU, cores", display_legend=True)
            .cell("RAM: usage, demand", resource_sensors("user_memory"), yaxis_label="Memory, bytes", display_legend=True)
            .cell("GPU: usage, demand", resource_sensors("gpu"), yaxis_label="GPU, units", display_legend=True)
    )


def _build_cluster_share(d):
    d.add(Rowset().row(height=2).cell("", Title("Cluster share", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell(
                "Resource shares: usage, demand, fair share",
                MultiSensor(
                    SchedulerOperations("yt.scheduler.operations_by_slot.dominant_usage_share")
                        .legend_format("dominant usage share"),
                    SchedulerOperations("yt.scheduler.operations_by_slot.dominant_demand_share")
                        .legend_format("dominant demand share"),
                    SchedulerOperations("yt.scheduler.operations_by_slot.dominant_fair_share.total")
                        .legend_format("dominant fair share"),
                ),
                display_legend=True,
            )
            .cell(
                "Fair share: CPU, RAM, GPU components",
                MultiSensor(
                    SchedulerOperations("yt.scheduler.operations_by_slot.fair_share.total.cpu")
                        .legend_format("CPU"),
                    SchedulerOperations("yt.scheduler.operations_by_slot.fair_share.total.memory")
                        .legend_format("memory"),
                    SchedulerOperations("yt.scheduler.operations_by_slot.fair_share.total.gpu")
                        .legend_format("GPU"),
                ),
                display_legend=True,
            )
            .cell(
                "Dominant fair share: strong, integral, weight proportional",
                MultiSensor(
                    SchedulerOperations("yt.scheduler.operations_by_slot.dominant_fair_share.strong_guarantee")
                        .legend_format("strong guarantee"),
                    SchedulerOperations("yt.scheduler.operations_by_slot.dominant_fair_share.integral_guarantee")
                        .legend_format("integral guarantee"),
                    SchedulerOperations("yt.scheduler.operations_by_slot.dominant_fair_share.weight_proportional")
                        .legend_format("weight proportional"),
                ).stack(True),
                display_legend=True,
            )
        )

    DESCRIPTION = """
**Usage**, **Demand**, **Fair share**: share of the cluster that is used, demanded or should be given to the operation. For example, 1.0 or 100% corresponds to the total amount of resources in the cluster.<EOLN>
**CPU. RAM, GPU fair share components**  help to determine operation's dominant resource.

In addition, fair share can be split into three parts: strong guarantee, integral guarantee and free resources, which are distributed between pools in proportion to their weights.
"""

    d.add(Rowset().row(height=5).cell("", Text(DESCRIPTION)))


def _build_job_metrics(d):
    d.add(Rowset().row(height=2).cell("", Title("Job metrics", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell("Job metrics: local disk IO",
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.user_job_io_total.rate")
                    .legend_format("user job io rate"),
            )
            .cell("Job metrics: CPU usage", MultiSensor(
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.user_job_cpu_system.rate")
                    .legend_format("user job system cpu rate"),
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.user_job_cpu_user.rate")
                    .legend_format("user job user cpu rate"),
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.job_proxy_cpu_system.rate")
                    .legend_format("job proxy system cpu rate"),
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.job_proxy_cpu_user.rate")
                    .legend_format("job proxy user cpu rate"),
            )
                .stack(True))
            .cell("Job metrics: RAM usage", MultiSensor(
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.user_job_memory_mb.rate")
                    .legend_format("user job memory"),
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.job_proxy_memory_mb.rate")
                    .legend_format("job proxy memory"),
            )
                .stack(True))
            # TODO(eshcherbin): Add expressions and divide first two sensors by 1000.
            .cell("Job metrics: GPU usage", MultiSensor(
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.gpu_load.rate")
                    .query_transformation("{query} / 1000.0")
                    .legend_format("gpu load rate"),
                SchedulerOperations("yt.scheduler.operations_by_slot.metrics.gpu_utilization_gpu.rate")
                    .query_transformation("{query} / 1000.0")
                    .legend_format("gpu utilization rate"),
                SchedulerOperations("yt.scheduler.operations_by_slot.resource_usage.gpu")
                    .legend_format("operations_by_slot gpu usage"),
            ))
    )

    DESCRIPTION = """
**Local disk I/O**: total amount of input/output operations per second (IOPS).<EOLN>
**CPU usage**: total detailed CPU usage statistics.<EOLN>
**RAM usage**: total detailed RAM usage of user job and job proxy.<EOLN>
**GPU usage**: total detailed GPU utilization statistics.

Consult [documentation](https://ytsaurus.tech/docs/en/user-guide/problems/jobstatistics) for more information on the corresponding job statistics.
"""

    d.add(Rowset().row(height=4).cell("", Text(DESCRIPTION.strip())))


def build_scheduler_operation(has_porto=True):
    d = Dashboard()
    _build_resource_usage(d)
    _build_cluster_share(d)
    if has_porto:
        _build_job_metrics(d)

    d.value("tree", TemplateTag("tree"))
    d.value("pool", TemplateTag("pool"))
    d.value("slot_index", TemplateTag("slot_index"))

    d.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter(
            "yt",
            "cluster",
            "-"))
    d.add_parameter(
        "tree",
        "Tree",
        MonitoringLabelDashboardParameter(
            "yt",
            "tree",
            "-"))
    d.add_parameter(
        "pool",
        "Pool",
        MonitoringLabelDashboardParameter(
            "yt",
            "pool",
            "-"))
    d.add_parameter(
        "slot_index",
        "Slot index",
        MonitoringLabelDashboardParameter(
            "yt",
            "slot_index",
            "-"))

    d.set_monitoring_serializer_options(dict(default_row_height=9))

    return d
