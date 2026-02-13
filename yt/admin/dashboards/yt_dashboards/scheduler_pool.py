# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import (
    Scheduler, SchedulerMemory, SchedulerCpu, SchedulerPools, SchedulerInternal, SchedulerRpc,
    CA, CAMemory, CACpu, CAInternal, CARpc,
    yt_host,
)

try:
    from .constants import (
        INTEGRAL_GUARANTEES_DOCUMENTATION_URL,
        JOB_STATISTICS_DOCUMENTATION_URL,
        RESOURCE_REQUEST_DESCRIPTION,
        SCHEDULER_DASHBOARD_DEFAULT_CLUSTER,
        SCHEDULER_DASHBOARD_DEFAULT_POOL,
        SCHEDULER_DASHBOARD_DEFAULT_TREE,
    )
except ImportError:
    from .yandex_constants import (
        INTEGRAL_GUARANTEES_DOCUMENTATION_URL,
        JOB_STATISTICS_DOCUMENTATION_URL,
        RESOURCE_REQUEST_DESCRIPTION,
        SCHEDULER_DASHBOARD_DEFAULT_CLUSTER,
        SCHEDULER_DASHBOARD_DEFAULT_POOL,
        SCHEDULER_DASHBOARD_DEFAULT_TREE,
    )

from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, MultiSensor, Text, Title
from yt_dashboard_generator.specific_tags.tags import TemplateTag

##################################################################

def _build_quotas_usage(d, os_documentation):
    def build_resource_sensor(resource):
        resource_in_config = resource if resource != "user_memory" else "memory"
        return MultiSensor(
            SchedulerPools(f"yt.scheduler.pools.resource_usage.{resource}")
                .nan_as_zero()
                .legend_format("Usage"),
            SchedulerPools(f"yt.scheduler.pools.resource_demand.{resource}")
                .nan_as_zero()
                .legend_format("Resource demand"),
            SchedulerPools(f"yt.scheduler.pools.effective_strong_guarantee_resources.{resource}")
                .nan_as_zero()
                .legend_format("Effective guarantee"),
            SchedulerPools(f"yt.scheduler.pools.specified_strong_guarantee_resources.{resource_in_config}")
                .nan_as_zero()
                .legend_format("Configured guarantee"),
            SchedulerPools(f"yt.scheduler.pools.specified_resource_limits.{resource_in_config}")
                .legend_format("Configured limit"),
        )

    d.add(Rowset().row(height=2).cell("", Title("Resources", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .row()
            .stack(False)
            .min(0)
            .cell("CPU", build_resource_sensor("cpu"), yaxis_label="Cores", display_legend=True)
            .cell("Memory", build_resource_sensor("user_memory").unit("UNIT_BYTES_SI"), yaxis_label="Bytes", display_legend=True)
            .cell("GPU", build_resource_sensor("gpu"), yaxis_label="GPUs", display_legend=True)
    )

    def build_operation_count_sensor(descriptions):
        return MultiSensor(*(
            SchedulerPools(f"yt.scheduler.pools.{name}_operation_count")
                .legend_format(f"{legend}")
            for legend, name in descriptions.items()
        ))

    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell(
                "Running Operations",
                build_operation_count_sensor({
                    "Usage": "running",
                    "Lightweight usage": "lightweight_running",
                    "Limit": "max_running",
                }),
                yaxis_label="Operations",
                display_legend=True,
            )
            .cell(
                "Total Operations",
                build_operation_count_sensor({
                    "Usage": "total",
                    "Limit": "max",
                }),
                yaxis_label="Operations",
                display_legend=True,
            )
    )

    DESCRIPTION = """
**Demand**: amount of resources needed to run all waiting and running jobs in the pool.<EOLN>
**Usage**: model resource consumption of the pool from scheduler's point of view. Represents the number of resources that are reserved for operations in the pool. Actual resource consumption may differ.<EOLN>
**Configured Guarantee**: amount of resources guaranteed for the pool that is specified in pool's config.<EOLN>
**Effective Guarantee**: amount of resources guaranteed for the pool that is derived from **Configured Guarantee**. In case of cluster resources shortage, **Effective Guarantee** can be lower than **Configured Guarantee**.<EOLN>
**Configured Limit**: maximum amount of resources given to the pool that is specified in pool's config.

**Running Operations**: number of operations in the pool which are running, i.e. are considered for job scheduling. Also displays the number of "lightweight" running operations, which are not accounted in the running operation count limit.<EOLN>
**Total Operations**: total number of operations in the pool, includes "Running operation count". The difference between Total Operations and Running Operations is the size of the queue of operations that are waiting to be started.

"""
    height = 8

    if not os_documentation:
        DESCRIPTION += RESOURCE_REQUEST_DESCRIPTION
        height += 1

    d.add(Rowset().row(height=height).cell("", Text(DESCRIPTION)))


def _build_cluster_share(d, os_documentation):
    d.add(Rowset().row(height=2).cell("", Title("Detailed Resource Distribution", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell(
                "Dominant Shares",
                MultiSensor(
                    SchedulerPools("yt.scheduler.pools.dominant_usage_share")
                        .legend_format("Dominant usage share"),
                    SchedulerPools("yt.scheduler.pools.dominant_demand_share")
                        .legend_format("Dominant demand share"),
                    SchedulerPools("yt.scheduler.pools.dominant_fair_share.total")
                        .legend_format("Dominant fair share"),
                ),
                yaxis_label="Cluster share",
                display_legend=True,
            )
            .cell(
                "Fair Share per Resource",
                MultiSensor(
                    SchedulerPools("yt.scheduler.pools.fair_share.total.cpu")
                        .legend_format("CPU"),
                    SchedulerPools("yt.scheduler.pools.fair_share.total.memory")
                        .legend_format("Memory"),
                    SchedulerPools("yt.scheduler.pools.fair_share.total.gpu")
                        .legend_format("GPU"),
                ),
                yaxis_label="Cluster share",
                display_legend=True,
            )
            .cell(
                "Dominant Fair Share per Type",
                MultiSensor(
                    SchedulerPools("yt.scheduler.pools.dominant_fair_share.strong_guarantee")
                        .legend_format("Strong guarantee"),
                    SchedulerPools("yt.scheduler.pools.dominant_fair_share.integral_guarantee")
                        .legend_format("Integral guarantee"),
                    SchedulerPools("yt.scheduler.pools.dominant_fair_share.weight_proportional")
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

    d.add(Rowset().row(height=3).cell("", Text(DESCRIPTION)))


def _build_job_metrics(d, os_documentation):
    d.add(Rowset().row(height=2).cell("", Title("Job Metrics", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell("Local Disk IO",
                SchedulerPools("yt.scheduler.pools.metrics.user_job_io_total.rate")
                    .legend_format("User job IO rate")
                    .unit("UNIT_IO_OPERATIONS_PER_SECOND"),
                yaxis_label="IO operations/sec",
            )
            .cell("CPU", MultiSensor(
                SchedulerPools("yt.scheduler.pools.metrics.user_job_cpu_system.rate")
                    .legend_format("User job system"),
                SchedulerPools("yt.scheduler.pools.metrics.user_job_cpu_user.rate")
                    .legend_format("User job user"),
                SchedulerPools("yt.scheduler.pools.metrics.job_proxy_cpu_system.rate")
                    .legend_format("Job proxy system"),
                SchedulerPools("yt.scheduler.pools.metrics.job_proxy_cpu_user.rate")
                    .legend_format("Job proxy user"),
            )
                .stack(True),
                yaxis_label="Cores")
            .cell("Memory", MultiSensor(
                SchedulerPools("yt.scheduler.pools.metrics.user_job_memory_mb.rate")
                    .legend_format("User job memory"),
                SchedulerPools("yt.scheduler.pools.metrics.job_proxy_memory_mb.rate")
                    .legend_format("Job proxy memory"),
            )
                .unit("UNIT_MEBIBYTES")
                .stack(True))
            # TODO(eshcherbin): Add expressions and divide first two sensors by 1000.
            .cell("GPU", MultiSensor(
                SchedulerPools("yt.scheduler.pools.metrics.gpu_load.rate")
                    .query_transformation("{query} / 1000.0")
                    .legend_format("Load"),
                SchedulerPools("yt.scheduler.pools.metrics.gpu_utilization_gpu.rate")
                    .query_transformation("{query} / 1000.0")
                    .legend_format("Utilization"),
                SchedulerPools("yt.scheduler.pools.resource_usage.gpu")
                    .legend_format("Usage"),
            ),
                yaxis_label="Sum of device fractions")
    )

    DESCRIPTION = """
**Local Disk IO**: total amount of I/O operations per second (IOPS) of all user jobs in the pool.<EOLN>
**CPU**: total detailed CPU usage statistics of all user jobs and their job proxies in the pool.<EOLN>
**Memory**: total detailed RAM usage of user job and job proxy.<EOLN>
**GPU**: total detailed GPU utilization statistics of all user jobs in the pool.

Consult [documentation]({}) for more information on the corresponding job statistics.
""".format(
        "https://ytsaurus.tech/docs/en/user-guide/problems/jobstatistics"
        if os_documentation
        else JOB_STATISTICS_DOCUMENTATION_URL)

    d.add(Rowset().row(height=5).cell("", Text(DESCRIPTION.strip())))


def _build_integral_guarantees(d, os_documentation):
    d.add(Rowset().row(height=2).cell("", Title("Integral guarantees", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .nan_as_zero()
        .stack(False)
        .min(0)
        .row()
            .cell("Accumulated Resource Volume: CPU",
                SchedulerPools("yt.scheduler.pools.accumulated_resource_volume.cpu")
                    .legend_format("Accumulated CPU volume"),
                yaxis_label="CPU * secs",
                display_legend=False,
            )
            .cell("Accumulated Resource Volume: Memory",
                SchedulerPools("yt.scheduler.pools.accumulated_resource_volume.user_memory")
                    .legend_format("Accumulated memory volume"),
                yaxis_label="Bytes * secs",
                display_legend=False,
            )
            .cell("Accumulated resource volume share",
                SchedulerPools("yt.scheduler.pools.accumulated_volume_dominant_share")
                    .legend_format("Accumulated dominant resource share"),
                yaxis_label="Cluster share * secs",
                display_legend=False,
            )
        .row()
            .cell("Accepted Free Volume: CPU",
                SchedulerPools("yt.scheduler.pools.accepted_free_volume.cpu")
                    .legend_format("Accepted free CPU volume"),
                yaxis_label="CPU * secs",
                display_legend=False,
            )
            .cell("Accepted Free Volume: Memory",
                SchedulerPools("yt.scheduler.pools.accepted_free_volume.user_memory")
                    .legend_format("Accepted free memory volume"),
                yaxis_label="Memory * secs",
                display_legend=False,
            )
    )

    DESCRIPTION = """
The following metrics are only relevant for pools with burst or relaxed integral guarantees.

**Accumulated Resource Volume Share**: amount of virtual resources accumulated in the pool which can be spent on integral guarantees.<EOLN>
**Accumulated Resource Volume (CPU/Memory)**: approximate amount of accumulated resources computed based on accumulated volume share and the current total resource amount in the cluster.<EOLN>
**Accepted Free Volume (CPU/Memory)**: virtual resource volume which was obtained by the pool via redistribution of other pools' unused integral resource flows.

Consult [documentation]({}) for more information on intergral guarantees.
""".format(
        "https://ytsaurus.tech/docs/en/user-guide/data-processing/scheduler/integral-guarantees"
        if os_documentation
        else INTEGRAL_GUARANTEES_DOCUMENTATION_URL)

    d.add(Rowset().row(height=6).cell("", Text(DESCRIPTION.strip())))


def build_scheduler_pool(has_porto=True, os_documentation=False):
    d = Dashboard()
    d.set_title("Scheduler pool [Autogenerated]")

    _build_quotas_usage(d, os_documentation)
    _build_cluster_share(d, os_documentation)
    if has_porto:
        _build_job_metrics(d, os_documentation)
    _build_integral_guarantees(d, os_documentation)

    d.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", SCHEDULER_DASHBOARD_DEFAULT_CLUSTER),
        backends=["monitoring"],
    )
    d.add_parameter(
        "cluster",
        "Cluster",
        GrafanaTextboxDashboardParameter(SCHEDULER_DASHBOARD_DEFAULT_CLUSTER),
        backends=["grafana"],
    )

    d.add_parameter(
        "tree",
        "Tree",
        MonitoringLabelDashboardParameter("yt", "tree", SCHEDULER_DASHBOARD_DEFAULT_TREE),
        backends=["monitoring"],
    )
    d.add_parameter(
        "tree",
        "Tree",
        GrafanaTextboxDashboardParameter(SCHEDULER_DASHBOARD_DEFAULT_TREE),
        backends=["grafana"],
    )
    d.value("tree", TemplateTag("tree"))

    d.add_parameter(
        "pool",
        "Pool",
        MonitoringLabelDashboardParameter("yt", "pool", SCHEDULER_DASHBOARD_DEFAULT_POOL),
        backends=["monitoring"],
    )
    d.add_parameter(
        "pool",
        "Pool",
        GrafanaTextboxDashboardParameter(SCHEDULER_DASHBOARD_DEFAULT_POOL),
        backends=["grafana"],
    )
    d.add_permission(
        "use",
        "//sys/pool_trees/$tree/$pool",
        "$cluster",
        ["//sys/pool_trees/$tree/<Root>"],
    )
    d.add_permission(
        "use",
        "//sys/pool_trees/$tree",
    )

    d.value("pool", TemplateTag("pool"))

    d.set_monitoring_serializer_options(dict(default_row_height=9))

    return d
