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
        RESOURCE_REQUEST_DESCRIPTION,
        JOB_STATISTICS_DOCUMENTATION_URL,
        INTEGRAL_GUARANTEES_DOCUMENTATION_URL
    )
except ImportError:
    RESOURCE_REQUEST_DESCRIPTION = ""
    JOB_STATISTICS_DOCUMENTATION_URL = ""
    INTEGRAL_GUARANTEES_DOCUMENTATION_URL = ""


from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, MultiSensor, Text, Title
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr

##################################################################

def _build_quotas_usage(d, os_documentation):
    def resource_sensors(resource):
        return MultiSensor(
            SchedulerPools(f"yt.scheduler.pools.resource_usage.{resource}")
                .legend_format("usage"),
            SchedulerPools(f"yt.scheduler.pools.resource_demand.{resource}")
                .legend_format("demand"),
            SchedulerPools(f"yt.scheduler.pools.effective_strong_guarantee_resources.{resource}")
                .legend_format("strong_guarantee"),
            SchedulerPools(f"yt.scheduler.pools.specified_resource_limits.{resource}")
                .legend_format("specified_limit"),
        )

    d.add(Rowset().row(height=2).cell("", Title("Usage, demand and strong guarantee")))
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell("CPU: usage, demand, guarantee", resource_sensors("cpu"), yaxis_label="CPU, cores", display_legend=True)
            .cell("RAM: usage, demand, guarantee", resource_sensors("user_memory"), yaxis_label="Memory, bytes", display_legend=True)
            .cell("GPU: usage, demand, guarantee", resource_sensors("gpu"), yaxis_label="GPU, cards", display_legend=True)
    )

    def operation_count_sensors(descriptions):
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
                "Running operation count",
                operation_count_sensors({
                    "usage": "running",
                    "lightweight usage": "lightweight_running",
                    "limit": "max_running",
                }),
                yaxis_label="Operation count",
                display_legend=True,
            )
            .cell(
                "Total operation count",
                operation_count_sensors({
                    "usage": "total",
                    "limit": "max",
                }),
                yaxis_label="Operation count",
                display_legend=True,
            )
    )

    DESCRIPTION = """
**Demand**: amount of resources needed to run all waiting and running jobs in the pool.<EOLN>
**Usage**: model resource consumption of the pool from scheduler's point of view. Represents the number of resources that are reserved for operations in the pool. Actual resource consumption may differ.<EOLN>
**Strong guarantee**: amount of resources that is guaranteed for operations in the pool at any moment.<EOLN>
**Total operation count**: total number of operations in the pool, includes "Running operation count". The difference between "Total operation count" and "Running operation count" is the size of the queue of operations that are waiting to be started.
"""
    height = 5

    if not os_documentation:
        DESCRIPTION += RESOURCE_REQUEST_DESCRIPTION
        height += 1

    d.add(Rowset().row(height=height).cell("", Text(DESCRIPTION)))


def _build_cluster_share(d, os_documentation):
    d.add(Rowset().row(height=2).cell("", Title("Cluster share")))
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell(
                "Resource shares: usage, demand, fair share",
                MultiSensor(
                    SchedulerPools("yt.scheduler.pools.dominant_usage_share")
                        .legend_format("dominant usage share"),
                    SchedulerPools("yt.scheduler.pools.dominant_demand_share")
                        .legend_format("dominant demand share"),
                    SchedulerPools("yt.scheduler.pools.dominant_fair_share.total")
                        .legend_format("dominant fair share"),
                ),
                display_legend=True,
            )
            .cell(
                "Fair share: CPU, RAM, GPU components",
                MultiSensor(
                    SchedulerPools("yt.scheduler.pools.fair_share.total.cpu")
                        .legend_format("CPU"),
                    SchedulerPools("yt.scheduler.pools.fair_share.total.memory")
                        .legend_format("memory"),
                    SchedulerPools("yt.scheduler.pools.fair_share.total.gpu")
                        .legend_format("GPU"),
                ),
                display_legend=True,
            )
            .cell(
                "Dominant fair share: strong, integral, weight proportional",
                MultiSensor(
                    SchedulerPools("yt.scheduler.pools.dominant_fair_share.integral_guarantee")
                        .legend_format("integral guarantee"),
                    SchedulerPools("yt.scheduler.pools.dominant_fair_share.strong_guarantee")
                        .legend_format("strong guarantee"),
                    SchedulerPools("yt.scheduler.pools.dominant_fair_share.weight_proportional")
                        .legend_format("weight proportional"),
                ).stack(True),
                display_legend=True,
            )
        )

    DESCRIPTION = """
**Usage**, **Demand**, **Fair share**: share of the cluster that is used, demanded or should be given to the pool. For example, 1.0 or 100% corresponds to the total amount of resources in the cluster.<EOLN>
**CPU. RAM, GPU fair share components**  help to determine pool's dominant resource.

In addition, fair share can be split into three parts: strong guarantee, integral guarantee and free resources, which are distributed between pools in proportion to their weights.
"""

    d.add(Rowset().row(height=5).cell("", Text(DESCRIPTION)))


def _build_job_metrics(d, os_documentation):
    d.add(Rowset().row(height=2).cell("", Title("Job metrics")))
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell("Job metrics: local disk IO",
                SchedulerPools("yt.scheduler.pools.metrics.user_job_io_total.rate")
                    .legend_format("user job io rate"),
            )
            .cell("Job metrics: CPU usage", MultiSensor(
                SchedulerPools("yt.scheduler.pools.metrics.user_job_cpu_system.rate")
                    .legend_format("user job system cpu rate"),
                SchedulerPools("yt.scheduler.pools.metrics.user_job_cpu_user.rate")
                    .legend_format("user job user cpu rate"),
                SchedulerPools("yt.scheduler.pools.metrics.job_proxy_cpu_system.rate")
                    .legend_format("job proxy system cpu rate"),
                SchedulerPools("yt.scheduler.pools.metrics.job_proxy_cpu_user.rate")
                    .legend_format("job proxy user cpu rate"),
            )
                .stack(True))
            # TODO(eshcherbin): Add expressions and divide first two sensors by 1000.
            .cell("Job metrics: GPU usage", MultiSensor(
                SchedulerPools("yt.scheduler.pools.metrics.gpu_load.rate")
                    .query_transformation("{query} / 1000.0")
                    .legend_format("gpu load rate"),
                SchedulerPools("yt.scheduler.pools.metrics.gpu_utilization_gpu.rate")
                    .query_transformation("{query} / 1000.0")
                    .legend_format("gpu utilization rate"),
                SchedulerPools("yt.scheduler.pools.resource_usage.gpu")
                    .legend_format("pools gpu usage"),
            ))
    )

    DESCRIPTION = """
**Local disk I/O**: total amount of input/output operations per second (IOPS) of all user jobs in the pool.<EOLN>
**CPU usage**: total detailed CPU usage statistics of all user jobs and their job proxies in the pool.<EOLN>
**GPU usage**: total detailed GPU utilization statistics of all user jobs in the pool.

Consult [documentation]({}) for more information on the corresponding job statistics.
""".format(
        "https://ytsaurus.tech/docs/en/user-guide/problems/jobstatistics"
        if os_documentation
        else JOB_STATISTICS_DOCUMENTATION_URL)

    d.add(Rowset().row(height=4).cell("", Text(DESCRIPTION.strip())))


def _build_integral_guarantees(d, os_documentation):
    d.add(Rowset().row(height=2).cell("", Title("Integral guarantee")))
    d.add(Rowset()
        .nan_as_zero()
        .stack(False)
        .min(0)
        .row()
            .cell("Accumulated resource volume: CPU",
                SchedulerPools("yt.scheduler.pools.accumulated_resource_volume.cpu")
                    .legend_format("accumulated CPU"),
            )
            .cell("Accumulated resource volume: RAM",
                SchedulerPools("yt.scheduler.pools.accumulated_resource_volume.user_memory")
                    .legend_format("accumulated memory"),
            )
            .cell("Accumulated resource volume share",
                SchedulerPools("yt.scheduler.pools.accumulated_volume_dominant_share")
                    .legend_format("accumulated dominant resource share"),
            )
        .row()
            .cell("Accepted free volume: CPU",
                SchedulerPools("yt.scheduler.pools.accepted_free_volume.cpu")
                    .legend_format("accepted free CPU"),
            )
            .cell("Accepted free volume: RAM",
                SchedulerPools("yt.scheduler.pools.accepted_free_volume.user_memory")
                    .legend_format("accepted free memory"),
            )
    )

    DESCRIPTION = """
The following metrics are only relevant for pools with burst or relaxed integral guarantees.

**Accumulated resource volume share**: amount of virtual resources accumulated in the pool which can be spent on integral guarantees. In `cluster_share*sec`.<EOLN>
**Accumulated resource volume (CPU/RAM)**: approximate amount of accumulated resources computed based on accumulated volume share and the current total resource amount in the cluster. In `cpu*sec`/`bytes*sec`.<EOLN>
**Accepted free volume (CPU/RAM)**: virtual resource volume which was obtained by the pool via redistribution of other pools' unused integral resource flows.

Consult [documentation]({}) for more information on intergral guarantees.
""".format(
        "https://ytsaurus.tech/docs/en/user-guide/data-processing/scheduler/integral-guarantees"
        if os_documentation
        else INTEGRAL_GUARANTEES_DOCUMENTATION_URL)

    d.add(Rowset().row(height=6).cell("", Text(DESCRIPTION.strip())))


def build_scheduler_pool(has_porto=True, os_documentation=False):
    d = Dashboard()
    _build_quotas_usage(d, os_documentation)
    _build_cluster_share(d, os_documentation)
    if has_porto:
        _build_job_metrics(d, os_documentation)
    _build_integral_guarantees(d, os_documentation)

    d.value("tree", TemplateTag("tree"))
    d.value("pool", TemplateTag("pool"))

    d.set_monitoring_serializer_options(dict(default_row_height=9))

    return d
