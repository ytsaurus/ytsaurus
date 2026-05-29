# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

try:
    from .constants import (
        SCHEDULER_GPU_DASHBOARD_DEFAULT_CLUSTER,
        SCHEDULER_GPU_DASHBOARD_DEFAULT_TREE,
    )
except ImportError:
    from .yandex_constants import (
        SCHEDULER_GPU_DASHBOARD_DEFAULT_CLUSTER,
        SCHEDULER_GPU_DASHBOARD_DEFAULT_TREE,
    )

from .common.sensors import (
    Scheduler, SchedulerCpu, SchedulerMemory, SchedulerPools, GpuSchedulerMonitor,
    yt_host,
)

from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter
from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import MultiSensor, Title

##################################################################


def _build_process_resources(d):
    def scheduler_thread_cpu(thread):
        return SchedulerCpu("yt.resource_tracker.user_cpu").value("thread", thread)
    d.add(Rowset()
        .stack(False)
        .all(yt_host)
        .row()
            .cell("Scheduler Memory", SchedulerMemory("yt.memory.generic.bytes_in_use_by_app").container_legend_format())
            .cell("Scheduler Control CPU", scheduler_thread_cpu("Control").container_legend_format())
        .row()
            .cell("Scheduler Node Shards CPU", scheduler_thread_cpu("NodeShard").container_legend_format())
            .cell("Scheduler Thread Pools CPU", scheduler_thread_cpu("FSUpdatePool|OrchidWorker|Background")
                .aggr(yt_host)
                .legend_format("{{thread}}"))
        .row()
            .cell("Control Thread Buckets", SchedulerCpu("yt.action_queue.time.cumulative.rate")
                .value("thread", "Control")
                .all("bucket")
                .aggr("queue")
                .aggr(yt_host)
                .legend_format("{{bucket}}"))
            .cell("GpuAssignmentPlanUpdate Bucket", SchedulerCpu("yt.action_queue.time.cumulative.rate")
                .value("thread", "Control")
                .value("bucket", "GpuAssignmentPlanUpdate")
                .aggr("queue")
                .aggr(yt_host)
                .container_legend_format())
    )


def _gpu(sensor):
    return Scheduler(sensor).value("tree", SCHEDULER_GPU_DASHBOARD_DEFAULT_TREE)


def _fs_pool(sensor):
    return (SchedulerPools(sensor)
        .value("tree", SCHEDULER_GPU_DASHBOARD_DEFAULT_TREE)
        .value("pool", "<Root>"))


def _gpu_module(sensor):
    return (Scheduler(sensor)
        .value("tree", SCHEDULER_GPU_DASHBOARD_DEFAULT_TREE)
        .all("module")
        .legend_format("{{module}}"))


def _build_gpu_scheduling_statistics(d):
    d.add(Rowset()
        .stack(False)
        .min(0)
        .row()
            .cell("Assignments", _gpu("yt.scheduler.gpu_policy.assignments_count"))
            .cell("Assigned GPU", _gpu("yt.scheduler.gpu_policy.assigned_gpu_count"))
        .row()
            .cell("Enabled operations", _gpu("yt.scheduler.gpu_policy.enabled_operations_count"))
            .cell("Full-host module-bound operations", _gpu("yt.scheduler.gpu_policy.full_host_module_bound_operations_count"))
        .row()
            .stack(True)
            .cell("Planned assignments rate", _gpu("yt.scheduler.gpu_policy.planned_assignments_count.rate"))
            .cell("Preempted assignments rate", _gpu("yt.scheduler.gpu_policy.preempted_assignments_count.rate"))
        .row()
            .cell("Total planning time", _gpu("yt.scheduler.gpu_policy.total_planning_time.avg"))
            .cell("Planning stages",
                MultiSensor(
                    Scheduler("yt.scheduler.gpu_policy.operation_resources_update_time.avg").legend_format("operation_resources_update"),
                    Scheduler("yt.scheduler.gpu_policy.full_host_planning_time.avg").legend_format("full_host"),
                    Scheduler("yt.scheduler.gpu_policy.regular_planning_time.avg").legend_format("regular"),
                    Scheduler("yt.scheduler.gpu_policy.extra_planning_time.avg").legend_format("extra"),
                )
                .stack(True)
                .value("tree", SCHEDULER_GPU_DASHBOARD_DEFAULT_TREE))
        .row()
            .cell("Total nodes per module", _gpu_module("yt.scheduler.gpu_policy.module.total_nodes_count"))
            .cell("Unreserved nodes per module", _gpu_module("yt.scheduler.gpu_policy.module.unreserved_nodes_count"))
        .row()
            .cell("Full-host module-bound ops per module", _gpu_module("yt.scheduler.gpu_policy.module.full_host_module_bound_operations_count"))
    )


def _build_fs_vs_gpu_comparison(d):
    def gpu_node_resource(resource):
        return (GpuSchedulerMonitor(f"gpu_scheduler.nodes.assigned_resource_usage.{resource}")
            .value("pool_tree", SCHEDULER_GPU_DASHBOARD_DEFAULT_TREE)
            .query_transformation("series_sum({query})"))

    d.add(Rowset()
        .stack(False)
        .min(0)
        .row()
            .cell("AssignedVsUsage",
                MultiSensor(
                    _gpu("yt.scheduler.gpu_policy.assignments_count").legend_format("gpu_policy.assignments_count"),
                    _fs_pool("yt.scheduler.pools.resource_usage.user_slots").legend_format("pools.resource_usage.user_slots"),
                ))
            .cell("Operations",
                MultiSensor(
                    _gpu("yt.scheduler.gpu_policy.enabled_operations_count").legend_format("gpu_policy.enabled_operations_count"),
                    _fs_pool("yt.scheduler.pools.total_operation_count").legend_format("pools.total_operation_count"),
                ))
        .row()
            .cell("PlannedVsPreempted",
                MultiSensor(
                    _gpu("yt.scheduler.gpu_policy.planned_assignments_count.rate").legend_format("planned"),
                    _gpu("yt.scheduler.gpu_policy.preempted_assignments_count.rate").legend_format("preempted"),
                ))
            .cell("PlanningTime", _gpu("yt.scheduler.gpu_policy.total_planning_time.avg"))
        .row()
            .cell("AssignedCpu",
                MultiSensor(
                    gpu_node_resource("cpu").legend_format("gpu_scheduler.nodes.cpu"),
                    _fs_pool("yt.scheduler.pools.resource_usage.cpu").legend_format("pools.resource_usage.cpu"),
                ))
            .cell("AssignedGpu",
                MultiSensor(
                    gpu_node_resource("gpu").legend_format("gpu_scheduler.nodes.gpu"),
                    _fs_pool("yt.scheduler.pools.resource_usage.gpu").legend_format("pools.resource_usage.gpu"),
                ))
        .row()
            .cell("AssignedMemory",
                MultiSensor(
                    gpu_node_resource("user_memory").legend_format("gpu_scheduler.nodes.user_memory"),
                    _fs_pool("yt.scheduler.pools.resource_usage.user_memory").legend_format("pools.resource_usage.user_memory"),
                ))
            .cell("AssignedNetwork",
                MultiSensor(
                    gpu_node_resource("network").legend_format("gpu_scheduler.nodes.network"),
                    _fs_pool("yt.scheduler.pools.resource_usage.network").legend_format("pools.resource_usage.network"),
                ))
    )


def build_scheduler_gpu(has_porto, backend):
    d = Dashboard()
    d.set_title("Scheduler GPU policy [Autogenerated]")

    d.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", SCHEDULER_GPU_DASHBOARD_DEFAULT_CLUSTER),
        backends=["monitoring"],
    )

    d.add_parameter(
        "cluster",
        "Cluster",
        GrafanaTextboxDashboardParameter(SCHEDULER_GPU_DASHBOARD_DEFAULT_CLUSTER),
        backends=["grafana"],
    )

    d.add_parameter(
        "tree",
        "Pool tree",
        MonitoringLabelDashboardParameter("yt", "pool_tree", SCHEDULER_GPU_DASHBOARD_DEFAULT_TREE),
        backends=["monitoring"],
    )

    d.add_parameter(
        "tree",
        "Pool tree",
        GrafanaTextboxDashboardParameter(SCHEDULER_GPU_DASHBOARD_DEFAULT_TREE),
        backends=["grafana"],
    )

    d.add(Rowset().row(height=2).cell("", Title("Process Resources", size="TITLE_SIZE_L")))
    _build_process_resources(d)

    d.add(Rowset().row(height=2).cell("", Title("GPU Scheduling Statistics", size="TITLE_SIZE_L")))
    _build_gpu_scheduling_statistics(d)

    d.add(Rowset().row(height=2).cell("", Title("FS vs GPU Dry-Run Comparison", size="TITLE_SIZE_L")))
    _build_fs_vs_gpu_comparison(d)

    d.set_monitoring_serializer_options(dict(default_row_height=8))

    return d
