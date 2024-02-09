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

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor
from yt_dashboard_generator.taggable import SystemFields, NotEquals, ContainerTemplate, SensorTemplate

##################################################################


def _build_process_resources(d):
    def scheduler_thread_cpu(thread):
        return SchedulerCpu("yt.resource_tracker.user_cpu").value("thread", thread)
    def ca_thread_cpu(thread):
        return CACpu("yt.resource_tracker.user_cpu").value("thread", thread)
    d.add(Rowset()
        .stack(False)
        .all(yt_host)
        .row()
            .cell("Scheduler Memory", SchedulerMemory("yt.memory.generic.bytes_in_use_by_app").container_legend_format())
            .cell("Controller Agent Memory", CAMemory("yt.memory.generic.bytes_in_use_by_app").container_legend_format())
        .row()
            .cell("Scheduler CPU Wait", SchedulerCpu("yt.resource_tracker.cpu_wait").aggr("thread"))
            .cell("Controller Agent CPU Wait", CACpu("yt.resource_tracker.cpu_wait").aggr("thread"))
        .row()
            .cell("Scheduler Control CPU", scheduler_thread_cpu("Control").container_legend_format())
            .cell("Controller Agent Control CPU", ca_thread_cpu("Control").container_legend_format())
        .row()
            .cell("Scheduler Node Shards CPU", scheduler_thread_cpu("NodeShard").container_legend_format())
            .cell("Controller Agent Controllers CPU", ca_thread_cpu("Controller").container_legend_format())
        .row()
            .cell("Scheduler Thread Pools CPU", scheduler_thread_cpu("FSUpdatePool|OrchidWorker|Background")
                .aggr(yt_host)
                .legend_format("{{thread}}"))
            .cell("Controller Agent JobTracker CPU", ca_thread_cpu("JobTracker").container_legend_format())
        .row()
            .cell("Scheduler RPC Heavy CPU", scheduler_thread_cpu("RpcHeavy").container_legend_format())
            .cell("Controller Agent RPC Heavy CPU", ca_thread_cpu("RpcHeavy").container_legend_format())
        .row()
            .cell("Control Thread Buckets", SchedulerCpu("yt.action_queue.time.cumulative.rate")
                .value("thread", "Control")
                .all("bucket")
                .aggr(yt_host)
                .legend_format("{{bucket}}"))
    )


def _build_scheduling_statistics(d):
    d.add(Rowset()
        .stack(False)
        .row()
            .min(0)
            .cell("Operation Count", SchedulerPools("yt.scheduler.pools.running_operation_count|yt.scheduler.pools.total_operation_count")
                .value("tree", "physical")
                .value("pool", "<Root>")
                .sensor_legend_format())
            .cell("Schedulable Element Count", SchedulerPools("yt.scheduler.pools.schedulable_element_count")
                .all("tree")
                .value("pool", "<Root>")
                .legend_format("{{tree}}"))
        .row()
            .stack(True)
            .aggr("schedule_jobs_stage", "scheduling_stage")
            .all("tree")
            .legend_format("{{tree}}")
            .cell("Preschedule job count rate", Scheduler("yt.scheduler.preschedule_job_count.rate"))
            .cell("Schedule job attempts", Scheduler("yt.scheduler.schedule_job_attempt_count.rate"))
        .row()
            .cell("Preemptive preschedule job attempts", Scheduler("yt.scheduler.preschedule_job_count.rate")
                .stack(True)
                .value("tree", "physical")
                .value("scheduling_stage", NotEquals("non_preemptive|regular*"))
                .legend_format("{{scheduling_stage}}"))
            .cell("Operation satisfaction distribution", SchedulerPools("yt.scheduler.pools.operation_satisfaction_distribution")
                .value("tree", "physical")
                .value("pool", "<Root>")
                .all("quantile")
                .legend_format("{{quantile}}"))
    )


def _build_logging(d):
    d.add(Rowset()
        .stack(False)
        .all(yt_host)
        .row()
            .cell("Scheduler Logging", SchedulerInternal("yt.logging.enqueued_events.rate|yt.logging.dropped_events.rate")
                .legend_format("{}, {}".format(ContainerTemplate, SensorTemplate)))
            .cell("Controller Agent Logging", CAInternal("yt.logging.enqueued_events.rate")
                .container_legend_format())
    )


def _build_network(d):
    rowset = Rowset().stack(False)
    for sensor, sensor_name_suffix in (("RxBytes", "RX"), ("TxBytes", "TX")):
        row = rowset.row()
        for service, service_name in (("scheduler", "Scheduler"), ("controller_agent", "Controller Agent")):
            row.cell(
                f"{service_name} Network {sensor_name_suffix}",
                Sensor(f"/Porto/Containers/Ifs/{sensor}", sensor_tag_name="path")
                    .value("service", "porto_iss")
                    .value("intf", "veth")
                    .value("host", "*-*")
                    .value("name", f"yt_{{{{cluster}}}}_{service}s")
            )
    d.add(rowset)


def _build_aborted_job_statistics(d):
    d.add(Rowset()
        .stack(False)
        .row()
            .cell("Aborted jobs rate", CA("yt.controller_agent.jobs.aborted_job_count.rate")
                .stack(True)
                .aggr(yt_host)
                .aggr("job_type", "tree")
                .value("abort_reason", "!preemption")
                .legend_format("{{abort_reason}}"))
            .cell("Aborted vs completed job time", SchedulerPools("yt.scheduler.pools.metrics.total_time_aborted.rate|yt.scheduler.pools.metrics.total_time_completed.rate")
                .stack(True)
                .value("tree", "physical")
                .value("pool", "<Root>")
                .sensor_legend_format())
    )


def _build_scheduler_rpc_statistics(d):
    scheduler_rate = SchedulerRpc("yt.rpc.server.request_count.rate")
    scheduler_queue_size = SchedulerRpc("yt.rpc.server.request_queue_size")
    scheduler_time = SchedulerRpc("yt.rpc.server.request_time.total.max")
    scheduler_concurrency = SchedulerRpc("yt.rpc.server.concurrency*")

    d.add(Rowset()
        .stack(False)
        .aggr("user")
        .row()
            .all(yt_host)
            .min(0)
            .container_legend_format()
            .cell("AllocationTrackerService: Heartbeat rate", scheduler_rate.service_method("JobTrackerService|AllocationTrackerService", "Heartbeat"))
            .cell("AllocationTrackerService: Heartbeat queue size", scheduler_queue_size.service_method("JobTrackerService|AllocationTrackerService", "Heartbeat"))
        .row()
            .all(yt_host)
            .min(0)
            .legend_format("{}, {}".format(ContainerTemplate, SensorTemplate))
            .cell("AllocationTrackerService: Heartbeat time max", scheduler_time.service_method("JobTrackerService|AllocationTrackerService", "Heartbeat"))
            .cell("AllocationTrackerService: Heartbeat concurrency", scheduler_concurrency.service_method("JobTrackerService|AllocationTrackerService", "Heartbeat"))
        .row()
            .aggr(yt_host)
            .min(0)
            .cell("Scheduler heartbeat throttling", Scheduler("yt.scheduler.node_heartbeat.concurrent_limit_reached_count.rate|yt.scheduler.node_heartbeat.concurrent_complexity_limit_reached_count.rate")
                .stack(False)
                .aggr("limit_type")
                .value("host", SystemFields.Aggr))
        .row()
            .all(yt_host)
            .min(0)
            .legend_format("{{method}}")
            .cell(
                "ControllerAgentTrackerService: Heartbeat rate",
                scheduler_rate.service_method("ControllerAgentTrackerService", "*Heartbeat"))
            .cell(
                "ControllerAgentTrackerService: Heartbeat time max",
                scheduler_time.service_method("ControllerAgentTrackerService", "*Heartbeat"))
    )



def _build_controller_agent_rpc_statistics(d):
    ca_rate = CARpc("yt.rpc.server.request_count.rate")
    ca_queue_size = CARpc("yt.rpc.server.request_queue_size")
    ca_time_max = CARpc("yt.rpc.server.request_time.total.max")

    d.add(Rowset()
        .stack(False)
        .aggr("user")
        .all(yt_host)
        .container_legend_format()
        .row()
            .cell("JobTrackerService: Heartbeat rate", ca_rate.service_method("JobTrackerService", "Heartbeat"))
            .cell("JobSpecService: GetJobSpecs rate", ca_rate.service_method("JobSpecService", "GetJobSpecs"))
        .row()
            .cell("JobTrackerService: Heartbeat time max", ca_time_max.service_method("JobTrackerService", "Heartbeat"))
            .cell("JobSpecService: GetJobSpecs time max", ca_time_max.service_method("JobSpecService", "GetJobSpecs"))
        .row()
            .cell("JobTrackerService: Heartbeat queue size", ca_queue_size.service_method("JobTrackerService", "Heartbeat"))
            .cell("JobSpecService: GetJobSpecs queue size", ca_queue_size.service_method("JobSpecService", "GetJobSpecs"))
    )


def build_scheduler_internal(has_porto):
    d = Dashboard()
    _build_process_resources(d)
    _build_scheduling_statistics(d)
    _build_logging(d)
    if has_porto:
        _build_network(d)
    _build_aborted_job_statistics(d)
    _build_scheduler_rpc_statistics(d)
    _build_controller_agent_rpc_statistics(d)

    d.set_monitoring_serializer_options(dict(default_row_height=8))

    return d
