# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import *

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr, PlainMonitoringExpr
from yt_dashboard_generator.backends.monitoring import MonitoringTextDashboardParameter
from yt_dashboard_generator.sensor import MultiSensor


def build_versions():
    spec_version_change_query_transformation = "sign(derivative({query}))"
    return (Rowset()
        .stack(True)
        .row()
            .cell("Controller versions", FlowController("yt.build.version").aggr("host"))
            .cell("Worker versions", FlowWorker("yt.build.version").aggr("host"))
            .cell("Specs version change", MultiSensor(
                MonitoringExpr(FlowController("yt.flow.controller.spec_version"))
                    .query_transformation(spec_version_change_query_transformation)
                    .alias("Spec change"),
                MonitoringExpr(FlowController("yt.flow.controller.dynamic_spec_version"))
                    .query_transformation(spec_version_change_query_transformation)
                    .alias("Dynamic spec change")))
    ).owner

def build_resource_usage():
    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell("Total VCPU (Controller)", FlowController("yt.resource_tracker.total_vcpu")
                .aggr("thread")
                .unit("UNIT_PERCENT"))
            .cell(
                "Busiest thread pool (Controller)",
                MonitoringExpr(FlowController("yt.resource_tracker.utilization"))
                    .unit("UNIT_PERCENT_UNIT")
                    .all("thread")
                    .group_by_labels("host", "v -> group_lines(\"sum\", top_avg(1, v))")
                    .alias("{{thread}} - {{host}}")
                    .top(10))
            .cell("Memory (Controller)", FlowController("yt.resource_tracker.memory_usage.rss").unit("UNIT_BYTES_SI"))
        .row()
            .cell("Total VCPU (Worker)", FlowWorker("yt.resource_tracker.total_vcpu")
                .aggr("thread")
                .unit("UNIT_PERCENT"))
            .cell(
                "Busiest thread pool (Worker)",
                MonitoringExpr(FlowWorker("yt.resource_tracker.utilization"))
                    .unit("UNIT_PERCENT_UNIT")
                    .all("thread")
                    .group_by_labels("host", "v -> group_lines(\"sum\", top_avg(1, v))")
                    .alias("{{thread}} - {{host}}")
                    .top(50))
            .cell("Memory (Worker)", FlowWorker("yt.resource_tracker.memory_usage.rss").unit("UNIT_BYTES_SI"))
    ).owner

def build_flow_layout():
    return (Rowset()
        .stack(False)
        .row()
            .cell("Registered workers count", FlowController("yt.flow.controller.worker_count").unit("UNIT_COUNT"))
            .cell("Computations count", FlowController("yt.flow.controller.computation_count").unit("UNIT_COUNT"))
            .cell("Partitions count", FlowController("yt.flow.controller.partition_count").unit("UNIT_COUNT"))
    ).owner

def build_flow_layout_mutations():
    return (Rowset()
        .stack(False)
        .row()
            .cell("Job statuses", FlowController("yt.flow.controller.job_status.*").unit("UNIT_COUNT"))
            .cell("Layout mutations", FlowController("yt.flow.controller.mutations.*.rate").unit("UNIT_COUNTS_PER_SECOND"))
            .cell("Job manage mutations", FlowController("yt.flow.controller.job_manager.*.rate").unit("UNIT_COUNTS_PER_SECOND"))
    ).owner


def build_streams():
    return (Rowset()
        .stack(False)
        .all("stream_id")
        .row()
            .cell(
                "Stream Time Lag",
                MonitoringExpr(FlowController("yt.flow.controller.streams.time_lag"))
                    .alias("{{stream_id}}")
                    .unit("UNIT_SECONDS")
                    .precision(1))
            .cell(
                "User Watermark Lag",
                MonitoringExpr(FlowController("yt.flow.controller.streams.user_time_lag"))
                    .all("user_timestamp_id")
                    .alias("{{stream_id}} - {{user_timestamp_id}}")
                    .unit("UNIT_SECONDS")
                    .precision(1))
        .row()
            .cell(
                "Stream count lags",
                MonitoringExpr(FlowController("yt.flow.controller.streams.count_lag"))
                    .alias("{{stream_id}}")
                    .unit("UNIT_COUNT"))
            .cell(
                "Stream size lags",
                MonitoringExpr(FlowController("yt.flow.controller.streams.byte_size_lag"))
                    .alias("{{stream_id}}")
                    .unit("UNIT_BYTES_SI"))
    )


def build_computations():
    return (Rowset()
        .stack(True)
        .aggr("host")
        .all("computation_id")
        .all("stream_id")
        .row()
            .cell(
                "Registered processed messages rate",
                MultiSensor(
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.input_streams.persisted_count.rate"))
                        .alias("input - {{computation_id}} - {{stream_id}}"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.source_streams.persisted_count.rate"))
                        .alias("source - {{computation_id}} - {{stream_id}}"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.timer_streams.unregistered_count.rate"))
                        .alias("timer - {{computation_id}} - {{stream_id}}")
                )
                    .unit("UNIT_COUNTS_PER_SECOND"))
            .cell(
                "Registered processed messages bytes rate",
                MultiSensor(
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.input_streams.persisted_bytes.rate"))
                        .alias("input - {{computation_id}} - {{stream_id}}"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.source_streams.persisted_bytes.rate"))
                        .alias("source - {{computation_id}} - {{stream_id}}"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.timer_streams.unregistered_bytes.rate"))
                        .alias("timer - {{computation_id}} - {{stream_id}}")
                )
                    .unit("UNIT_BYTES_SI_PER_SECOND"))
        .row()
            .cell(
                "Registered generated messages rate",
                MultiSensor(
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.output_streams.registered_count.rate"))
                        .alias("output - {{computation_id}} - {{stream_id}}"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.timer_streams.registered_count.rate"))
                        .alias("timer - {{computation_id}} - {{stream_id}}")
                )
                    .unit("UNIT_COUNTS_PER_SECOND"))
            .cell(
                "Registered generated messages bytes rate",
                MultiSensor(
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.output_streams.registered_bytes.rate"))
                        .alias("{{computation_id}}/{{stream_id}} - output"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.timer_streams.registered_bytes.rate"))
                        .alias("{{computation_id}}/{{stream_id}} - timer")
                )
                    .unit("UNIT_BYTES_SI_PER_SECOND"))
    )


def build_computation_resources():
    return (Rowset()
        .stack(True)
        .all("computation_id")
        .aggr("host")
        .row()
            .cell(
                "Computation cpu time",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.cpu_time.rate"))
                    .alias("{{computation_id}}")
                    .unit("UNIT_PERCENT_UNIT"))
            .cell(
                "Computation memory usage",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.memory_usage"))
                    .alias("{{computation_id}}")
                    .unit("UNIT_BYTES_SI"))
            .cell(
                "Input buffers size",
                MonitoringExpr(FlowWorker("yt.flow.worker.buffer_state.computations.input.size"))
                    .all("stream_id")
                    .alias("{{computation_id}} / {{stream_id}}")
                    .unit("UNIT_BYTES_SI"))
            .cell(
                "Output buffers size",
                MonitoringExpr(FlowWorker("yt.flow.worker.buffer_state.computations.output.size"))
                    .all("stream_id")
                    .alias("{{computation_id}} / {{stream_id}}")
                    .unit("UNIT_BYTES_SI"))
    )

def build_partition_store_commits():
    return (Rowset()
        .stack(False)
        .all("computation_id")
        .row()
            .cell(
                "Partition store commit rate",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.partition_store.commit_total.rate"))
                    .aggr("host")
                    .unit("UNIT_REQUESTS_PER_SECOND"))
            .cell(
                "Partition store failed commit rate",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.partition_store.commit_failed.rate"))
                    .aggr("host")
                    .unit("UNIT_REQUESTS_PER_SECOND"))
            .cell(
                "Partition store commit time",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.partition_store.commit_time.max"))
                    .all("host")
                    .alias("{{computation_id}} - {{host}}")
                    .top(50)
                    .unit("UNIT_SECONDS"))
            .cell(
                "Input messages lookup time",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.partition_store.input_messages.lookup_time.max"))
                    .all("host")
                    .alias("{{computation_id}} - {{host}}")
                    .top(50)
                    .unit("UNIT_SECONDS"))
    )

def build_heartbeats():
    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell(
                "Handshake/heartbeats requests",
                MultiSensor(
                    MonitoringExpr(FlowController("yt.rpc.server.request_count.rate").value("method", "Heartbeat")),
                    MonitoringExpr(FlowController("yt.rpc.server.request_count.rate").value("method", "Handshake"))
                )
                    .unit("UNIT_REQUESTS_PER_SECOND")
                    .aggr("host"),
            )
            .cell(
                "Worker heartbeat prepare time",
                FlowWorker("yt.flow.worker.controller_connector.heartbeat.prepare_request_time.max")
                    .unit("UNIT_SECONDS")
            )
            .cell(
                "Worker heartbeat wait response time",
                FlowWorker("yt.flow.worker.controller_connector.heartbeat.wait_response_time.max")
                    .unit("UNIT_SECONDS")
            )
            .cell(
                "Worker heartbeat process response time",
                FlowWorker("yt.flow.worker.controller_connector.heartbeat.process_response_time.max")
                    .unit("UNIT_SECONDS")
            )
    )

def build_epoch_timings():
    return (Rowset()
        .row()
            .cell(
                "Epoch parts time (Computation: {{computation_id}})",
                MonitoringExpr(
                    FlowWorker("yt.flow.worker.computation.epoch_parts_time.rate")
                        .value("computation_id", "{{computation_id}}")
                        .value("part", "!-"))
                    .aggr("host")
                    .stack(True))
            .cell(
                "Epoch duration max time",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.epoch_time.max"))
                    .all("host")
                    .all("computation_id")
                    .alias("{{computation_id}} - {{host}}")
                    .top(50)
                    .unit("UNIT_SECONDS")
                    .stack(False))
            .cell(
                "Epoch count total",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.epoch.rate"))
                    .aggr("host")
                    .all("computation_id")
                    .unit("UNIT_COUNT")
                    .stack(False))
            .cell(
                "Controller iteration duration",
                MonitoringExpr(FlowController("yt.flow.controller.*iteration_time.max"))
                    .stack(False)
                    .unit("UNIT_SECONDS"))
    )

def build_logging():
    return (Rowset()
        .row()
            .cell(
                "Controller top logging (warning+)",
                MonitoringExpr(
                    FlowController("yt.logging.written_events.rate")
                        .value("level", "warning|error|alert|fatal|maximum")
                        .value("category", "!-"))
                    .aggr("host")
                    .top()
                    .stack(False))
            .cell(
                "Worker top logging (warning+)",
                MonitoringExpr(
                    FlowWorker("yt.logging.written_events.rate")
                        .value("level", "warning|error|alert|fatal|maximum")
                        .value("category", "!-"))
                    .aggr("host")
                    .top()
                    .stack(False))
            .cell(
                "Controller top logging",
                MonitoringExpr(
                    FlowController("yt.logging.written_events.rate")
                        .value("level", "!-")
                        .value("category", "!-"))
                    .aggr("host")
                    .top()
                    .stack(False))
            .cell(
                "Worker top logging",
                MonitoringExpr(
                    FlowWorker("yt.logging.written_events.rate")
                        .value("level", "!-")
                        .value("category", "!-"))
                    .aggr("host")
                    .top()
                    .stack(False))
    )

def build_message_distributor():
    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell(
                "Push messages timeout",
                MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.push_messages_timeout_count.rate"))
                    .top()
                    .unit("UNIT_REQUESTS_PER_SECOND"))
            .cell(
                "Retransmits count rate",
                MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.retransmit_count.rate"))
                    .top()
                    .unit("UNIT_COUNTS_PER_SECOND"))
            .cell(
                "Unknown messages queue size",
                MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.unknown_task_queue_size"))
                    .top()
                    .unit("UNIT_COUNT"))
            .cell(
                "Inflight message count",
                MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.inflight_messages_count"))
                    .top()
                    .unit("UNIT_COUNT"))
    )

def build_pipeline():
    d = Dashboard()
    d.add(build_versions())
    d.add(build_resource_usage())
    d.add(build_flow_layout())
    d.add(build_flow_layout_mutations())
    d.add(build_streams())
    d.add(build_computations())
    d.add(build_computation_resources())
    d.add(build_partition_store_commits())
    d.add(build_heartbeats())
    d.add(build_epoch_timings())
    d.add(build_logging())
    d.add(build_message_distributor())

    d.set_title("[YT Flow] Pipeline general")
    d.add_parameter("project", "Pipeline project", MonitoringTextDashboardParameter())
    d.add_parameter("cluster", "Cluster", MonitoringTextDashboardParameter())
    d.add_parameter("proxy", "YT proxy", MonitoringTextDashboardParameter(default_value="-"))
    d.add_parameter("pipeline_path", "Pipeline path", MonitoringTextDashboardParameter(default_value="-"))
    d.add_parameter("computation_id", "Computation (only for some graphs)", MonitoringTextDashboardParameter(default_value="-"))

    return (d
        .value("project", TemplateTag("project"))
        .value("cluster", TemplateTag("cluster")))
