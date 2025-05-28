# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from .common import (
    build_versions, build_message_rate, build_resource_usage,
    add_common_dashboard_parameters)

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import MultiSensor, EmptyCell

from textwrap import dedent


def build_flow_status():

    def job_status(status, alias):
        return (FlowController(f"yt.flow.controller.job_status.{status}")
            .aggr("computation_id")
            .aggr("previous_job_finish_reason")
            .aggr("job_finish_reason")  # Temporary code.
            .query_transformation(f'alias({{query}}, "{alias}")'))

    def recovery_by_reason(statuses, alias):
        return (FlowController("|".join(f"yt.flow.controller.job_status.{status}" for status in statuses))
            .aggr("computation_id")
            .all("previous_job_finish_reason")
            .query_transformation(f'alias(series_sum("previous_job_finish_reason", {{query}}), "{alias}")'))

    recovery_by_reason_description = dedent("""\
        **Expect to see zero on this panel if pipeline is stably working.**
        ---
        **Recovering** — current job is preloading data or its status is unknown.
        **Warming up** — current job is working ≤ 5 minutes.
        ---
        **after Failed** — previous job failed.
        **after LostWorker** — previous job worker is gone (release/reallocation/OOM/crash/...).
        **after ExpiredLease** — job lease is expired or aborted (problems with YT / manual lease aborting / ...).
        **after Rebalanced** — previous job was stopped by balancer.
        **after Unknown** — partition is newly created or last job finish reason is unknown.
        **after Stopped** — pipeline is pausing / stopping.
        **after PartitionStateChanged** — partition changed its status (to completing or interrupting).
    """).replace("\n", "\n\n")

    return (Rowset()
        .stack(False)
        .row()
            .cell("Partitions by current job status",
                MultiSensor(
                    job_status("ok", "Working"),
                    job_status("working_old", "Stably working (≥ 5 min after recovering)"),
                    job_status("working_young", "Warming up (working ≤ 5 min after recovering)"),
                    job_status("working_with_retryable_error", "Has retryable errors"),
                    job_status("preparing", "Recovering (new job is preparing)"),
                    job_status("unknown", "Unknown"))
                    .min(0.8)
                    .unit("UNIT_COUNT")
                    .axis_type("YAXIS_TYPE_LOGARITHMIC"),
                colors={
                    "Working": "#00b200",
                    "Stably working (≥ 5 min after recovering)": "#00b200",
                    "Warming up (working ≤ 5 min after recovering)": "#b7e500",
                    "Has retryable errors": "#cc0000",
                    "Recovering (new job is preparing)": "#ffa500",
                    "Unknown": "#11114e",
                })
            .cell("Recovering/warming up by reason",
                MultiSensor(
                    recovery_by_reason(["unknown", "preparing"], "Recovering after {{{{previous_job_finish_reason}}}}"),
                    recovery_by_reason(["working_young"], "Warming up after {{{{previous_job_finish_reason}}}}"))
                    .stack(True)
                    .unit("UNIT_COUNT"),
                display_legend=False,
                description=recovery_by_reason_description,
                colors={
                    "Recovering after Failed": "#610000",
                    "Recovering after LostWorker": "#2d89e5",
                    "Recovering after ExpiredLease": "#e5e500",
                    "Recovering after PartitionStateChanged": "#e59400",
                    "Recovering after Rebalanced": "#b70000",
                    "Recovering after Stopped": "#00e500",
                    "Recovering after Unknown": "#0000e5",
                    "Warming up after Failed": "#d09999",
                    "Warming up after LostWorker": "#add6ff",
                    "Warming up after ExpiredLease": "#ffff66",
                    "Warming up after PartitionStateChanged": "#ffdb99",
                    "Warming up after Rebalanced": "#ea9999",
                    "Warming up after Stopped": "#99ff99",
                    "Warming up after Unknown": "#9999ff",
                })
            .cell("Registered workers count", FlowController("yt.flow.controller.worker_count").unit("UNIT_COUNT"))
            .cell("", EmptyCell())
    ).owner


def build_lags():
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
                "Event Watermark Lag",
                MultiSensor(
                    MonitoringExpr(FlowController("yt.flow.controller.streams.user_time_lag"))
                        .all("user_timestamp_id")
                        .alias("{{stream_id}} - {{user_timestamp_id}}"),
                    MonitoringExpr(FlowController("yt.flow.controller.streams.event_time_lag"))
                        .alias("{{stream_id}}"),
                )
                    .unit("UNIT_SECONDS")
                    .precision(1))
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

def build_epoch_timings():
    return (Rowset()
        .row()
            .cell(
                "Epoch parts time",
                MonitoringExpr(
                    FlowWorker("yt.flow.worker.computation.epoch_parts_time.rate")
                        .value("part", "!-"))
                    .aggr("host")
                    .aggr("computation_id")
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
            .cell("", EmptyCell())
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


def build_partition_aggregates():
    return (Rowset()
        .stack(False)
        .all("computation_id")
        .row()
            .cell(
                "Max partition cpu usage",
                MonitoringExpr(FlowController("yt.flow.controller.computations.partition_cpu_usage.max"))
                    .alias("{{computation_id}}"))
            .cell(
                "Average partition cpu usage",
                MonitoringExpr(FlowController("yt.flow.controller.computations.partition_cpu_usage.avg"))
                    .alias("{{computation_id}}"))
            .cell(
                "Max partition messages per second",
                MonitoringExpr(FlowController("yt.flow.controller.computations.partition_input_messages_per_second.max"))
                    .all("stream_id")
                    .alias("{{computation_id}} - {{stream_id}}")
                    .unit("UNIT_COUNTS_PER_SECOND"))
            .cell(
                "Average partition messages per second",
                MonitoringExpr(FlowController("yt.flow.controller.computations.partition_input_messages_per_second.avg"))
                    .all("stream_id")
                    .alias("{{computation_id}} - {{stream_id}}")
                    .unit("UNIT_COUNTS_PER_SECOND"))
    )


def build_flow_general():
    d = Dashboard()
    d.add(build_versions())
    d.add(build_resource_usage("controller", add_component_to_title=True))
    d.add(build_resource_usage("worker", add_component_to_title=True))
    d.add(build_flow_status())
    d.add(build_lags())
    d.add(build_message_rate())
    d.add(build_epoch_timings())
    d.add(build_computation_resources())
    d.add(build_partition_aggregates())
    d.add(build_partition_store_commits())
    d.add(build_logging())

    d.set_title("[YT Flow] Pipeline general")
    add_common_dashboard_parameters(d)

    return (d
        .value("project", TemplateTag("project"))
        .value("cluster", TemplateTag("cluster")))
