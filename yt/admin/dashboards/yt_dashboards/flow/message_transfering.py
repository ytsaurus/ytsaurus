# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowWorker

from .common import build_versions, build_message_rate, add_common_dashboard_parameters

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import MultiSensor


def build_message_distributor():
    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell(
                "Distributor: push messages timeout",
                MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.push_messages_timeout_count.rate"))
                    .top()
                    .unit("UNIT_REQUESTS_PER_SECOND"))
            .cell(
                "Distributor: retransmits count rate",
                MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.retransmit_count.rate"))
                    .top()
                    .unit("UNIT_COUNTS_PER_SECOND"))
            .cell(
                "Distributor: unknown messages queue size",
                MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.unknown_task_queue_size"))
                    .top()
                    .unit("UNIT_COUNT"))
            .cell(
                "Distributor: unknown messages rate",
                MultiSensor(
                    MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.added_unknown_tasks.rate")),
                    MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.fully_removed_unknown_tasks.rate")))
                .aggr("host")
                .unit("UNIT_COUNT"))
        .row()
            .cell(
                "Distributor: added -> accepted time",
                MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.message_accepted_time.max"))
                    .top()
                    .unit("UNIT_SECONDS"))
            .cell(
                "Distributor: added -> processed time",
                MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.message_processing_finished_time.max"))
                    .top()
                    .unit("UNIT_SECONDS"))
            .cell(
                "Distributor: queued message count",
                MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.queued_messages_count"))
                    .aggr("host")
                    .all("stream_id")
                    .top()
                    .stack(True)
                    .unit("UNIT_COUNT"))
            .cell(
                "Distributor: inflight message count",
                MonitoringExpr(FlowWorker("yt.flow.worker.message_distributor.inflight_messages_count"))
                    .aggr("host")
                    .all("stream_id")
                    .top()
                    .stack(True)
                    .unit("UNIT_COUNT"))
    )


def build_buffers():
    input_size = MonitoringExpr(FlowWorker("yt.flow.worker.buffer_state.computations.input.size"))
    input_limit = MonitoringExpr(FlowWorker("yt.flow.worker.buffer_state.computations.input.limit"))
    output_size = MonitoringExpr(FlowWorker("yt.flow.worker.buffer_state.computations.output.size"))
    output_limit = MonitoringExpr(FlowWorker("yt.flow.worker.buffer_state.computations.output.limit"))

    return (Rowset()
        .all("computation_id")
        .aggr("host")
        .row()
            .cell(
                "Input buffers size",
                input_size
                    .all("stream_id")
                    .alias("{{computation_id}} - {{stream_id}}")
                    .stack(True)
                    .unit("UNIT_BYTES_SI"))
            .cell(
                "Input buffers fill ratio",
                (input_size / input_limit)
                    .all("stream_id")
                    .alias("{{computation_id}} - {{stream_id}}")
                    .max(10)
                    .unit("UNIT_PERCENT_UNIT"))
            .cell(
                "Output buffers size",
                output_size
                    .all("stream_id")
                    .alias("{{computation_id}} - {{stream_id}}")
                    .stack(True)
                    .unit("UNIT_BYTES_SI"))
            .cell(
                "Output buffers fill ratio",
                (output_size / output_limit)
                    .all("stream_id")
                    .alias("{{computation_id}} - {{stream_id}}")
                    .max(10)
                    .unit("UNIT_PERCENT_UNIT"))
    )


def build_flow_message_transfering():
    d = Dashboard()
    d.add(build_versions())
    d.add(build_message_rate())
    d.add(build_buffers())
    d.add(build_message_distributor())

    d.set_title("[YT Flow] Pipeline message transfering")
    add_common_dashboard_parameters(d)

    return (d
        .value("project", TemplateTag("project"))
        .value("cluster", TemplateTag("cluster")))
