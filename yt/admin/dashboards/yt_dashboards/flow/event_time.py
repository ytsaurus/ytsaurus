# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from .common import build_versions, add_common_dashboard_parameters

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import EmptyCell
from yt_dashboard_generator.taggable import NotEquals


def build_lags():
    return (Rowset()
        .stack(False)
        .row()
            .cell(
                "Aggregated stream time lag",
                MonitoringExpr(FlowController("yt.flow.controller.streams.time_lag"))
                    .value("stream_id", "__*__")
                    .alias("{{stream_id}}")
                    .unit("UNIT_SECONDS")
                    .precision(1))
            .cell(
                "Stream time lag",
                MonitoringExpr(FlowController("yt.flow.controller.streams.time_lag"))
                    .value("stream_id", NotEquals("__*__"))
                    .alias("{{stream_id}}")
                    .unit("UNIT_SECONDS")
                    .precision(1))
            .cell(
                "Aggregated event watermark lag",
                MonitoringExpr(FlowController("yt.flow.controller.streams.event_time_lag"))
                    .value("stream_id", "__*__")
                    .alias("{{stream_id}}")
                    .unit("UNIT_SECONDS")
                    .precision(1))
            .cell(
                "Event watermark lag",
                MonitoringExpr(FlowController("yt.flow.controller.streams.event_time_lag"))
                    .value("stream_id", NotEquals("__*__"))
                    .alias("{{stream_id}}")
                    .unit("UNIT_SECONDS")
                    .precision(1))
    )


def build_late_messages():
    late = MonitoringExpr(FlowWorker("yt.flow.worker.computation.watermark_generator.late.rate"))
    total = MonitoringExpr(FlowWorker("yt.flow.worker.computation.watermark_generator.total.rate"))
    return (Rowset()
        .stack(False)
        .all("computation_id")
        .aggr("host")
        .row()
            .cell("Late messages (by event timestamp)", late)
            .cell("Total messages", total)
            .cell("Late messages ratio", late / total)
            .cell("", EmptyCell())
    )


def build_flow_event_time():
    d = Dashboard()
    d.add(build_versions())
    d.add(build_lags())
    d.add(build_late_messages())

    d.set_title("[YT Flow] Pipeline event time & watermarks")
    add_common_dashboard_parameters(d)

    return (d
        .value("project", TemplateTag("project"))
        .value("cluster", TemplateTag("cluster")))
