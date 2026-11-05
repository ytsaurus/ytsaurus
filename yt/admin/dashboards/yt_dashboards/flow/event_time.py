# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from .common import build_event_lag_percentile, create_dashboard

from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
from yt_dashboard_generator.backends.monitoring import MonitoringTextDashboardParameter
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import EmptyCell
from yt_dashboard_generator.taggable import NotEquals, SystemFields


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
    late = MonitoringExpr(FlowWorker("yt.flow.worker.computation.watermark_info.late.rate"))
    total = MonitoringExpr(FlowWorker("yt.flow.worker.computation.watermark_info.total.rate"))
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


def build_event_lag_per_computation(backend="monitoring"):
    # Per-message lag = now() - EventTimestamp, captured at three points:
    # input (in PostCommit, per processed input message/timer),
    # output (in PostCommit, per produced output message),
    # sink (inside the sink itself at its natural delivery moment — at
    # registration for sync sinks, on per-message ack for async sinks).
    # The percentile is the dashboard's "percentile" parameter.
    def lag_percentile(metric, alias, *extra):
        group_labels = ["computation_id", "stream_id"] + list(extra)
        return (build_event_lag_percentile(metric, "{{percentile}}", SystemFields.All, group_labels, backend)
            .alias(alias)
            .unit("UNIT_SECONDS")
            .stack(False))

    input_desc = "Lag = now() - EventTimestamp, measured at input (per processed message/timer)."
    output_desc = "Lag = now() - EventTimestamp, measured at output (per emitted message)."
    sink_desc = (
        "Lag = now() - EventTimestamp, measured by the sink at its natural delivery "
        "moment. For async sinks (e.g. logbroker) this is the per-message ack, which "
        "may arrive long after the epoch commit.")

    return (Rowset()
        .row()
            .cell(
                "Input event lag — p{{percentile}} per stream",
                lag_percentile("yt.flow.worker.computation.event_lag.input.lag",
                    "{{computation_id}} / {{stream_id}}"),
                description=input_desc)
            .cell(
                "Output event lag — p{{percentile}} per stream",
                lag_percentile("yt.flow.worker.computation.event_lag.output.lag",
                    "{{computation_id}} / {{stream_id}}"),
                description=output_desc)
            .cell(
                "Sink event lag — p{{percentile}} per sink",
                lag_percentile("yt.flow.worker.computation.sink.event_lag.lag",
                    "{{computation_id}} / {{stream_id}} → {{sink_id}}", "sink_id"),
                description=sink_desc)
    )


def build_flow_event_time(backend="monitoring"):
    def fill(d):
        d.add(build_lags())
        d.add(build_late_messages())
        d.add(build_event_lag_per_computation(backend))

    d = create_dashboard("event-time", fill, backend=backend)
    d.add_parameter("percentile", "Percentile", MonitoringTextDashboardParameter("90"), backends=["monitoring"])
    d.add_parameter("percentile", "Percentile", GrafanaTextboxDashboardParameter("90"), backends=["grafana"])
    return d
