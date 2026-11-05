# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from .common import create_dashboard

from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import MultiSensor


def build_sweep_progress_rowset():
    return (
        Rowset()
        .stack(False)
        .aggr("host")
        .value("computation_id", "!-")
        .all("stream_id")
        .row()
        .cell(
            "Visit rate (generated / consumed)",
            MultiSensor(
                MonitoringExpr(
                    FlowWorker("yt.flow.worker.computation.key_visitor_streams.registered_count.rate")
                ).alias("generated - {{stream_id}}"),
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.key_visitor_streams.persisted_count.rate")).alias(
                    "consumed - {{stream_id}}"
                ),
            ).unit("UNIT_COUNTS_PER_SECOND"),
            description="Visits emitted into the fill buffer vs handed to processing.",
        )
        .cell(
            "Sweep rate vs limit",
            MultiSensor(
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.key_visitor_streams.throttler.value.rate")).alias(
                    "swept units/s - {{stream_id}}"
                ),
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.key_visitor_streams.throttler.limit")).alias(
                    "limit - {{stream_id}}"
                ),
            ).unit("UNIT_COUNTS_PER_SECOND"),
            description="Swept hash units/s vs the configured sweep-rate limit.",
        )
        .cell(
            "Sweep throttler queue",
            MonitoringExpr(FlowWorker("yt.flow.worker.computation.key_visitor_streams.throttler.queue_size"))
            .alias("{{stream_id}}")
            .unit("UNIT_COUNT"),
            description="Sweep requests waiting on the rate throttler.",
        )
    )


def build_sweep_lag_rowset():
    return (
        Rowset()
        .stack(False)
        .all("stream_id")
        .row()
        .cell(
            "Stream time lag",
            MonitoringExpr(FlowController("yt.flow.controller.streams.time_lag"))
            .alias("{{stream_id}}")
            .unit("UNIT_SECONDS")
            .precision(1),
            description="How far the sweep trails its period, in event time.",
        )
        .cell(
            "Stream count lag",
            MonitoringExpr(FlowController("yt.flow.controller.streams.count_lag"))
            .alias("{{stream_id}}")
            .unit("UNIT_COUNT"),
            description="Not-yet-committed messages in the stream.",
        )
    )


def build_joiner_rowset():
    joiner_alias = "{{computation_id}} / {{external_state_joiner}}"
    return (
        Rowset()
        .stack(False)
        .value("computation_id", "!-")
        .all("computation_id")
        .all("external_state_joiner")
        .row()
        .cell(
            "Joiner source unavailable",
            MonitoringExpr(FlowWorker("yt.flow.worker.computation.static_table_key_visitor_joiner.source_unavailable"))
            .all("host")
            .group_by_labels("external_state_joiner", "v -> group_lines(\"max\", v)")
            .alias(joiner_alias)
            .unit("UNIT_NONE")
            .min(0),
            description="0 in steady state, 1 while the source is gated after a failed read (max across hosts).",
        )
        .cell(
            "Joiner failed reads rate",
            MonitoringExpr(FlowWorker("yt.flow.worker.computation.static_table_key_visitor_joiner.failed_reads.rate"))
            .aggr("host")
            .alias(joiner_alias)
            .unit("UNIT_COUNTS_PER_SECOND"),
            description="Source reads that threw; each gates the source for the backoff.",
        )
        .cell(
            "Joiner reader opens rate",
            MonitoringExpr(FlowWorker("yt.flow.worker.computation.static_table_key_visitor_joiner.reader_opens.rate"))
            .aggr("host")
            .alias(joiner_alias)
            .unit("UNIT_COUNTS_PER_SECOND"),
            description="Forward-reader opens; spikes = backward jumps, 0 = reads stopped.",
        )
        .cell(
            "Joiner listed cache size",
            MonitoringExpr(FlowWorker("yt.flow.worker.computation.static_table_key_visitor_joiner.listed_size"))
            .aggr("host")
            .alias(joiner_alias)
            .unit("UNIT_COUNT"),
            description="Rows read ahead but not yet consumed.",
        )
    )


def build_flow_key_visitor(backend="monitoring"):
    def fill(d):
        d.add(build_sweep_progress_rowset())
        d.add(build_sweep_lag_rowset())
        d.add(build_joiner_rowset())

    return create_dashboard("key-visitor", fill, backend=backend)
