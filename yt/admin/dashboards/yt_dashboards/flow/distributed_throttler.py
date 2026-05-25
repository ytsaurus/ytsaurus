# flake8: noqa
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from .common import create_dashboard

from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import EmptyCell


# Server-side sensors live under /distributed_throttler on the controller,
# tagged with throttler_id. The underlying TThroughputThrottler emits
# /value, /released, /queue_size, /wait_time, /limit.
def build_throttler_server():
    return (Rowset()
        .all("throttler_id")
        .aggr("host")
        .row()
            .cell(
                "Global quota granted (server view)",
                MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.value.rate"))
                    .alias("{{throttler_id}}")
                    .unit("UNIT_COUNTS_PER_SECOND")
                    .stack(False)
                    .precision(1),
                description="Rate of token-bucket grants on the controller. This is the true "
                            "rate at which the global quota drains.")
            .cell(
                "Configured limit",
                MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.limit"))
                    .alias("{{throttler_id}}")
                    .unit("UNIT_COUNT")
                    .stack(False)
                    .precision(1),
                description="Current Limit from dynamic spec.")
            .cell(
                "Pending queue size",
                MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.queue_size"))
                    .alias("{{throttler_id}}")
                    .unit("UNIT_COUNT")
                    .stack(False)
                    .precision(1),
                description="Per-throttler count of in-flight RequestQuota calls waiting in the "
                            "priority queue on the controller. Sustained non-zero values mean "
                            "the throttler is saturated.")
            .cell(
                "Server wait time, max",
                MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.wait_time.max"))
                    .all("host")
                    .alias("{{throttler_id}} - {{host}}")
                    .top(50)
                    .unit("UNIT_SECONDS")
                    .stack(False)
                    .precision(2),
                description="Longest time a RequestQuota call sat on the server's priority queue. "
                            "Proxy for contention severity at the slowest-priority client. "
                            "Per-host to expose stragglers.")
    )


# Client-side consumption is collected by TMetricsTrackingThrottler wrapping
# TPrefetchingThrottler on each job. The sensor path is /computation/throttlers
# tagged with computation_id + throttler_id.
def build_throttler_client():
    return (Rowset()
        .all("computation_id")
        .all("throttler_id")
        .aggr("host")
        .row()
            .cell(
                "Local quota consumed per computation",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.throttlers.consumed.rate"))
                    .query_transformation('series_sum(["computation_id", "throttler_id"], {query})')
                    .alias("{{computation_id}} / {{throttler_id}}")
                    .unit("UNIT_COUNTS_PER_SECOND")
                    .stack(False)
                    .precision(1),
                description="Rate at which the computation actually draws from the throttler "
                            "(Throttle/TryAcquire/Acquire on the client). Compare to the "
                            "server-side 'quota granted' — the difference is local prefetch "
                            "buffering + any tokens still sitting in the prefetch pool.")
            .cell(
                "Local quota released per computation",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.throttlers.released.rate"))
                    .query_transformation('series_sum(["computation_id", "throttler_id"], {query})')
                    .alias("{{computation_id}} / {{throttler_id}}")
                    .unit("UNIT_COUNTS_PER_SECOND")
                    .stack(False)
                    .precision(1),
                description="Rate at which the computation returns quota via Release().")
            .cell(
                "Local Throttle() wait time, max",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.throttlers.wait_time.max"))
                    .all("host")
                    .alias("{{computation_id}} / {{throttler_id}} - {{host}}")
                    .top(50)
                    .unit("UNIT_SECONDS")
                    .stack(False)
                    .precision(2),
                description="Per-host max of time the computation spent blocked in Throttle(). "
                            "Spikes here indicate the local prefetch pool ran dry. "
                            "Top hosts surface stragglers.")
            .cell("", EmptyCell())
    )


# RPC-level view: prefetch requests from workers to the controller and their
# error counters. Filtered to the distributed throttler service so that other
# worker→controller RPCs don't pollute the graphs.
def build_throttler_rpc():
    service = "DistributedThrottlerService"
    method = "RequestQuota"

    def metric(sensor_suffix, alias_title):
        return (MonitoringExpr(FlowWorker(f"yt.rpc.client.{sensor_suffix}.rate"))
            .value("yt_service", service)
            .value("method", method)
            .aggr("host")
            .alias(alias_title)
            .unit("UNIT_REQUESTS_PER_SECOND")
            .stack(False)
            .precision(1))

    return (Rowset()
        .row()
            .cell(
                "Prefetch requests per second",
                metric("request_count", "requests"),
                description="Rate of RequestQuota RPCs from workers to the controller — the "
                            "cadence at which the prefetching layer refills local pools.")
            .cell(
                "Failed prefetch requests",
                metric("failed_request_count", "failed"),
                description="Non-retryable failures on RequestQuota.")
            .cell(
                "Timed-out prefetch requests",
                metric("timed_out_request_count", "timed out"),
                description="RequestQuota hitting the per-RPC timeout. Expected briefly during "
                            "controller failover; sustained non-zero is a problem.")
            .cell(
                "Prefetch request time, max",
                MonitoringExpr(FlowWorker("yt.rpc.client.request_time.total.max"))
                    .value("yt_service", service)
                    .value("method", method)
                    .all("host")
                    .alias("{{host}}")
                    .top(50)
                    .unit("UNIT_SECONDS")
                    .stack(False)
                    .precision(2),
                description="How long a RequestQuota call spent in-flight. Includes both time "
                            "the controller queued the request and network. Per-host top.")
    )


def build_flow_distributed_throttler():
    def fill(d):
        d.add(build_throttler_server())
        d.add(build_throttler_client())
        d.add(build_throttler_rpc())

    return create_dashboard("distributed-throttler", fill)
