# flake8: noqa
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from .common import build_series_sum, create_dashboard

from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import EmptyCell


# Server-side sensors live under /distributed_throttler on the controller,
# tagged with throttler_id. The underlying TThroughputThrottler emits
# /value, /released, /queue_size, /wait_time, /limit.
def build_throttler_server():
    return (
        Rowset()
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
            "rate at which the global quota drains.",
        )
        .cell(
            "Configured limit",
            MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.limit"))
            .alias("{{throttler_id}}")
            .unit("UNIT_COUNT")
            .stack(False)
            .precision(1),
            description="Current Limit from dynamic spec.",
        )
        .cell(
            "Token-bucket queue size",
            MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.queue_size"))
            .alias("{{throttler_id}}")
            .unit("UNIT_COUNT")
            .stack(False)
            .precision(1),
            description="Number of selected quota chunks currently waiting in the global "
            "token bucket. Logical requests not yet dispatched are shown in the "
            "per-class queued-request graphs.",
        )
        .cell(
            "Server wait time, max",
            MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.wait_time.max"))
            .all("host")
            .alias("{{throttler_id}} - {{host}}")
            .top(50)
            .unit("UNIT_SECONDS")
            .stack(False)
            .precision(2),
            description="Longest wait of a selected chunk in the global token bucket. "
            "Per-class first-dispatch wait is shown below. Per-host to expose stragglers.",
        )
    )


def build_quota_classes():
    return (
        Rowset()
        .all("throttler_id")
        .all("quota_class")
        .aggr("host")
        .row()
        .cell(
            "Quota granted by class",
            MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.quota_class.granted.rate"))
            .alias("{{throttler_id}} / {{quota_class}}")
            .unit("UNIT_COUNTS_PER_SECOND")
            .stack(False)
            .precision(1),
            description="Successful server token-bucket grants attributed to each active "
            "quota class. Their sum is the bucket's global grant rate.",
        )
        .cell(
            "Queued logical requests by class",
            MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.quota_class.pending_requests"))
            .alias("{{throttler_id}} / {{quota_class}}")
            .unit("UNIT_COUNT")
            .stack(False)
            .precision(0),
            description="Logical requests waiting in each class queue; the currently " "dispatched chunk is excluded.",
        )
        .cell(
            "Queued quota amount by class",
            MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.quota_class.pending_amount"))
            .alias("{{throttler_id}} / {{quota_class}}")
            .unit("UNIT_COUNT")
            .stack(False)
            .precision(1),
            description="Remaining quota units waiting in each class queue.",
        )
        .cell(
            "Class wait time, max",
            MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.quota_class.wait_time.max"))
            .all("host")
            .alias("{{throttler_id}} / {{quota_class}} - {{host}}")
            .top(50)
            .unit("UNIT_SECONDS")
            .stack(False)
            .precision(2),
            description="Longest first-dispatch wait for a logical request in each class.",
        )
        .row()
        .cell(
            "Configured class weight",
            MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.quota_class.weight"))
            .alias("{{throttler_id}} / {{quota_class}}")
            .unit("UNIT_COUNT")
            .stack(False)
            .precision(2),
            description="Current scheduling weight. Weights define long-run bandwidth "
            "shares between backlogged classes.",
        )
        .cell(
            "Unknown class fallbacks",
            MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.unknown_class_requests.rate"))
            .aggr("quota_class")
            .alias("{{throttler_id}}")
            .unit("UNIT_REQUESTS_PER_SECOND")
            .stack(False)
            .precision(1),
            description="Requests carrying an unknown non-empty class id and routed to " "the reserved default class.",
        )
        .cell(
            "Classless requests",
            MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.classless_requests.rate"))
            .aggr("quota_class")
            .alias("{{throttler_id}}")
            .unit("UNIT_REQUESTS_PER_SECOND")
            .stack(False)
            .precision(1),
            description="Requests carrying no class id at all: manually obtained "
            "throttlers and computations that configure no class. On a bucket with "
            "weighted classes these compete in the reserved default class.",
        )
        .cell(
            "Quota refunded by class",
            MonitoringExpr(FlowController("yt.flow.controller.distributed_throttler.quota_class.refunded.rate"))
            .alias("{{throttler_id}} / {{quota_class}}")
            .unit("UNIT_COUNTS_PER_SECOND")
            .stack(False)
            .precision(1),
            description="Quota returned to the bucket after a request died before full "
            "delivery. Granted minus refunded is the quota actually consumed.",
        )
    )


# Client-side consumption is collected by TMetricsTrackingThrottler wrapping
# TPrefetchingThrottler on each job. The sensor path is /computation/throttlers
# tagged with computation_id + throttler_id.
def build_throttler_client(backend="monitoring"):
    def summed_by_throttler(sensor_name):
        return build_series_sum(sensor_name, ["computation_id", "throttler_id"], backend)

    return (
        Rowset()
        .all("computation_id")
        .all("throttler_id")
        .aggr("host")
        .row()
        .cell(
            "Local quota consumed per computation",
            summed_by_throttler("yt.flow.worker.computation.throttlers.consumed.rate")
            .alias("{{computation_id}} / {{throttler_id}}")
            .unit("UNIT_COUNTS_PER_SECOND")
            .stack(False)
            .precision(1),
            description="Rate at which the computation actually draws from the throttler "
            "(Throttle/TryAcquire/Acquire on the client). Compare to the "
            "server-side 'quota granted' — the difference is local prefetch "
            "buffering + any tokens still sitting in the prefetch pool.",
        )
        .cell(
            "Local quota released per computation",
            summed_by_throttler("yt.flow.worker.computation.throttlers.released.rate")
            .alias("{{computation_id}} / {{throttler_id}}")
            .unit("UNIT_COUNTS_PER_SECOND")
            .stack(False)
            .precision(1),
            description="Rate at which the computation returns quota via Release().",
        )
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
            "Top hosts surface stragglers.",
        )
        .cell("", EmptyCell())
    )


# RPC-level view: prefetch requests from workers to the controller and their
# error counters. Filtered to the distributed throttler service so that other
# worker→controller RPCs don't pollute the graphs.
def build_throttler_rpc():
    service = "DistributedThrottlerService"
    method = "RequestQuota"

    def metric(sensor_suffix, alias_title):
        return (
            MonitoringExpr(FlowWorker(f"yt.rpc.client.{sensor_suffix}.rate"))
            .value("yt_service", service)
            .value("method", method)
            .aggr("host")
            .alias(alias_title)
            .unit("UNIT_REQUESTS_PER_SECOND")
            .stack(False)
            .precision(1)
        )

    return (
        Rowset()
        .row()
        .cell(
            "Prefetch requests per second",
            metric("request_count", "requests"),
            description="Rate of RequestQuota RPCs from workers to the controller — the "
            "cadence at which the prefetching layer refills local pools.",
        )
        .cell(
            "Failed prefetch requests",
            metric("failed_request_count", "failed"),
            description="Non-retryable failures on RequestQuota.",
        )
        .cell(
            "Timed-out prefetch requests",
            metric("timed_out_request_count", "timed out"),
            description="RequestQuota hitting the per-RPC timeout. Expected briefly during "
            "controller failover; sustained non-zero is a problem.",
        )
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
            "the controller queued the request and network. Per-host top.",
        )
    )


def build_flow_distributed_throttler(backend="monitoring"):
    def fill(d):
        d.add(build_throttler_server())
        d.add(build_quota_classes())
        d.add(build_throttler_client(backend))
        d.add(build_throttler_rpc())

    return create_dashboard("distributed-throttler", fill, backend=backend)
