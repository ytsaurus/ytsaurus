# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import EmptyCell, MultiSensor
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr, PlainMonitoringExpr
from yt_dashboard_generator.specific_tags.tags import TemplateTag

from .common import action_queue_utilization

from .proxy_resources import memory_guarantee, anon_memory_limit, anon_memory_usage, oom_tracker_threshold

from ..common.sensors import *

##################################################################


def build_rpc_proxy_cpu(has_porto):
    utilization = MonitoringExpr(RpcProxyCpu("yt.resource_tracker.utilization")).value("thread", "*").alias("{{thread}} {{container}}")
    cpu_usage = (lambda thread: MultiSensor(RpcProxyCpu("yt.resource_tracker.thread_count"),
        MonitoringExpr(RpcProxyCpu("yt.resource_tracker.total_cpu")) / 100)
        .value("thread", thread))

    return (Rowset()
            .stack(False)
            .value("proxy_role", TemplateTag("tablet_cell_bundle"))
            .top()
            .row()
                .cell("CPU Total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.guarantee").value("container_category", "pod"))
                                        .top(3)
                                        .alias("Container CPU Guarantee {{container}}")/100,
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.total").value("container_category", "pod"))
                                        .alias("Container CPU Usage {{container}}")/100),
                                    skip_cell=not has_porto)
                .cell("Memory Total", MultiSensor(
                                    memory_guarantee.series_min().alias("Container Memory Guarantee") if has_porto else None,
                                    anon_memory_limit.series_min().alias("Anon Memory Limit") if has_porto else None,
                                    oom_tracker_threshold.series_min().alias("OOM tracker threshold"),
                                    anon_memory_usage.alias("Anon Memory Usage {{container}}") if has_porto else None))
            .row()
                .cell(
                    "Memory usage per method",
                    MonitoringExpr(RpcProxyMemory("yt.memory.heap_usage.rpc")
                                   .all("rpc")
                                   .top(False)),
                )
                .cell(
                    "Memory usage per user",
                    MonitoringExpr(RpcProxyMemory("yt.memory.heap_usage.user")
                                   .all("user")),
                )
            .row()
                .cell("Worker thread pool CPU usage", cpu_usage("Worker"))
                .cell("BusXfer thread pool CPU usage", cpu_usage("BusXferFS"))
            .row()
                .cell("CPU wait (all threads)", MonitoringExpr(RpcProxyCpu("yt.resource_tracker.cpu_wait")
                                                            .aggr("thread"))/100)
                .cell("", EmptyCell())
            .row()
                .cell("Threads utilization", utilization)
                .cell("Action queue utilization", action_queue_utilization(RpcProxyCpu))
            ).owner


def build_rpc_proxy_network():
    return (Rowset()
        .stack(False)
        .top()
        .row()
            .cell("Net TX total", MultiSensor(
                                MonitoringExpr(RpcProxyPorto("yt.porto.network.tx_limit").value("container_category", "pod"))
                                    .alias("Container Net Tx Guarantee {{container}}"),
                                MonitoringExpr(RpcProxyPorto("yt.porto.network.tx_bytes").value("container_category", "pod"))
                                    .alias("Container Net Tx Bytes Rate {{container}}"),
                                MonitoringExpr(RpcProxyInternal("yt.bus.out_bytes.rate")
                                    .aggr("band", "network", "encrypted")).alias("Rpc Proxy TX Bytes Rate {{container}}")))
            .cell("Net RX Total", MultiSensor(
                                MonitoringExpr(RpcProxyPorto("yt.porto.network.rx_limit").value("container_category", "pod"))
                                    .alias("Container Net Rx Guarantee {{container}}"),
                                MonitoringExpr(RpcProxyPorto("yt.porto.network.rx_bytes").value("container_category", "pod"))
                                    .alias("Container Net Rx Bytes Rate {{container}}"),
                                MonitoringExpr(RpcProxyInternal("yt.bus.in_bytes.rate")
                                    .aggr("band", "network", "encrypted")).alias("Rpc Proxy RX Bytes Rate {{container}}")))
        .row()
            .cell(
                "Pending out bytes",
                MonitoringExpr(RpcProxyInternal("yt.bus.pending_out_bytes"))
                    .all("band", "network")
                    .aggr("encrypted")
                    .alias("band: {{band}}, net: {{network}}")
                    .stack())
            .cell(
                "TCP retransmits rate",
                MonitoringExpr(RpcProxyInternal("yt.bus.retransmits.rate"))
                    .all("network")
                    .aggr("band", "encrypted")
                    .alias("net: {{network}}"))
    ).owner


def build_rpc_proxy_rpc_request_rate():
    request_rate = (RpcProxyRpc("yt.rpc.server.{}.rate")
                    .aggr("#U")
                    .all("yt_service", "method"))

    return (Rowset()
            .stack(False)
            .top()
            .row()
                .cell("RPC request rate", request_rate("request_count"))
                .cell("RPC inflight", RpcProxyRpc("yt.rpc.server.concurrency")
                    .aggr("#U")
                    .all("yt_service", "method"))
            .row()
                .cell("RPC failed request rate", request_rate("failed_request_count"))
                .cell("RPC timed out request rate", request_rate("timed_out_request_count"))
            ).owner


def build_rpc_proxy_rpc_request_wait():
    request_wait_max = (RpcProxyRpc("yt.rpc.server.request_time.{}.max")
                    .aggr("#U")
                    .all("yt_service", "method"))

    return (Rowset()
            .stack(False)
            .top()
            .row()
                .cell("RPC local wait max", request_wait_max("local_wait"))
                .cell("RPC remote wait max", request_wait_max("remote_wait"))
            ).owner


def build_rpc_proxy_maintenance(has_porto):
    return (Rowset()
            .stack(False)
            .row()
                .cell("Rpc Proxy restarts", MonitoringExpr(RpcProxy("yt.server.restarted")
                        .top()
                        .stack(True)
                        .value("window", "5min")).host_container_legend_format())
                .cell("Rpc Proxy OOMs", MultiSensor(
                        MonitoringExpr(RpcProxyPorto("yt.porto.memory.oom_kills").value("container_category", "pod"))
                            .diff()
                            .top_max(10)
                            .host_container_legend_format("porto oom kills"),
                        (MonitoringExpr(RpcProxyYtcfgen("yt.error_watcher.ooms"))
                            + PlainMonitoringExpr("constant_line(0)"))
                            .diff()
                            .drop_below(0)
                            .top_max(10)
                            .host_container_legend_format("memory limit kills")
                    ),
                    skip_cell=not has_porto)
            ).owner
