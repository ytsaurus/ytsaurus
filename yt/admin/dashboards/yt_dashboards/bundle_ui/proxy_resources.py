# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor, EmptyCell
from yt_dashboard_generator.backends.monitoring import MonitoringTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr

from . import common

from ..common.sensors import *

##################################################################


def top_max_bottom_min(sensor):
    return common.top_max_bottom_min(RpcProxyPorto(sensor))


def build_rpc_proxy_resource_overview_rowset():
    return (Rowset()
            .stack(False)
            .row()
                .cell("CPU Total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.guarantee").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container CPU Guarantee")/100,
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.total").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container CPU Usage")/100,
                                    MonitoringExpr(RpcProxyPorto("yt.resource_tracker.total_cpu")
                                        .sensor_stack()
                                        .aggr(MonitoringTag("host"))
                                        .all("thread")).alias("{{thread}}")/100))
                .cell("CPU per container", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.guarantee").value("container_category", "pod")
                                        .all(MonitoringTag("host")))
                                        .top(1)
                                        .alias("Guarantee {{container}}")/100,
                                    *[x / 100 for x in top_max_bottom_min("yt.porto.vcpu.total")]))
            .row()
                .cell("Memory Total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.memory.memory_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Memory Guarantee"),
                                    MonitoringExpr(RpcProxyPorto("yt.porto.memory.anon_usage").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Memory Usage")))
                .cell("Memory per container", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.memory.memory_limit").value("container_category", "pod")
                                        .all(MonitoringTag("host"))).alias("Guarantee {{container}}")
                                        .top(1),
                                    *top_max_bottom_min("yt.porto.memory.anon_usage")))
            .row()
                .cell("Net TX total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.network.tx_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Net Tx Guarantee"),
                                    MonitoringExpr(RpcProxyPorto("yt.porto.network.tx_bytes").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Net Tx Bytes Rate"),
                                    MonitoringExpr(RpcProxyInternal("yt.bus.out_bytes.rate")
                                        .aggr(MonitoringTag("host"), "band", "network", "encrypted")).alias("Rpc Proxy TX Bytes Rate")))
                .cell("Net TX per container", MultiSensor(MonitoringExpr(RpcProxyPorto("yt.porto.network.tx_limit").value("container_category", "pod")
                                        .all(MonitoringTag("host"))).alias("Guarantee {{container}}")
                                        .top(1),
                                        *top_max_bottom_min("yt.porto.network.tx_bytes")))
            .row()
                .cell("Net RX Total", MultiSensor(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.network.rx_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Net Rx Guarantee"),
                                    MonitoringExpr(RpcProxyPorto("yt.porto.network.rx_bytes").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))).alias("Container Net Rx Bytes Rate"),
                                    MonitoringExpr(RpcProxyInternal("yt.bus.in_bytes.rate")
                                        .aggr(MonitoringTag("host"), "band", "network", "encrypted")).alias("Rpc Proxy RX Bytes Rate")))
                .cell("Net RX per container", MultiSensor(MonitoringExpr(RpcProxyPorto("yt.porto.network.rx_limit").value("container_category", "pod")
                                        .all(MonitoringTag("host"))).alias("Guarantee {{container}}")
                                        .top(1),
                                        *top_max_bottom_min("yt.porto.network.rx_bytes")))
            .row()
                .cell(
                    "Memory per user",
                    RpcProxyMemory("yt.*heap_usage.user")
                        .all("user")
                        .aggr(MonitoringTag("host"))
                        .top())
                .cell("", EmptyCell())
    ).owner
