# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor
from yt_dashboard_generator.backends.monitoring import MonitoringTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.specific_tags.tags import TemplateTag

from ..common.sensors import *

##################################################################


def with_trend(sensor, limit_sensor):
    return MultiSensor(MonitoringExpr(sensor).alias("Usage"),
                MonitoringExpr(sensor).linear_trend("-45d", "60d").alias("Trend"),
                MonitoringExpr(limit_sensor).alias("Limit"))


def build_tablet_static_planning():
    memory_usage = (lambda category: with_trend(
        TabNode("yt.cluster_node.memory_usage.used").value("category", category),
        TabNode("yt.cluster_node.memory_usage.limit").value("category", category)))

    return (Rowset()
            .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
            .stack(False)
            .aggr(MonitoringTag("host"))
            .row()
                .cell("Tablet static memory", memory_usage("tablet_static")))


def build_node_resource_capacity_planning():
    per_container_limit = 629145600
    node_count = MonitoringExpr(BundleController("yt.bundle_controller.resource.target_tablet_node_count")).all(MonitoringTag("host")).top(1)
    return (Rowset()
            .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
            .stack(False)
            .row()
                .cell("Tablet Node CPU Total", with_trend(
                                    MonitoringExpr(TabNodePorto("yt.porto.vcpu.total").value("container_category", "pod")
                                        .aggr(MonitoringTag("host")))/100,
                                    MonitoringExpr(TabNodePorto("yt.porto.vcpu.guarantee").value("container_category", "pod")
                                        .aggr(MonitoringTag("host")))/100))
                .cell("Tablet Node Memory Total", with_trend(
                                    MonitoringExpr(TabNodePorto("yt.porto.memory.anon_usage").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))),
                                    MonitoringExpr(TabNodePorto("yt.porto.memory.memory_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host")))))
            .row()
                .cell("Tablet Node Net TX total", with_trend(
                                    TabNodePorto("yt.porto.network.tx_bytes").value("container_category", "pod").aggr(MonitoringTag("host")),
                                    node_count * per_container_limit))
                .cell("Tablet Node Net RX Total", with_trend(
                                    TabNodePorto("yt.porto.network.rx_bytes").value("container_category", "pod").aggr(MonitoringTag("host")),
                                    node_count * per_container_limit))
            .row()
                .stack(True)
                .cell("Disk Write Total", MultiSensor(
                                    MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.disk_space.rate")
                                        .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium")
                                        .all("method")).alias("{{method}}")))
                .cell("Disk Read Total", MultiSensor(
                                    MonitoringExpr(NodeTablet("yt.tablet_node.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                        .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium")
                                        .all("method")).alias("{{method}}"),
                                    MonitoringExpr(NodeTablet("yt.tablet_node.lookup.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                        .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                        .alias("lookup"),
                                    MonitoringExpr(NodeTablet("yt.tablet_node.select.chunk_reader_statistics.data_bytes_read_from_disk.rate")
                                        .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
                                        .alias("select")))
            ).owner


def build_rpc_proxy_resource_capacity_planning():
    per_container_limit = 157286400
    rpc_proxy_count = (
        MonitoringExpr(BundleController("yt.bundle_controller.resource.target_rpc_proxy_count")).
            all(MonitoringTag("host"))
            .top(1)
            .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle")))
    return (Rowset()
            .stack(False)
            .row()
                .value("proxy_role", TemplateTag("tablet_cell_bundle"))
                .cell("Rpc Proxy CPU Total", with_trend(
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.total").value("container_category", "pod")
                                        .aggr(MonitoringTag("host")))/100,
                                    MonitoringExpr(RpcProxyPorto("yt.porto.vcpu.guarantee").value("container_category", "pod")
                                        .aggr(MonitoringTag("host")))/100))
                .cell("Rpc Proxy Memory Total", with_trend(
                                    RpcProxyPorto("yt.porto.memory.anon_usage").value("container_category", "pod")
                                        .aggr(MonitoringTag("host")),
                                    RpcProxyPorto("yt.porto.memory.memory_limit").value("container_category", "pod")
                                        .aggr(MonitoringTag("host"))))
            .row()
                .cell("Rpc Proxy Net TX total", with_trend(
                                    RpcProxyPorto("yt.porto.network.tx_bytes").value("container_category", "pod")
                                        .value("proxy_role", TemplateTag("tablet_cell_bundle"))
                                        .aggr(MonitoringTag("host")),
                                    rpc_proxy_count * per_container_limit))
                .cell("Rpc Proxy Net RX Total", with_trend(
                                    RpcProxyPorto("yt.porto.network.rx_bytes").value("container_category", "pod")
                                        .value("proxy_role", TemplateTag("tablet_cell_bundle"))
                                        .aggr(MonitoringTag("host")),
                                    rpc_proxy_count.alias("Container Net RX Limit") * per_container_limit))
            ).owner
