# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor, Text, EmptyCell
from yt_dashboard_generator.backends.monitoring import MonitoringTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr

from . import common

from ..common.sensors import *

##################################################################


def top_max_bottom_min(sensor):
    return common.top_max_bottom_min(TabNodePorto(sensor))


cpu_guarantee = MonitoringExpr(TabNodePorto("yt.porto.cpu.guarantee")
    .value("container_category", "pod")) / 100
container_cpu_usage = MonitoringExpr(TabNodePorto("yt.porto.cpu.total")
    .value("container_category", "pod")) / 100

vcpu_guarantee = MonitoringExpr(TabNodePorto("yt.porto.vcpu.guarantee")
    .value("container_category", "pod")) / 100
container_vcpu_usage = MonitoringExpr(TabNodePorto("yt.porto.vcpu.total")
    .value("container_category", "pod")) / 100

memory_guarantee = MonitoringExpr(TabNodePorto("yt.porto.memory.memory_limit")
    .value("container_category", "pod"))
container_memory_usage = MonitoringExpr(TabNodePorto("yt.porto.memory.anon_usage")
    .value("container_category", "pod"))


def build_user_resource_overview_rowset():
    net_guarantee = lambda direction: MonitoringExpr(TabNodePorto(f"yt.porto.network.{direction}_limit")
        .value("container_category", "pod"))
    net_usage = lambda direction: MonitoringExpr(TabNodePorto(f"yt.porto.network.{direction}_bytes")
        .value("container_category", "pod"))

    node_net_aggr_tags = ["band", "network", "encrypted"]

    def net(row, direction):
        DIR = direction.upper()
        Dir = direction.capitalize()
        return (row.row()
            .min(0)
            .cell(f"Net {DIR} total", MultiSensor(
                    net_guarantee(direction).alias(f"Container Net {Dir} Guarantee"),
                    net_usage(direction).alias(f"Container Net {Dir} Bytes Rate"),
                    MonitoringExpr(TabNodeInternal("yt.bus.out_bytes.rate")
                        .aggr(MonitoringTag("host"), *node_net_aggr_tags)).alias(f"Node {DIR} Bytes Rate"))
                .aggr(MonitoringTag("host")))
            .cell(f"Net {DIR} per container", MultiSensor(
                net_guarantee(direction).all(MonitoringTag("host")).series_max().alias("Guarantee"),
                *top_max_bottom_min(f"yt.porto.network.{direction}_bytes"))))

    def chunk_reader_statistics(reader_type):
        if reader_type:
            reader_type += "."
        return MonitoringExpr(NodeTablet(
            f"yt.tablet_node.{reader_type}chunk_reader_statistics.data_bytes_read_from_disk.rate"))

    cpu_vs_vcpu_hint = """\
About the difference between CPU and vCPU:
- CPU: actual physical cores of the instances. Thread pool sizes are measured \
    in terms of the CPU. Instances may have different CPU limits due to different hardware.
- vCPU: virtual cores of the instances scaled in order that all instances have \
    similar actual performance. CPU quotas correspond to vCPU.
"""

    return (Rowset()
        .stack(False)
        .row(height=5)
            .cell("", Text(cpu_vs_vcpu_hint))
            .cell("", EmptyCell())
        .row()
            .cell("CPU Total", MultiSensor(
                    cpu_guarantee.alias("Container CPU Guarantee"),
                    container_cpu_usage.alias("Container CPU Usage"),
                    MonitoringExpr(TabNodeCpu("yt.resource_tracker.total_cpu")
                        .sensor_stack()
                        .all("thread")).alias("{{thread}}") / 100)
                .aggr(MonitoringTag("host")))
            .cell(
                "CPU Per container",
                MultiSensor(
                    *[x / 100 for x in top_max_bottom_min("yt.porto.cpu.total")]),
                description="Physical CPU limits may differ between containers and are not displayed")
        .row()
            .cell("vCPU Total", MultiSensor(
                    vcpu_guarantee.alias("Container vCPU Guarantee"),
                    container_vcpu_usage.alias("Container vCPU Usage"))
                .aggr(MonitoringTag("host")))
            .cell("vCPU Per container", MultiSensor(
                vcpu_guarantee.all(MonitoringTag("host")).series_max().alias("Guarantee"),
                *[x / 100 for x in top_max_bottom_min("yt.porto.vcpu.total")]))
        .row()
            .min(0)
            .cell("Memory Total", MultiSensor(
                    memory_guarantee.alias("Container Memory Guarantee"),
                    container_memory_usage.alias("Container Memory Usage"),
                    MonitoringExpr(TabNode("yt.cluster_node.memory_usage.used")
                        .sensor_stack()
                        .all("category")).alias("{{category}}"))
                .aggr(MonitoringTag("host")))
            .cell("Memory per container", MultiSensor(
                memory_guarantee.all(MonitoringTag("host")).series_max().alias("Guarantee"),
                *top_max_bottom_min("yt.porto.memory.anon_usage")))
        .apply_func(lambda row: net(row, "tx"))
        .apply_func(lambda row: net(row, "rx"))
        .row()
            .stack(True)
            .cell("Disk Write Total", MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.disk_space.rate")
                .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium")
                .all("method")).alias("{{method}}"))
            .cell("Disk Write per container", MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.disk_space.rate")
                .aggr("method", "table_path", "table_tag", "account", "medium"))
                .alias("{{container}}")
                .all(MonitoringTag("host"))
                .top()
                .stack(False))
        .row()
            .cell("Disk Read Total", MultiSensor(
                    chunk_reader_statistics("").all("method").alias("{{method}}"),
                    chunk_reader_statistics("lookup").alias("lookup"),
                    chunk_reader_statistics("select").alias("select"))
                .aggr("table_path", "table_tag", "account", "medium", "user")
                .aggr(MonitoringTag("host"))
                .stack())
            .cell(
                "Disk Read per container",
                (chunk_reader_statistics("") + chunk_reader_statistics("lookup") + chunk_reader_statistics("select"))
                    .aggr("method", "table_path", "table_tag", "account", "medium", "user")
                    .alias("{{container}}")
                    .all(MonitoringTag("host"))
                    .top_avg(10)
                    .stack(False))
        # Not really useful signal at the moment, removed it from the dashboard to avoid confusion.
        #  .row()
        #      .stack(True)
        #      .cell("Master CPU", MonitoringExpr(Master("yt.tablet_server.update_tablet_stores.cumulative_time.rate")
        #            .aggr(MonitoringTag("host"), "container", "table_type", "update_reason"))
        #            .alias("Tablet Stores Update"))
        ).owner
