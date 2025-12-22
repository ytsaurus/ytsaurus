# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor, Text, Title, EmptyCell
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
anon_memory_limit = MonitoringExpr(TabNodePorto("yt.porto.memory.anon_limit")
    .value("container_category", "pod"))
anon_memory_usage = MonitoringExpr(TabNodePorto("yt.porto.memory.anon_usage")
    .value("container_category", "pod"))
oom_tracker_threshold = MonitoringExpr(TabNodeMemory("yt.memory.tcmalloc.desired_usage_limit_bytes"))


def disk_statistics(row, medium_name=None):
    Medium = "Medium" if medium_name else "Disk"

    def _throttler_statistics(type):
        if not medium_name:
            return []

        def _medium_throttler(type, metric):
            return MonitoringExpr(NodeTablet(f"yt.tablet_node.distributed_throttlers.{metric}")
                .value("throttler_id", f"{medium_name}_medium_{type}"))

        return [
            _medium_throttler(type, "configured_limit")
                .all(MonitoringTag("host"))
                .series_min()
                .alias(f"{Medium} {type} limit"),
            _medium_throttler(type, "usage")
                .aggr(MonitoringTag("host"))
                .alias(f"{Medium} {type} usage")
        ]

    def _with_medium_selector(expr):
        return expr.value("medium", medium_name) if medium_name else expr.aggr("medium")

    def _changelog_written_bytes():
        return _with_medium_selector(
            MonitoringExpr(NodeTablet("yt.tablet_node.remote_changelog.medium_written_bytes.rate")))

    def _chunk_writer_disk_space(writer_type):
        if writer_type and writer_type != '*':
            writer_type += "."

        return _with_medium_selector(
            MonitoringExpr(NodeTablet(f"yt.tablet_node.chunk_writer.{writer_type}disk_space.rate")))

    def _chunk_bytes_read(reader_type):
        if reader_type and reader_type != '*':
            reader_type += "."

        return _with_medium_selector(
            MonitoringExpr(NodeTablet(f"yt.tablet_node.{reader_type}chunk_reader_statistics.*_bytes_read_from_disk.rate")))

    return (row
        .row()
            .cell(f"{Medium} write total", MultiSensor(
                *_throttler_statistics("write"),
                MonitoringExpr
                    .flatten_sensors(
                        _changelog_written_bytes().alias("journal_writes")
                            .aggr(MonitoringTag("host"), "cell_id"),
                        _chunk_writer_disk_space("").all("method").alias("{{method}}")
                            .aggr(MonitoringTag("host"), "table_path", "table_tag", "account"),
                        _chunk_writer_disk_space("hunks").all("method").alias("hunks_{{method}}")
                            .aggr(MonitoringTag("host"), "table_path", "table_tag", "account"))
                    .sensor_stack())
                    .stack(False))
            .cell(f"{Medium} write per container",
                MonitoringExpr
                    .flatten_sensors(
                        _changelog_written_bytes()
                            .aggr("cell_id"),
                        _chunk_writer_disk_space("*")
                            .aggr("method", "table_path", "table_tag", "account"))
                    .series_sum("container")
                    .alias("{{container}}")
                    .all(MonitoringTag("host"))
                    .top_avg(10)
                    .stack(False))
        .row()
            .cell(f"{Medium} read total", MultiSensor(
                *_throttler_statistics("read"),
                MonitoringExpr
                    .flatten_sensors(
                        _chunk_bytes_read("").all("method").series_sum("method").alias("{{method}}"),
                        _chunk_bytes_read("chunk_reader.hunks").all("method").series_sum("method").alias("hunks_{{method}}"),
                        _chunk_bytes_read("lookup").series_sum().alias("lookup"),
                        _chunk_bytes_read("lookup.hunks").series_sum().alias("hunks_lookup"),
                        _chunk_bytes_read("select").series_sum().alias("select"),
                        _chunk_bytes_read("select.hunks").series_sum().alias("hunks_select"))
                    .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "user")
                    .sensor_stack())
                    .stack(False))
            .cell(f"{Medium} read per container",
                _chunk_bytes_read("*")
                    .aggr("method", "table_path", "table_tag", "account", "user")
                    .series_sum("container")
                    .alias("{{container}}")
                    .all(MonitoringTag("host"))
                    .top_avg(10)
                    .stack(False)))


def build_disk_statistics_rowsets(medium_names=["default", "ssd_blobs", "ssd_journals", "nvme_blobs"]):
        rowsets = [Rowset().row(height=2).cell("", Title("Disk statistics per medium", size="TITLE_SIZE_M")).owner]

        for medium_name in medium_names:
            rowsets.append(Rowset().row(height=2).cell("", Title(f"Medium: {medium_name}", size="TITLE_SIZE_S")).owner)
            rowsets.append(Rowset().apply_func(lambda r: disk_statistics(r, medium_name)).owner)

        return rowsets


def build_user_resource_overview_rowset(has_porto):
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
                *top_max_bottom_min(f"yt.porto.network.{direction}_bytes")),
                skip_cell=not has_porto))


    cpu_vs_vcpu_hint = """\
About the difference between CPU and vCPU:
- CPU: actual physical cores of the instances. Thread pool sizes are measured \
    in terms of the CPU. Instances may have different CPU limits due to different hardware.
- vCPU: virtual cores of the instances scaled in order that all instances have \
    similar actual performance. CPU quotas correspond to vCPU.
"""
    if not has_porto:
        cpu_vs_vcpu_hint += "\nCurrently vCPU is not available in this dashboard."

    return (Rowset()
        .stack(False)
        .row(height=5)
            .cell("", Text(cpu_vs_vcpu_hint))
            .cell("", EmptyCell(), skip_cell=not has_porto)
        .row()
            .cell("CPU Total", MultiSensor(
                    cpu_guarantee.alias("Container CPU Guarantee") if has_porto else None,
                    container_cpu_usage.alias("Container CPU Usage") if has_porto else None,
                    MonitoringExpr(TabNodeCpu("yt.resource_tracker.total_cpu")
                        .sensor_stack()
                        .all("thread")).alias("{{thread}}") / 100)
                .aggr(MonitoringTag("host")))
            .cell(
                "CPU Per container",
                MultiSensor(
                    *[x / 100 for x in top_max_bottom_min("yt.porto.cpu.total")]),
                description="Physical CPU limits may differ between containers and are not displayed",
                skip_cell=not has_porto)
        .row()
            .cell("vCPU Total", MultiSensor(
                    vcpu_guarantee.alias("Container vCPU Guarantee"),
                    container_vcpu_usage.alias("Container vCPU Usage"))
                .aggr(MonitoringTag("host")),
                skip_cell=not has_porto)
            .cell("vCPU Per container", MultiSensor(
                vcpu_guarantee.all(MonitoringTag("host")).series_max().alias("Guarantee"),
                *[x / 100 for x in top_max_bottom_min("yt.porto.vcpu.total")]),
                skip_cell=not has_porto)
        .row()
            .min(0)
            .cell("Memory Total", MultiSensor(
                    memory_guarantee.alias("Container Memory Guarantee") if has_porto else None,
                    anon_memory_limit.alias("Anon Memory Limit") if has_porto else None,
                    oom_tracker_threshold.alias("OOM tracker threshold"),
                    anon_memory_usage.alias("Anon Memory Usage") if has_porto else None,
                    MonitoringExpr(TabNode("yt.cluster_node.memory_usage.used")
                        .sensor_stack()
                        .all("category")).alias("{{category}}"))
                .aggr(MonitoringTag("host")))
            .cell("Memory per container", MultiSensor(
                memory_guarantee.all(MonitoringTag("host")).series_min().alias("Container Memory Guarantee"),
                anon_memory_limit.all(MonitoringTag("host")).series_min().alias("Anon Memory Limit"),
                oom_tracker_threshold.all(MonitoringTag("host")).series_min().alias("OOM tracker threshold"),
                *top_max_bottom_min("yt.porto.memory.anon_usage")),
                skip_cell=not has_porto)
        .apply_func(lambda row: net(row, "tx"))
        .apply_func(lambda row: net(row, "rx"))
        .apply_func(disk_statistics)
        # Not really useful signal at the moment, removed it from the dashboard to avoid confusion.
        #  .row()
        #      .stack(True)
        #      .cell("Master CPU", MonitoringExpr(Master("yt.tablet_server.update_tablet_stores.cumulative_time.rate")
        #            .aggr(MonitoringTag("host"), "container", "table_type", "update_reason"))
        #            .alias("Tablet Stores Update"))
        ).owner
