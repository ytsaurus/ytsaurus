# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor
from yt_dashboard_generator.backends.monitoring import MonitoringTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.specific_tags.tags import TemplateTag

from ..common.sensors import *

##################################################################


def build_efficiency_rowset():
    def make(sensor):
        return MultiSensor(sensor.alias("{{tablet_cell_bundle}}"), sensor.aggr("tablet_cell_bundle").alias("whole {{cluster}}"))

    chunk_writer_disk_space = MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.disk_space.rate")
        .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium", "method"))

    write_dw = MonitoringExpr(NodeTablet("yt.tablet_node.write.data_weight.rate")
        .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))

    compression_cpu_time = MonitoringExpr(NodeTablet("yt.tablet_node.chunk_writer.compression_cpu_time.rate")
        .aggr(MonitoringTag("host"), "table_path", "table_tag", "account", "medium", "method"))

    disk_read = lambda method: MonitoringExpr(NodeTablet(f"yt.tablet_node.{method}.chunk_reader_statistics.data_bytes_read_from_disk.rate")
        .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
    method_read = lambda method: MonitoringExpr(NodeTablet(f"yt.tablet_node.{method}.data_weight.rate")
        .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
    decompression_cpu_time = lambda method: MonitoringExpr(NodeTablet(f"yt.tablet_node.{method}.decompression_cpu_time.rate")
        .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))
    method_cpu = lambda method: MonitoringExpr(NodeTablet(f"yt.tablet_node.{method}.cumulative_cpu_time.rate")
        .aggr(MonitoringTag("host"), "table_path", "table_tag", "user"))

    return (Rowset()
        .stack(False)
        .value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
        .row()
            .cell(
                "Disk bytes written per user written byte",
                make(chunk_writer_disk_space / write_dw))
            .cell(
                "Compression CPU cores per user written gigabyte",
                make(compression_cpu_time * (2 ** 30) / write_dw))
        .row()
            .cell(
                "Disk bytes read per user lookup byte",
                make(disk_read("lookup") / method_read("lookup")))
            .cell(
                "Disk bytes read per user select byte",
                make(disk_read("select") / method_read("select")))
        .row()
            .cell(
                "Compression CPU cores per user lookup gigabyte",
                make(decompression_cpu_time("lookup") * (2 ** 30) / method_read("lookup")))
            .cell(
                "Compression CPU cores per user select gigabyte",
                make(decompression_cpu_time("select") * (2 ** 30) / method_read("select")))
        .row()
            .cell(
                "Lookup CPU cores per user lookup gigabyte",
                make(method_cpu("multiread") * (2 ** 30) / method_read("lookup")))
            .cell(
                "Select CPU cores per user select gigabyte",
                make(method_cpu("execute") * (2 ** 30) / method_read("select")))
        ).owner
