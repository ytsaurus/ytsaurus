# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor
from yt_dashboard_generator.backends.monitoring import MonitoringTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr

from .common import cpu_usage

from ..common.sensors import *

##################################################################


def build_user_lsm():
    lsm_dw = MonitoringExpr(
        NodeTablet("yt.tablet_node.store_compactor.out_data_weight.rate")
            .aggr("eden", "reason"))

    write_dw = MonitoringExpr(
        NodeTablet("yt.tablet_node.write.data_weight.rate")
            .aggr("user"))

    osc = (lambda kind:
        MonitoringExpr(NodeTablet("yt.tablet_node.tablet.overlapping_store_count.max"))
            .top_max(100)
            .aggr("table_path", "table_tag")
            .all(kind)
            .series_max(kind))

    queue_size_hint = """\
Normally queue size should be around zero. The queue larger than a few hundreds \
or steady growth rate mean that compaction subsystem is overloaded.
"""

    wa_hint = """\
Ratio of compacted bytes to inserted bytes (flushes are not included and constitute \
an additional factor of 1 to write amplification). Normal values are around 2-5.
"""

    return (Rowset()
        .all(MonitoringTag("host"))
        .stack(False)
        .top()
        .row()
            .cell("Running compactions", NodeTablet("yt.tablet_node.store_compactor.running_compactions"))
            .cell("Running partitionings", NodeTablet("yt.tablet_node.store_compactor.running_partitionings"))
        .row()
            .cell(
                "Compaction queue size",
                NodeTablet("yt.tablet_node.store_compactor.feasible_compactions"),
                description=queue_size_hint)
            .cell(
                "Partitioning queue size",
                NodeTablet("yt.tablet_node.store_compactor.feasible_partitionings"),
                description=queue_size_hint)
        .row()
            .cell("Running flushes", NodeTablet("yt.tablet_node.store_flusher.running_store_flushes"))
            .cell(
                "LSM write amplification", (
                    (lsm_dw.value("activity", "compaction") + lsm_dw.value("activity", "partitioning"))
                        .moving_avg("10m") /
                    write_dw.moving_avg("10m"))
                        .aggr(MonitoringTag("host"), "table_tag", "table_path"),
                description=wa_hint
            )
        .row()
            .cell(
                "Max overlapping store count (per node)",
                NodeTablet("yt.tablet_node.tablet.overlapping_store_count.max")
                    .aggr("table_path", "table_tag"))
            .cell(
                "Max overlapping store count (per table)",
                MultiSensor(osc("table_path"), osc("table_tag")).top(False))
        .row()
            .cell("StoreCompact thread pool CPU usage", cpu_usage("StoreCompact"))
            .cell("Compression thread pool CPU usage", cpu_usage("Compression"))
        ).owner
