# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import EmptyCell, MultiSensor
from yt_dashboard_generator.backends.monitoring import MonitoringTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.specific_tags.tags import DuplicateTag

from .common import action_queue_utilization, cpu_usage

from .resources import vcpu_guarantee, container_vcpu_usage

from ..common.sensors import *

##################################################################


def build_total_cpu():
    return (Rowset()
        .stack(False)
        .min(0)
        .row()
            .cell("vCPU usage and guarantee", MultiSensor(
                vcpu_guarantee.all(MonitoringTag("host")).series_max().alias("Guarantee"),
                container_vcpu_usage.alias("Usage {{container}}")).top())
            .cell(
                "Node CPU usage (all threads)",
                MonitoringExpr(TabNodeCpu("yt.resource_tracker.total_cpu").aggr("thread")).top() / 100)
        .row()
            .top()
            .cell(
                "CPU wait (all threads)",
                MonitoringExpr(TabNodeCpu("yt.resource_tracker.cpu_wait").aggr("thread")) / 100)
            .cell(
                "CPU throttled",
                MonitoringExpr(TabNodePorto("yt.porto.cpu.throttled").value("container_category", "pod")) / 100)
    ).owner


def build_user_thread_cpu():
    # total_cpu - cpu consumption in 100* of core
    # utilization is in 0..1
    # wait is in 100*core

    utilization_aggr = (
        (MonitoringExpr(TabNodeCpu("yt.resource_tracker.total_cpu")) / 100 /
            MonitoringExpr(TabNodeCpu("yt.resource_tracker.thread_count")))
            .all("thread")
            .alias("{{thread}} {{container}}")
            .aggr(DuplicateTag(MonitoringTag("host")))
            .top_max(10))

    utilization_all = (
        MonitoringExpr(TabNodeCpu("yt.resource_tracker.utilization"))
            .all("thread")
            .alias("{{thread}} {{container}}")
            .all(DuplicateTag(MonitoringTag("host")))
            .top_max(10))

    return (Rowset()
        .stack(False)
        .min(0)
        .top()
        .row()
            .cell("TabletLookup thread pool CPU usage", cpu_usage("TabletLookup"))
            .cell("Query thread pool CPU usage", cpu_usage("Query"))
        .row()
            .cell("TabletSlot thread pool CPU usage", cpu_usage("TabletSlot*"))
            .cell("Compression thread pool CPU usage", cpu_usage("Compression"))
        .row()
            .cell("StoreCompact thread pool CPU usage", cpu_usage("StoreCompact"))
            .cell("ChunkReader thread pool CPU usage", cpu_usage("ChunkReader"))
        .row()
            .cell("BusXfer thread pool CPU usage", cpu_usage("BusXfer|BusXferFS"))
            .cell("", EmptyCell())
        .row()
            .cell(
                "Threads utilization",
                MultiSensor(utilization_all, utilization_aggr).top(False))
            .cell("Action queue utilization", action_queue_utilization(TabNodeCpu))
        ).owner
