# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowWorker

from .common import create_dashboard

from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import EmptyCell, MultiSensor


def build_companion_manager():
    return (
        Rowset()
        .stack(False)
        .all("host")
        .row()
        .cell(
            "Companion restarts rate",
            MonitoringExpr(FlowWorker("yt.flow.worker.resource.companion_manager.restarts.rate"))
            .query_transformation("sum(rate_to_delta({query})) by 1m")
            .value("resource", "CompanionManager"),
            description="Rate of companion process restarts per minute. Check companion logs for errors in case of high restarts rate.",
        )
        .cell(
            "Companion memory",
            MonitoringExpr(FlowWorker("yt.flow.worker.resource.companion_manager.memory_usage"))
            .unit("UNIT_BYTES_SI")
            .value("resource", "CompanionManager"),
            description="Companion process memory usage. May be imprecise due to sampling nature of measuring.",
        )
        .cell(
            "Companion threads",
            MonitoringExpr(FlowWorker("yt.flow.worker.resource.companion_manager.thread_count"))
            .value("resource", "CompanionManager"),
        )
        .cell("", EmptyCell())
    ).owner


def build_companion_jvm_memory():
    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell(
                "JVM heap memory used (by region)",
                MonitoringExpr(FlowWorker("jvm.memory.used"))
                    .value("area", "heap")
                    .all("id")
                    .alias("{{id}} - {{host}}")
                    .unit("UNIT_BYTES_SI"),
                description="JVM heap memory used per region (Eden, Survivor, Old Gen). Aggregated across hosts.")
            .cell(
                "JVM heap memory committed (by region)",
                MonitoringExpr(FlowWorker("jvm.memory.committed"))
                    .value("area", "heap")
                    .all("id")
                    .alias("{{id}} - {{host}}")
                    .unit("UNIT_BYTES_SI"),
                description="JVM heap memory reserved by the JVM from the OS, per region.")
            .cell(
                "JVM non-heap memory used",
                MonitoringExpr(FlowWorker("jvm.memory.used"))
                    .value("area", "nonheap")
                    .all("id")
                    .alias("{{id}} - {{host}}")
                    .unit("UNIT_BYTES_SI"),
                description="JVM non-heap memory: Metaspace, Compressed Class Space, CodeHeap regions. Continuous growth may indicate a classloader leak.")
            .cell(
                "JVM direct buffer memory",
                MonitoringExpr(FlowWorker("jvm.buffer.memory.used"))
                    .all("id")
                    .alias("{{id}} - {{host}}")
                    .unit("UNIT_BYTES_SI"),
                description="Off-heap NIO direct / mapped buffer memory. Not accounted in heap; relevant for native interop and Netty.")
    ).owner


def build_companion_jvm_gc():
    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell(
                "GC pause time, max",
                MonitoringExpr(FlowWorker("jvm.gc.pause"))
                    .value("metric_type", "max")
                    .alias("{{cause}} - {{host}}")
                    .unit("UNIT_SECONDS"),
                description="Max GC pause duration. GC pauses block all application threads.")
            .cell(
                "GC pause rate",
                MonitoringExpr(FlowWorker("jvm.gc.pause"))
                    .value("metric_type", "count")
                    .alias("{{cause}} - {{host}}")
                    .unit("UNIT_COUNTS_PER_SECOND"),
                description="Number of GC pauses per second.")
            .cell(
                "Heap allocation rate",
                MonitoringExpr(FlowWorker("jvm.gc.memory.allocated"))
                    .unit("UNIT_BYTES_SI_PER_SECOND"),
                description="Rate at which the young generation is being filled. High allocation rate increases GC pressure.")
            .cell(
                "Live data size after GC",
                MultiSensor(
                    MonitoringExpr(FlowWorker("jvm.gc.live.data.size"))
                        .alias("live data - {{host}}"),
                    MonitoringExpr(FlowWorker("jvm.gc.max.data.size"))
                        .alias("max - {{host}}"),
                )
                    .unit("UNIT_BYTES_SI"),
                description="Old-gen size after full GC vs configured max heap. Sustained growth toward max indicates a memory leak.")
    ).owner


def build_companion_jvm_threads_classes():
    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell(
                "JVM threads by state",
                MonitoringExpr(FlowWorker("jvm.threads.states"))
                    .all("state")
                    .alias("{{state}} - {{host}}")
                    .unit("UNIT_COUNT"),
                description="Live JVM threads broken down by state (runnable, blocked, waiting, timed-waiting, new, terminated).")
            .cell(
                "JVM thread counts",
                MultiSensor(
                    MonitoringExpr(FlowWorker("jvm.threads.live")).alias("live - {{host}}"),
                    MonitoringExpr(FlowWorker("jvm.threads.daemon")).alias("daemon - {{host}}"),
                    MonitoringExpr(FlowWorker("jvm.threads.peak")).alias("peak - {{host}}"),
                )
                    .unit("UNIT_COUNT"),
                description="Total live, daemon, and peak JVM thread counts. Unbounded growth suggests a thread leak.")
            .cell(
                "Deadlocked threads",
                MultiSensor(
                    MonitoringExpr(FlowWorker("jvm.threads.deadlocked"))
                        .alias("deadlocked - {{host}}"),
                    MonitoringExpr(FlowWorker("jvm.threads.deadlocked.monitor"))
                        .alias("deadlocked.monitor - {{host}}"),
                )
                    .unit("UNIT_COUNT"),
                description="Number of deadlocked threads. Must be zero — non-zero indicates a JVM-level deadlock requiring a thread dump.")
            .cell(
                "JVM classes loaded",
                MonitoringExpr(FlowWorker("jvm.classes.loaded"))
                    .unit("UNIT_COUNT"),
                description="Number of currently loaded classes. Sustained growth without bound suggests a classloader leak (often paired with Metaspace growth).")
    ).owner


def build_companion_process():
    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell(
                "Process CPU usage",
                MonitoringExpr(FlowWorker("process.cpu.usage"))
                    .unit("UNIT_PERCENT_UNIT"),
                description="Fraction of total available CPU consumed by the companion JVM process.")
            .cell(
                "System CPU usage",
                MonitoringExpr(FlowWorker("system.cpu.usage"))
                    .unit("UNIT_PERCENT_UNIT"),
                description="Fraction of total available CPU consumed by all processes on the host.")
            .cell(
                "System load average (1m)",
                MonitoringExpr(FlowWorker("system.load.average.1m"))
                    .unit("UNIT_NONE"),
                description="1-minute load average on the host. Compare against system.cpu.count to gauge overload.")
            .cell(
                "JVM threads started rate",
                MonitoringExpr(FlowWorker("jvm.threads.started"))
                    .unit("UNIT_COUNTS_PER_SECOND"),
                description="Rate of new thread creation. Spikes may indicate pool churn or thread leaks.")
    ).owner


def build_flow_companion_manager():
    def fill(d):
        d.add(build_companion_manager())
        d.add(build_companion_jvm_memory())
        d.add(build_companion_jvm_gc())
        d.add(build_companion_jvm_threads_classes())
        d.add(build_companion_process())

    return create_dashboard("companion-manager", fill)
