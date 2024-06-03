# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import *

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.backends.monitoring import MonitoringTextDashboardParameter


def build_versions():
    return (Rowset()
        .stack(True)
        .aggr("host")
        .row()
            .cell("Controller versions", FlowController("yt.build.version"))
            .cell("Worker versions", FlowWorker("yt.build.version"))
    ).owner

def build_resource_usage():
    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell("Total CPU (Controller)", FlowController("yt.resource_tracker.total_cpu")
                .aggr("thread"))
            .cell("Control thread (Controller)", FlowController("yt.resource_tracker.total_cpu")
                .value("thread", "Control"))
            .cell("Total Memory (Controller)", FlowController("yt.resource_tracker.memory_usage.rss"))
        .row()
            .cell("Total CPU (Worker)", FlowWorker("yt.resource_tracker.total_cpu")
                .aggr("thread"))
            .cell(
                "Busiest thread",
                MonitoringExpr(FlowWorker("yt.resource_tracker.utilization"))
                    .all("thread")
                    .group_by_labels("host", "v -> group_lines(\"sum\", top_avg(1, v))"))
            .cell("Memory (Worker)", FlowWorker("yt.resource_tracker.memory_usage.rss"))
    ).owner

def build_flow_layout():
    return (Rowset()
        .stack(False)
        .row()
            .cell("Computations count", FlowController("yt.flow.controller.computation_count"))
            .cell("Partitions count", FlowController("yt.flow.controller.partition_count"))
            .cell("New jobs rate", FlowController("yt.flow.controller.new_job_count.rate"))
    ).owner

def build_watermarks():
    return (Rowset()
        .stack(False)
        .row()
            .cell("System watermark", FlowController("yt.flow.controller.system_timestamp_watermark_lag"))
            .cell("User watermarks", FlowController("yt.flow.controller.user_timestamp_watermark_lag"))
    ).owner

def build_message_distributor():
    return (Rowset()
        .stack(False)
        .aggr("host")
        .aggr("computation")
        .row()
            .cell("Total undelivered messages", FlowWorker("yt.flow.worker.computation.undelivered_output_messages_count"))
            .cell("Input messages rate", FlowWorker("yt.flow.worker.computation.input_messages.rate"))
            .cell("Output messages rate", FlowWorker("yt.flow.worker.computation.output_messages.rate"))

    )

def build_partition_store():
    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell("Partition store commit time", FlowWorker("yt.flow.worker.computation.partition_store.commit_time.max"))
            .cell("Input messages lookup time", FlowWorker("yt.flow.worker.computation.partition_store.input_messages.lookup_time.max"))
    )


def build_pipeline():
    d = Dashboard()
    d.add(build_versions())
    d.add(build_resource_usage())
    d.add(build_flow_layout())
    d.add(build_watermarks())
    d.add(build_message_distributor())
    d.add(build_partition_store())

    d.set_title("[YT Flow] Pipeline general")
    d.add_parameter("project", "Pipeline project", MonitoringTextDashboardParameter())
    d.add_parameter("cluster", "Cluster", MonitoringTextDashboardParameter())

    return (d
        .value("project", TemplateTag("project"))
        .value("cluster", TemplateTag("cluster")))
