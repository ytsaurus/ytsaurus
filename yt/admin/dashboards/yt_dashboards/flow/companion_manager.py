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
from yt_dashboard_generator.sensor import EmptyCell


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


def build_flow_companion_manager():
    def fill(d):
        d.add(build_companion_manager())

    return create_dashboard("companion-manager", fill)
