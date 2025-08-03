# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowWorker

from .common import build_versions, add_common_dashboard_parameters

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.backends.monitoring import MonitoringTextDashboardParameter


def build_epoch_timings():
    return (Rowset()
        .row()
            .cell(
                "Epoch parts time (Computation: {{computation_id}})",
                MonitoringExpr(
                    FlowWorker("yt.flow.worker.computation.epoch_parts_time.rate")
                        .value("computation_id", "{{computation_id}}")
                        .value("part", "!-"))
                    .aggr("host")
                    .stack(True))
            .cell(
                "Epoch duration max time",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.epoch_time.max"))
                    .all("host")
                    .value("computation_id", "{{computation_id}}")
                    .alias("{{computation_id}} - {{host}}")
                    .top(50)
                    .unit("UNIT_SECONDS")
                    .stack(False))
            .cell(
                "Epoch count total",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.epoch.rate"))
                    .aggr("host")
                    .value("computation_id", "{{computation_id}}")
                    .unit("UNIT_COUNT")
                    .stack(False))
            .cell(
                "Computation list (copy id from here)",
                MonitoringExpr(FlowWorker("yt.flow.worker.computation.epoch_parts_time.rate"))
                    .aggr("host")
                    .value("part", "-")
                    .all("computation_id")
                    .query_transformation("sign({query})")
                    .stack(True)
                    .unit("UNIT_COUNT"))
    )


def build_flow_computation():
    d = Dashboard()
    d.add(build_versions())
    d.add(build_epoch_timings())

    d.set_title("[YT Flow] Pipeline computation")
    add_common_dashboard_parameters(d)
    d.add_parameter("computation_id", "Computation (only for some graphs)", MonitoringTextDashboardParameter(default_value="-"))

    return (d
        .value("project", TemplateTag("project"))
        .value("cluster", TemplateTag("cluster")))
