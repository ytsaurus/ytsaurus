# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController

from .common import build_versions, add_common_dashboard_parameters

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import Text, EmptyCell


def build_one_core():
    transformation = "sign(ramp(moving_avg({query}, 5m) - 0.8))"

    description_text = (
        "**One core overflow &rarr;**\n"
        "If you see values greater than 0, check that your partitions "
        "is not too large for one-core processing"
    )

    return (Rowset()
        .row()
            .cell("", Text(description_text))
            .cell(
                "One core overflow",
                MonitoringExpr(FlowController("yt.flow.controller.computations.partition_cpu_usage.max"))
                    .query_transformation(transformation)
                    .alias("{{computation_id}}")
                    .stack(True))
            .cell("", EmptyCell())
            .cell("", EmptyCell())
    ).owner


def build_flow_diagnostics():
    d = Dashboard()
    d.add(build_versions())
    d.add(build_one_core())

    d.set_title("[YT Flow] Pipeline diagnostics")
    add_common_dashboard_parameters(d)

    return d.value("project", TemplateTag("project")).value("cluster", TemplateTag("cluster"))
