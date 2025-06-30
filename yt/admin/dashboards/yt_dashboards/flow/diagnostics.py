# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from .common import build_versions, add_common_dashboard_parameters, build_text_row

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import Text, EmptyCell


def add_single_core_diagnostic(row):
    transformation = "sign(ramp(moving_avg({query}, 5m) - 0.8))"

    description_text = (
        "**Single core overflow &rarr;**\n"
        "If you see values greater than 0, pipeline has a partition that cannot be processed in single thread mode:\n"
        "* Check if the desired partition count is too small.\n"
        "* Check heavy_hitters (heavy hitter is a key with very significant messages rate)\n\n"
        "It may slow down whole pipeline.\n"
    )

    return (row
        .cell("", Text(description_text))
        .cell(
            "One core overflow",
            MonitoringExpr(FlowController("yt.flow.controller.computations.partition_cpu_usage.max"))
                .query_transformation(transformation)
                .alias("{{computation_id}}")
                .stack(True))
    )


def add_slow_input_messages_lookup_diagnostic(row):
    transformation = "ramp(moving_percentile(series_max({query}), 5m, 0.5) - 0.5)"

    description_text = (
        "**Slow input messages lookup &rarr;**\n"
        "If you see values greater than 0, check your input messages table:\n"
        "* Check that the table is properly resharded (`reshard_flow_tables`).\n"
        "* Check you bundle UI for bundle problems.\n\n"
        "It may slow down whole pipeline.\n"
    )

    return (row
        .cell("", Text(description_text))
        .cell(
            "Slow input messages lookup",
            MonitoringExpr(FlowWorker("yt.flow.worker.computation.partition_store.input_messages.lookup_time.max"))
                .aggr("computation_id")
                .query_transformation(transformation)
                .alias("{{computation_id}}")
                .stack(True))
    )


def add_empty_diagnostic(row):
    description_text = (
        "**Empty diagnostic &rarr;**\n"
        "It waits to be created :)"
    )

    return (row
        .cell("", Text(description_text))
        .cell("", MonitoringExpr(FlowWorker("")))
    )


def build_diagnostics():
    return (Rowset().set_cell_per_row(4)
        .row()
            .apply_func(add_single_core_diagnostic)
            .apply_func(add_slow_input_messages_lookup_diagnostic)
        .row()
            .apply_func(add_empty_diagnostic)
            .apply_func(add_empty_diagnostic)
    ).owner


def build_flow_diagnostics():
    d = Dashboard()
    d.add(build_versions())
    d.add(build_text_row(
        "# If you see values &ge; 0, something is wrong\n"
        "Values on graphs have no physical meaning. Consider anything &ge; 0 as lit warning.\n\n"
        "[Diagnostic documentation](https://yt.yandex-team.ru/docs/flow/release/problems)"
    ))
    d.add(build_diagnostics())

    d.set_title("[YT Flow] Pipeline diagnostics")
    add_common_dashboard_parameters(d)

    return d.value("project", TemplateTag("project")).value("cluster", TemplateTag("cluster"))
