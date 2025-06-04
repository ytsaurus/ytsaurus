# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.sensor import MultiSensor, Text

from yt_dashboard_generator.backends.monitoring import MonitoringTextDashboardParameter


def build_versions():
    spec_version_change_query_transformation = "sign(derivative({query}))"

    def make_url(dashboard):
        return (f"https://monitoring.yandex-team.ru/projects/yt/dashboards/ytsaurus-flow-{dashboard}"
            "?p[project]={{project}}&p[cluster]={{cluster}}")

    description_text = "\n".join(
        f"* [{dashboard.replace('-', ' ').capitalize()} dashboard]({make_url(dashboard)})"
        for dashboard in [
            "general",
            "diagnostics",
            "event-time",
            "controller",
            "worker",
            "computation",
            "message-transfering"
        ]
    )

    return (Rowset()
        .stack(True)
        .row()
            .cell(
                "Controller versions",
                FlowController("yt.build.version").aggr("host"),
                description="Color change on this graph means deploying new controller binary")
            .cell(
                "Worker versions",
                FlowWorker("yt.build.version").aggr("host"),
                description="Color change on this graph means deploying new worker binary")
            .cell(
                "Specs version change",
                MultiSensor(
                    MonitoringExpr(FlowController("yt.flow.controller.spec_version"))
                        .query_transformation(spec_version_change_query_transformation)
                        .alias("Spec change"),
                    MonitoringExpr(FlowController("yt.flow.controller.dynamic_spec_version"))
                        .query_transformation(spec_version_change_query_transformation)
                        .alias("Dynamic spec change")),
                description="Spikes mean that [dynamic] spec has been changed")
            .cell("", Text(description_text))
    ).owner


def build_resource_usage(component: str, add_component_to_title: bool):
    sensor = {
        "controller": FlowController,
        "worker": FlowWorker,
    }[component]
    title_suffix = f" ({component.capitalize()})" if add_component_to_title else ""

    vcpu_description = (
        "VCPU is CPU multiplied by processor coefficient, "
        "so that new/old/Intel/AMD processors spend similar amount of VCPU-time to perform the same task"
    )

    retransmits_description = (
        "If you see significant increase on this graph, check that network limits are not exceeded"
    )

    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell(
                "Total VCPU" + title_suffix,
                sensor("yt.resource_tracker.total_vcpu")
                    .aggr("thread")
                    .unit("UNIT_PERCENT"),
                description=vcpu_description)
            .cell(
                "Busiest thread pool" + title_suffix,
                MonitoringExpr(sensor("yt.resource_tracker.utilization"))
                    .unit("UNIT_PERCENT_UNIT")
                    .all("thread")
                    .group_by_labels("host", "v -> group_lines(\"sum\", top_avg(1, v))")
                    .alias("{{thread}} - {{host}}")
                    .top(10))
            .cell("Memory" + title_suffix, sensor("yt.resource_tracker.memory_usage.rss").unit("UNIT_BYTES_SI"))
            .cell(
                "Network retransmits" + title_suffix,
                sensor("yt.bus.retransmits.rate")
                    .aggr("band")
                    .aggr("encrypted")
                    .aggr("network"),
                description=retransmits_description)
    ).owner


def build_text_row(text: str):
    return (Rowset()
        .row()
            .cell("", Text(text))
    )


def add_common_dashboard_parameters(dashboard):
    dashboard.add_parameter("project", "Pipeline project", MonitoringTextDashboardParameter())
    dashboard.add_parameter("cluster", "Cluster", MonitoringTextDashboardParameter())
    dashboard.add_parameter("proxy", "YT proxy", MonitoringTextDashboardParameter(default_value="-"))
    dashboard.add_parameter("pipeline_path", "Pipeline path", MonitoringTextDashboardParameter(default_value="-"))
