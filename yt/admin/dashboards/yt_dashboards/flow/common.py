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
            .cell("Controller versions", FlowController("yt.build.version").aggr("host"))
            .cell("Worker versions", FlowWorker("yt.build.version").aggr("host"))
            .cell("Specs version change", MultiSensor(
                MonitoringExpr(FlowController("yt.flow.controller.spec_version"))
                    .query_transformation(spec_version_change_query_transformation)
                    .alias("Spec change"),
                MonitoringExpr(FlowController("yt.flow.controller.dynamic_spec_version"))
                    .query_transformation(spec_version_change_query_transformation)
                    .alias("Dynamic spec change")))
            .cell("", Text(description_text))
    ).owner


def build_resource_usage(component: str, add_component_to_title: bool):
    sensor = {
        "controller": FlowController,
        "worker": FlowWorker,
    }[component]
    title_suffix = f" ({component.capitalize()})" if add_component_to_title else ""

    return (Rowset()
        .stack(False)
        .all("host")
        .row()
            .cell("Total VCPU" + title_suffix, sensor("yt.resource_tracker.total_vcpu")
                .aggr("thread")
                .unit("UNIT_PERCENT"))
            .cell(
                "Busiest thread pool" + title_suffix,
                MonitoringExpr(sensor("yt.resource_tracker.utilization"))
                    .unit("UNIT_PERCENT_UNIT")
                    .all("thread")
                    .group_by_labels("host", "v -> group_lines(\"sum\", top_avg(1, v))")
                    .alias("{{thread}} - {{host}}")
                    .top(10))
            .cell("Memory" + title_suffix, sensor("yt.resource_tracker.memory_usage.rss").unit("UNIT_BYTES_SI"))
            .cell("Network retransmits" + title_suffix, sensor("yt.bus.retransmits.rate")
                .aggr("band")
                .aggr("encrypted")
                .aggr("network"))
    ).owner


def build_message_rate():
    return (Rowset()
        .stack(True)
        .aggr("host")
        .all("computation_id")
        .all("stream_id")
        .row()
            .cell(
                "Registered processed messages rate",
                MultiSensor(
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.input_streams.persisted_count.rate"))
                        .alias("input - {{computation_id}} - {{stream_id}}"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.source_streams.persisted_count.rate"))
                        .alias("source - {{computation_id}} - {{stream_id}}"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.timer_streams.unregistered_count.rate"))
                        .alias("timer - {{computation_id}} - {{stream_id}}")
                )
                    .unit("UNIT_COUNTS_PER_SECOND"))
            .cell(
                "Registered generated messages rate",
                MultiSensor(
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.output_streams.registered_count.rate"))
                        .alias("output - {{computation_id}} - {{stream_id}}"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.timer_streams.registered_count.rate"))
                        .alias("timer - {{computation_id}} - {{stream_id}}")
                )
                    .unit("UNIT_COUNTS_PER_SECOND"))
            .cell(
                "Registered processed messages bytes rate",
                MultiSensor(
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.input_streams.persisted_bytes.rate"))
                        .alias("input - {{computation_id}} - {{stream_id}}"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.source_streams.persisted_bytes.rate"))
                        .alias("source - {{computation_id}} - {{stream_id}}"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.timer_streams.unregistered_bytes.rate"))
                        .alias("timer - {{computation_id}} - {{stream_id}}")
                )
                    .unit("UNIT_BYTES_SI_PER_SECOND"))
            .cell(
                "Registered generated messages bytes rate",
                MultiSensor(
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.output_streams.registered_bytes.rate"))
                        .alias("output - {{computation_id}} - {{stream_id}}"),
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.timer_streams.registered_bytes.rate"))
                        .alias("timer - {{computation_id}} - {{stream_id}}")
                )
                    .unit("UNIT_BYTES_SI_PER_SECOND"))
    )


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
