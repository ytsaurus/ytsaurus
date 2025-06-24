# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from yt_dashboard_generator.backends.monitoring import MonitoringTextDashboardParameter
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import EmptyCell, MultiSensor, Text

from textwrap import dedent


def build_versions():
    spec_version_change_query_transformation = "sign(derivative({query}))"

    def make_url(dashboard):
        return (f"https://monitoring.yandex-team.ru/projects/yt/dashboards/ytsaurus-flow-{dashboard}"
            "?p[project]={{project}}&p[cluster]={{cluster}}")

    description_rows = ["&#128196; [Diagnosis and problem solving documentation](https://yt.yandex-team.ru/docs/flow/release/problems)"] + [
        f"&#128200; [{dashboard.replace('-', ' ').capitalize()} dashboard]({make_url(dashboard)})"
        for dashboard in [
            "general",
            "diagnostics",
            "event-time",
            "controller",
            "worker",
            "computation",
            "message-transfering"
        ]
    ]
    description_text = "\n".join(description_rows)

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


def build_extra_cpu(component: str):
    sensor = {
        "controller": FlowController,
        "worker": FlowWorker,
    }[component]

    return (Rowset()
        .row()
            .cell(
                "VCPU by thread name",
                sensor("yt.resource_tracker.total_vcpu")
                    .aggr("host")
                    .all("thread")
                    .stack(True)
                    .unit("UNIT_PERCENT"))
            .cell(
                "Waitings in action queue",
                MonitoringExpr(sensor("yt.action_queue.time.wait.max"))
                    .aggr("queue")
                    .aggr("bucket")
                    .all("thread")
                    .all("host")
                    .group_by_labels("thread", "v -> group_lines(\"sum\", top_avg(1, v))")
                    .alias("{{thread}} - {{host}}")
                    .unit("UNIT_SECONDS"),
                description="If there are high values here, check that thread pools are not overloaded")
            .cell("", EmptyCell())
            .cell("", EmptyCell())
    ).owner


def build_yt_rpc(component: str):
    sensor = {
        "controller": FlowController,
        "worker": FlowWorker,
    }[component]

    def rps_metric(who, metric):
        return (MonitoringExpr(sensor(f"yt.rpc.{who}.{metric}.rate"))
            .aggr("host")
            .all("method")
            .all("yt_service")
            .alias("{{yt_service}}.{{method}}")
            .stack(False)
            .unit("UNIT_REQUESTS_PER_SECOND")
        )

    def bps_metric(who, kind):
        return (MonitoringExpr(
                sensor(f"yt.rpc.{who}.{kind}_message_body_bytes.rate|yt.rpc.{who}.{kind}_message_attachment_bytes.rate"))
            .aggr("host")
            .all("method")
            .all("yt_service")
            .query_transformation('series_sum(["method", "yt_service"], {query})')
            .alias("{{yt_service}}.{{method}}")
            .stack(True)
            .unit("UNIT_BYTES_SI_PER_SECOND")
        )

    def max_time_metric(who):
        return (MonitoringExpr(sensor(f"yt.rpc.{who}.request_time.total.max"))
            .all("host")
            .all("method")
            .all("yt_service")
            .group_by_labels("method", "v -> group_lines(\"sum\", top_avg(1, v))")
            .alias("{{yt_service}}.{{method}} - {{host}}")
            .stack(False)
            .unit("UNIT_SECONDS")
        )

    return (Rowset()
        .row()
            .cell("YT RPC client requests", rps_metric("client", "request_count"))
            .cell("YT RPC client failed requests", rps_metric("client", "failed_request_count"))
            .cell("YT RPC client cancelled requests", rps_metric("client", "cancelled_request_count"))
            .cell("YT RPC client timed out requests", rps_metric("client", "timed_out_request_count"))
        .row()
            .cell("YT RPC client request data weight", bps_metric("client", "request"))
            .cell("YT RPC client response data weight", bps_metric("client", "response"))
            .cell("YT RPC client max request time", max_time_metric("client"))
            .cell("", EmptyCell())
        .row()
            .cell("YT RPC server requests", rps_metric("server", "request_count"))
            .cell("YT RPC server failed requests", rps_metric("server", "failed_request_count"))
            .cell("", EmptyCell())
            .cell("", EmptyCell())
        .row()
            .cell("YT RPC server request data weight", bps_metric("server", "request"))
            .cell("YT RPC server response data weight", bps_metric("server", "response"))
            .cell("YT RPC server max request time", max_time_metric("server"))
            .cell("", EmptyCell())
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
