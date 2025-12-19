# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from yt_dashboard_generator.backends.monitoring import (
    MonitoringProjectDashboardParameter,
    MonitoringLabelDashboardParameter,
    MonitoringTextDashboardParameter,
)
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.sensor import EmptyCell, MultiSensor, Text

import dataclasses
import urllib.parse

from textwrap import dedent
from typing import Callable

@dataclasses.dataclass
class DashboardMeta:
    short_name: str
    name: str
    title: str


def get_dashboards_meta():
    dashboards = {}
    for short_name in [
        "general",
        "diagnostics",
        "event-time",
        "controller",
        "worker",
        "computation",
        "one-worker",
        "message-transfering",
        "state-cache"
    ]:
        dashboards[short_name] = DashboardMeta(
            short_name=short_name,
            name=f"ytsaurus-flow-{short_name}",
            title=short_name.replace('-', ' ').capitalize(),
        )
    return dashboards


DASHBOARDS_META = get_dashboards_meta()


def build_dashboard_links(dashboard_short_name: str):
    assert dashboard_short_name in DASHBOARDS_META, f"Unknown dashboard {dashboard_short_name}"

    return [
        {
            "group": {
                "items": [
                    {
                        "link": {
                            "title": meta.title,
                            "openInNewTab": False,
                            "dashboard": {
                                "dashboardName": meta.name,
                                "parameters": {
                                    "project": "{{project}}",
                                    "cluster": "{{cluster}}",
                                    "pipeline_cluster": "{{pipeline_cluster}}",
                                    "pipeline_path": "{{pipeline_path}}"
                                },
                                "applyTimeRange": True,
                                "projectId": "yt",
                            },
                        },
                    }
                    for meta in DASHBOARDS_META.values()
                ],
                "title": "Dashboards",
            },
        },
        {
            "group": {
                "items": [
                    {
                        "link": {
                            "title": "This dashboard documentation",
                            "openInNewTab": False,
                            "url": f"https://yt.yandex-team.ru/docs/flow/release/ui#{dashboard_short_name}-dashboard",
                        },
                    },
                    {
                        "link": {
                            "title": "Flow documentation",
                            "openInNewTab": False,
                            "url": "https://yt.yandex-team.ru/docs/flow/about",
                        },
                    },
                ],
                "title": "Documentation",
            },
        },
        {
            "link": {
                "title": "Tracing",
                "openInNewTab": False,
                "url": "https://monitoring.yandex-team.ru/projects/yt/traces?query=%7Bproject%20%3D%20%22{{project}}%22%2C%20service%20%3D%20%22{{cluster}}%22%7D",
            },
        }
    ]


def build_versions(worker_host_aggr: bool = True):
    spec_version_change_query_transformation = "sign(derivative({query}))"

    def make_url(name):
        return (f"https://monitoring.yandex-team.ru/projects/yt/dashboards/{name}"
            "?p[project]={{project}}&p[cluster]={{cluster}}")

    description_rows = ["&#128196; [Diagnosis and problem solving documentation](https://yt.yandex-team.ru/docs/flow/release/problems)"] + [
        f"&#128200; [{dashboard_meta.title} dashboard]({make_url(dashboard_meta.name)})"
        for dashboard_meta in DASHBOARDS_META.values()
    ]
    description_text = "\n".join(description_rows)

    worker_versions = MonitoringExpr(FlowWorker("yt.build.version")).alias("{{version}}")
    if worker_host_aggr:
        worker_versions = worker_versions.aggr("host")
    else:
        worker_versions = worker_versions.value("host", TemplateTag("host"))

    return (Rowset()
        .stack(True)
        .row()
            .cell(
                "Controller versions",
                MonitoringExpr(FlowController("yt.build.version"))
                    .alias("{{version}}")
                    .aggr("host"),
                description="Color change on this graph means deploying new controller binary")
            .cell(
                "Worker versions",
                worker_versions,
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
                (MonitoringExpr(sensor("yt.resource_tracker.total_vcpu")) / 100)
                    .aggr("thread")
                    .unit("UNIT_NONE"),
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
                (MonitoringExpr(sensor("yt.resource_tracker.total_vcpu")) / 100)
                    .aggr("host")
                    .all("thread")
                    .stack(True)
                    .unit("UNIT_NONE"))
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
    dashboard.add_parameter("project", "Pipeline project", MonitoringProjectDashboardParameter())
    dashboard.add_parameter(
        "cluster",
        "Cluster",
        MonitoringLabelDashboardParameter("", "cluster", "-", selectors='{sensor="yt.build.version"}'),
    )
    dashboard.add_parameter("pipeline_cluster", "Pipeline cluster", MonitoringTextDashboardParameter(default_value="-"))
    dashboard.add_parameter("pipeline_path", "Pipeline path", MonitoringTextDashboardParameter(default_value="-"))


def create_dashboard(short_name: str, filler: Callable[Dashboard, None], worker_host_aggr: bool = True):
    d = Dashboard()
    d.set_title(f"[YT Flow] {DASHBOARDS_META[short_name].title}")
    d.set_monitoring_links(build_dashboard_links(short_name))
    add_common_dashboard_parameters(d)

    d.add(build_versions(worker_host_aggr=worker_host_aggr))

    filler(d)

    d.value("project", TemplateTag("project"))
    d.value("cluster", TemplateTag("cluster"))

    return d
