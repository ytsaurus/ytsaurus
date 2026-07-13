# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
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
        "state-cache",
        "companion-manager",
        "distributed-throttler",
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
            "link": {
                "title": "UI YT",
                "openInNewTab": True,
                "url": "https://yt.yandex-team.ru/{{pipeline_cluster}}/flows/graph?path={{pipeline_path}}",
            },
        },
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


def build_versions(worker_host_aggr: bool = True, backend: str = "monitoring"):
    def make_url(name):
        return (f"https://monitoring.yandex-team.ru/projects/yt/dashboards/{name}"
            "?p[project]={{project}}&p[cluster]={{cluster}}"
            "&p[pipeline_cluster]={{pipeline_cluster}}&p[pipeline_path]={{pipeline_path}}")

    def make_grafana_url(name):
        # A host-relative link by dashboard uid; grafana interpolates the
        # ${...} template variables inside text panel content.
        return (f"/d/{name}"
            "?var-project=${project}&var-cluster=${cluster}"
            "&var-pipeline_cluster=${pipeline_cluster}&var-pipeline_path=${pipeline_path}")

    url = make_url if backend == "monitoring" else make_grafana_url
    description_rows = ["&#128196; [Diagnosis and problem solving documentation](https://yt.yandex-team.ru/docs/flow/release/problems)"] + [
        f"&#128200; [{dashboard_meta.title} dashboard]({url(dashboard_meta.name)})"
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
                        .derivative()
                        .sign()
                        .alias("Spec change"),
                    MonitoringExpr(FlowController("yt.flow.controller.dynamic_spec_version"))
                        .derivative()
                        .sign()
                        .alias("Dynamic spec change")),
                description="Spikes mean that [dynamic] spec has been changed")
            .cell("", Text(description_text))
    ).owner


def build_event_lag_percentile(metric: str, percentile: str, computation_id, group_labels: list, backend: str):
    """Percentile of an event lag histogram sensor, one line per group_labels combination.

    percentile is in Solomon percent form: "90" or a "{{percentile}}" parameter reference.
    """
    extra_labels = [label for label in group_labels if label not in ("computation_id", "stream_id")]

    if backend == "monitoring":
        sensor = (MonitoringExpr(FlowWorker(metric))
            .aggr("host")
            .value("computation_id", computation_id)
            .all("stream_id")
            .all("bin"))
        for label in extra_labels:
            sensor = sensor.all(label)
        labels_vector = "as_vector(" + ", ".join(f'"{label}"' for label in group_labels) + ")"
        return MonitoringExpr.func(
            "group_by_labels", sensor, labels_vector,
            f"v -> histogram_percentile({percentile}, v)")

    # The exporter publishes these sensors as native Prometheus histograms;
    # compute the percentile from the "le" buckets. Raw per-host series are
    # summed here, so no aggregation layer is required ("all" excludes the
    # aggregated host="Aggr" series).
    grouping = ", ".join(["le"] + group_labels)
    grafana_percentile = percentile.replace("{{percentile}}", "$percentile")
    return (MonitoringExpr(FlowWorker(f"{metric}.bucket.rate"))
        .all("host")
        .value("computation_id", computation_id)
        .all("stream_id")
        .query_transformation(
            f"histogram_quantile(({grafana_percentile}) / 100, sum by ({grouping}) ({{query}}))"))


def build_median_over_hosts(metric: str, group_labels: list, backend: str):
    """Median across hosts of a sensor, one line per group_labels combination."""
    expr = MonitoringExpr(FlowWorker(metric)).all("host")
    if backend == "monitoring":
        labels = ", ".join(f'"{label}"' for label in group_labels)
        return expr.query_transformation(f'group_lines("median", [{labels}], {{query}})')
    return expr.query_transformation(f'quantile by ({", ".join(group_labels)}) (0.5, {{query}})')


def build_series_sum(metric: str, group_labels: list, backend: str):
    """Sum of a sensor's series, one line per group_labels combination."""
    expr = MonitoringExpr(FlowWorker(metric))
    if backend == "monitoring":
        labels = ", ".join(f'"{label}"' for label in group_labels)
        return expr.query_transformation(f'series_sum([{labels}], {{query}})')
    return expr.series_sum(*group_labels)


def build_resource_usage(component: str, add_component_to_title: bool, backend: str = "monitoring"):
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

    # An unlimited cgroup exports HierarchicalMemoryLimit as PAGE_COUNTER_MAX
    # (~9.2e18); such points would flatten the RSS line, so drop everything
    # above 1e18 (real limits are TBs at most).
    memory_limit = MonitoringExpr(sensor("yt.memory.cgroup.memory_limit"))
    if backend == "monitoring":
        memory_limit = memory_limit.query_transformation("drop_above({query}, 1e18)")
    else:
        memory_limit = memory_limit.query_transformation("({query}) < 1e18")
    memory_limit = memory_limit.alias("Limit")

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
            .cell(
                "Memory" + title_suffix,
                MultiSensor(
                    MonitoringExpr(sensor("yt.resource_tracker.memory_usage.rss"))
                        .alias("RSS"),
                    memory_limit)
                    .unit("UNIT_BYTES_SI"))
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
            .series_sum("method", "yt_service")
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
    dashboard.add_parameter(
        "project",
        "Pipeline project",
        MonitoringProjectDashboardParameter(),
        backends=["monitoring"],
    )
    dashboard.add_parameter(
        "cluster",
        "Cluster",
        MonitoringLabelDashboardParameter(
            project_id="yt",
            label_key="cluster",
            default_value="-",
            selectors='{sensor="yt.build.version"}',
        ),
        backends=["monitoring"],
    )

    dashboard.add_parameter(
        "pipeline_cluster",
        "Pipeline cluster",
        MonitoringLabelDashboardParameter(
            project_id="yt",
            label_key="pipeline_cluster",
            default_value="-",
            selectors='{sensor="yt.build.version"}',
        ),
        backends=["monitoring"],
    )

    dashboard.add_parameter(
        "pipeline_path",
        "Pipeline path",
        MonitoringLabelDashboardParameter(
            project_id="yt",
            label_key="pipeline_path",
            default_value="-",
            selectors='{sensor="yt.build.version"}',
        ),
        backends=["monitoring"],
    )

    for name, title in (
        ("project", "Pipeline project"),
        ("cluster", "Cluster"),
        ("pipeline_cluster", "Pipeline cluster"),
        ("pipeline_path", "Pipeline path"),
    ):
        dashboard.add_parameter(
            name,
            title,
            GrafanaTextboxDashboardParameter(".*"),
            backends=["grafana"],
        )


def create_dashboard(short_name: str, filler: Callable[Dashboard, None], worker_host_aggr: bool = True, backend: str = "monitoring"):
    d = Dashboard()
    d.set_title(f"[YT Flow] {DASHBOARDS_META[short_name].title}")
    d.set_monitoring_links(build_dashboard_links(short_name))
    add_common_dashboard_parameters(d)

    d.add(build_versions(worker_host_aggr=worker_host_aggr, backend=backend))

    filler(d)

    d.value("project", TemplateTag("project"))
    d.value("cluster", TemplateTag("cluster"))
    d.value("pipeline_cluster", TemplateTag("pipeline_cluster"))
    d.value("pipeline_path", TemplateTag("pipeline_path"))

    return d
