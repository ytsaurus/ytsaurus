# flake8: noqa

from .common.sensors import *

try:
    from .constants import (
        CYPRESS_PROXIES_DASHBOARD_DEFAULT_CLUSTER,
        CYPRESS_PROXIES_DASHBOARD_DEFAULT_HOST,
    )
except ImportError:
    from .yandex_constants import (
        CYPRESS_PROXIES_DASHBOARD_DEFAULT_CLUSTER,
        CYPRESS_PROXIES_DASHBOARD_DEFAULT_HOST,
    )

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter
from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
from yt_dashboard_generator.sensor import Title


def add_cells(rowset: Rowset, cell_args: list[tuple[str, Sensor]]) -> None:
    for i, args in enumerate(cell_args):
        if i % 2 == 0:
            row = rowset.row()
        row.cell(*args)


def list_common_cell_args() -> list[tuple[str, Sensor]]:
    return [(
        "Total Cpu By Thread",
        (CypressProxyCpu("yt.resource_tracker.total_cpu")
         .all("thread"))
    )]


def list_per_queue_cell_args() -> list[tuple[str, Sensor]]:
    return [(
        "Execute Request Rate",
        (CypressProxyRpc("yt.rpc.server.request_count.rate")
         .value("yt_service", "ObjectService")
         .value("method", "Execute")
         .stack(False))
    ), (
        "Execute Concurrency",
        (CypressProxyRpc("yt.rpc.server.concurrency")
         .value("yt_service", "ObjectService")
         .value("method", "Execute")
         .stack(False))
    ), (
        "Execute Failed Request Rate",
        (CypressProxyRpc("yt.rpc.server.failed_request_count.rate")
         .aggr("user")
         .value("yt_service", "ObjectService")
         .value("method", "Execute")
         .stack(False))
    ), (
        "Execute Request Queue Size",
        (CypressProxyRpc("yt.rpc.server.request_queue_size")
         .value("yt_service", "ObjectService")
         .value("method", "Execute")
         .stack(False))
    ), (
        "Execute Timed Out Request Rate",
        (CypressProxyRpc("yt.rpc.server.timed_out_request_count.rate")
         .aggr("user")
         .value("yt_service", "ObjectService")
         .value("method", "Execute")
         .stack(False))
    )]


def build_cypress_proxies_all() -> Rowset:
    rowset = Rowset().top()

    cell_args = list_common_cell_args()
    cell_args.extend([(
        "Memory",
        (CypressProxyMemory("yt.resource_tracker.memory_usage.rss")
         .stack(False))
    ), (
        "RpcLight Utilization",
        (CypressProxyCpu("yt.resource_tracker.utilization")
         .value("thread", "RpcLight")
         .stack(False))
    ), (
        "RpcHeavy Utilization",
        (CypressProxyCpu("yt.resource_tracker.utilization")
         .value("thread", "RpcHeavy")
         .stack(False))
    ), (
       "RCT Objects Alive",
        (CypressProxyMemory("yt.ref_counted_tracker.total.objects_alive")
         .stack(False))
    ), (
       "RCT Bytes Alive",
        (CypressProxyMemory("yt.ref_counted_tracker.total.bytes_alive")
         .stack(False))
    ), (
        "Execute Request Time",
        (CypressProxyRpc("yt.rpc.server.request_time.total.max")
        .value("yt_service", "ObjectService")
        .value("method", "Execute")
        .all("queue"))
    )])

    cell_args.extend(
        (a[0], a[1].aggr("queue"))
        for a in list_per_queue_cell_args())

    add_cells(rowset, cell_args)
    return rowset


def build_cypress_proxies_aggr() -> Rowset:
    rowset = Rowset().top()

    cell_args = list_common_cell_args()
    cell_args.extend(
        (a[0], a[1].all("queue"))
        for a in list_per_queue_cell_args())

    add_cells(rowset, cell_args)
    return rowset


def add_parameters(
    dashboard: Dashboard,
    default_cluster: str,
    default_host: str,
) -> None:
    dashboard.add_parameter(
        "cluster",
        "Cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", default_cluster),
        backends=["monitoring"])
    dashboard.add_parameter(
        "cluster",
        "Cluster",
        GrafanaTextboxDashboardParameter(default_cluster),
        backends=["grafana"])

    dashboard.add_parameter(
        "host",
        "Host",
        MonitoringLabelDashboardParameter("yt", "host", default_host),
        backends=["monitoring"])
    dashboard.add_parameter(
        "host",
        "Host",
        GrafanaTextboxDashboardParameter(default_host),
        backends=["grafana"])


def build_cypress_proxies() -> Dashboard:
    dashboard = Dashboard()

    rowsets = [
        (build_cypress_proxies_all()
            .value("host", TemplateTag("host"))),
        (Rowset()
            .row(height=2)
            .cell("", Title("Aggregated By Host", size="TITLE_SIZE_L"))),
        (build_cypress_proxies_aggr()
            .aggr("host")),
    ]

    for rowset in rowsets:
        dashboard.add(rowset)

    dashboard.set_title("Cypress Proxies [Autogenerated]")
    add_parameters(
        dashboard,
        CYPRESS_PROXIES_DASHBOARD_DEFAULT_CLUSTER,
        CYPRESS_PROXIES_DASHBOARD_DEFAULT_HOST)

    dashboard.value("cluster", TemplateTag("cluster"))

    return dashboard
