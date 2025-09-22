# flake8: noqa

try:
    from yt_dashboards.constants import (
        HTTP_PROXIES_DASHBOARD_DEFAULT_CLUSTER,
        HTTP_PROXIES_DASHBOARD_DEFAULT_PROXY_ROLE,
        HTTP_PROXIES_DASHBOARD_DEFAULT_HOST,
    )
except ImportError:
    from yt_dashboards.yandex_constants import (
        HTTP_PROXIES_DASHBOARD_DEFAULT_CLUSTER,
        HTTP_PROXIES_DASHBOARD_DEFAULT_PROXY_ROLE,
        HTTP_PROXIES_DASHBOARD_DEFAULT_HOST,
    )


from yt_dashboard_generator.backends.grafana import GrafanaTextboxDashboardParameter
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter
from yt_dashboard_generator.specific_sensors.monitoring import MonitoringExpr
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import MultiSensor, Title, Text

from .common.sensors import *


def add_proxy_parameters(
    dashboard: Dashboard,
    default_cluster: str = HTTP_PROXIES_DASHBOARD_DEFAULT_CLUSTER,
    default_proxy_role: str = HTTP_PROXIES_DASHBOARD_DEFAULT_PROXY_ROLE,
    default_host: str = HTTP_PROXIES_DASHBOARD_DEFAULT_HOST,
):
    # cluster
    dashboard.add_parameter(
        "cluster",
        "Cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", default_cluster),
        backends=["monitoring"],
    )
    dashboard.add_parameter(
        "cluster",
        "Cluster",
        GrafanaTextboxDashboardParameter(default_cluster),
        backends=["grafana"],
    )

    # proxy_role
    dashboard.add_parameter(
        "proxy_role",
        "Proxy Role",
        MonitoringLabelDashboardParameter("yt", "proxy_role", default_proxy_role),
        backends=["monitoring"],
    )
    dashboard.add_parameter(
        "proxy_role",
        "Proxy Role",
        GrafanaTextboxDashboardParameter(default_proxy_role),
        backends=["grafana"],
    )

    # host
    dashboard.add_parameter(
        "host",
        "Host",
        MonitoringLabelDashboardParameter("yt", "host", default_host),
        backends=["monitoring"],
    )
    dashboard.add_parameter(
        "host",
        "Host",
        GrafanaTextboxDashboardParameter(default_host),
        backends=["grafana"],
    )

    dashboard.value("cluster", TemplateTag("cluster"))
    dashboard.value("proxy_role", TemplateTag("proxy_role"))
    dashboard.value("host", TemplateTag("host"))


def build_http_proxies_rowsets(has_porto: bool):
    rowsets = []

    rowsets.append(Rowset()
        .row(height=2)
        .cell("", Title("Memory"))
    )

    if has_porto:
        rowsets.append(Rowset()
            .value("container_category", "pod")
            .row(height=12)
                .cell(
                    "OOM kills",
                    MonitoringExpr(HttpProxyPorto("yt.porto.memory.oom_kills_total")).diff()
                )
                .cell(
                    "Memory Porto",
                    MultiSensor(
                        HttpProxyPorto("yt.porto.memory.anon_usage"),
                        HttpProxyPorto("yt.porto.memory.anon_limit"),
                    )
                ))

    rowsets.append(Rowset()
        .row(height=12)
            .cell(
                "Memory By User (tracker)",
                MonitoringExpr(HttpProxy("yt.http_proxy.memory_usage.pool_used")).aggr("category").all("pool").top_max(10)
            )
            .cell(
                "Memory By User (allocator)",
                MonitoringExpr(HttpProxyMemory("yt.memory.heap_usage.user")).all("user").top_max(10)
            )
        .row(height=12)
            .cell(
                "Memory By Category",
                MonitoringExpr(HttpProxy("yt.http_proxy.memory_usage.used")).all("category")
            )
            .cell(
                "Memory By Command (allocator)",
                MonitoringExpr(HttpProxyMemory("yt.memory.heap_usage.command")).all("command")
            )
        .row(height=12)
            .cell(
                "Memory Total (tracker)",
                MultiSensor(
                    HttpProxy("yt.http_proxy.memory_usage.used").aggr("category"),
                    HttpProxy("yt.http_proxy.memory_usage.limit").value("category", "heavy_request"),
                )
            )
        )

    rowsets.append(Rowset()
        .row(height=2)
        .cell("", Title("CPU"))
    )

    if has_porto:
        rowsets.append(Rowset()
            .value("container_category", "pod")
            .row(height=12)
                .cell(
                    "CPU Porto",
                    MultiSensor(
                        HttpProxyPorto("yt.porto.cpu.user"),
                        HttpProxyPorto("yt.porto.cpu.system"),
                        HttpProxyPorto("yt.porto.cpu.limit"),
                        HttpProxyPorto("yt.porto.cpu.total"),
                    )
                )
            )

    rowsets.append(Rowset()
        .row(height=12)
            .cell(
                "CPU Resource Tracker",
                MultiSensor(
                    HttpProxyCpu("yt.resource_tracker.user_cpu").aggr("thread"),
                    HttpProxyCpu("yt.resource_tracker.system_cpu").aggr("thread"),
                    HttpProxyCpu("yt.resource_tracker.total_cpu").aggr("thread"),
                )
            )
            .cell(
                "CPU By Threads",
                MonitoringExpr(HttpProxyCpu("yt.resource_tracker.user_cpu|yt.resource_tracker.system_cpu").all("thread")).top_max(10)
            )
        )

    rowsets.append(Rowset()
        .row(height=2)
        .cell("", Title("Requests"))
    )

    rowsets.append(Rowset()
        .top(10)
        .row(height=12)
            .cell(
                "Top Users By Request Rate",
                MonitoringExpr(HttpProxy("yt.http_proxy.request_count.rate").aggr("command").all("user"))
            )
            .cell(
                "Top Commands By Request Rate",
                MonitoringExpr(HttpProxy("yt.http_proxy.request_count.rate").all("command").aggr("user"))
            )
        .row(height=12)
            .cell(
                "Top Users By Error Count",
                MonitoringExpr(HttpProxy("yt.http_proxy.api_error_count.rate").aggr("command").all("user"))
            )
            .cell(
                "Top Commands By Error Count",
                MonitoringExpr(HttpProxy("yt.http_proxy.api_error_count.rate").all("command").aggr("user"))
            )
        .row(height=12)
            .cell(
                "Top Users By Concurrency Semaphore",
                MonitoringExpr(HttpProxy("yt.http_proxy.concurrency_semaphore").aggr("command").all("user"))
            )
            .cell(
                "Top Users By Request Cpu Time",
                MonitoringExpr(HttpProxy("yt.http_proxy.cumulative_request_cpu_time.rate").aggr("command").all("user"))
            )
        .row(height=12)
            .cell(
                "Top Users By Bytes In Rate",
                MonitoringExpr(HttpProxy("yt.http_proxy.bytes_in.rate").aggr("command").all("user").aggr("network"))
            )
            .cell(
                "Top Users By Bytes Out Rate",
                MonitoringExpr(HttpProxy("yt.http_proxy.bytes_out.rate").aggr("command").all("user").aggr("network"))
            )
        )

    rowsets.append(Rowset()
        .row(height=2)
        .cell("", Title("RPC client"))
    )

    rowsets.append(Rowset()
        .all("method")
        .top(10)
        .row(height=12)
            .cell(
                "RPC Client Request Count",
                MonitoringExpr(HttpProxyRpcClient("yt.rpc.client.request_count.rate"))
            )
            .cell(
                "RPC Client Failed Request Count",
                MonitoringExpr(HttpProxyRpcClient("yt.rpc.client.failed_request_count.rate"))
            )
        .row(height=12)
            .cell(
                "RPC Client Timed Out Request Count",
                MonitoringExpr(HttpProxyRpcClient("yt.rpc.client.timed_out_request_count.rate"))
            )
    )

    return rowsets


def build_http_proxies(has_porto: bool = True):
    dashboard = Dashboard()

    for rowset in build_http_proxies_rowsets(has_porto):
        dashboard.add(rowset)

    dashboard.set_title("HTTP proxies [Autogenerated]")
    dashboard.set_description("")
    add_proxy_parameters(
        dashboard,
        HTTP_PROXIES_DASHBOARD_DEFAULT_CLUSTER,
        HTTP_PROXIES_DASHBOARD_DEFAULT_PROXY_ROLE,
        HTTP_PROXIES_DASHBOARD_DEFAULT_HOST,
    )

    return dashboard
