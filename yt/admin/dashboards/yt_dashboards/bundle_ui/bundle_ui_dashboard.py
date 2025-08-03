# flake8: noqa
from yt_dashboard_generator.dashboard import Dashboard
from yt_dashboard_generator.specific_tags.tags import TemplateTag

from yt_dashboard_generator.backends.monitoring import (
    MonitoringTag, MonitoringLabelDashboardParameter)

from .user_load import build_max_lookup_select_execute_time_per_host, build_user_load
from .lsm import build_user_lsm
from .cpu import build_user_thread_cpu, build_total_cpu
from .maintenance import build_user_hydra, build_tablet_balancer, build_bundle_controller
from .memory import build_user_memory, build_reserved_memory
from .network import (
    build_tablet_network, build_user_network, build_throttling, build_rpc_request_rate,
    build_lookup_select_ack_time, build_rpc_message_size_stats_per_host)
from .disk import (
    build_user_disk, build_user_background_disk, build_user_caches,
    build_block_cache_planning)
from .efficiency import build_efficiency_rowset
from .resources import build_user_resource_overview_rowset
from .proxy_resources import build_rpc_proxy_resource_overview_rowset
from .proxy_details import (
    build_rpc_proxy_cpu, build_rpc_proxy_network, build_rpc_proxy_rpc_request_rate,
    build_rpc_proxy_rpc_request_wait, build_rpc_proxy_maintenance)
from .planning import (
    build_node_resource_capacity_planning,
    build_rpc_proxy_resource_capacity_planning,
    build_tablet_static_planning)

from ..key_filter import build_key_filter_rowset
from .key_filter import build_bundle_key_filter_rowset

from ..common.sensors import *


try:
    from .constants import BUNDLE_UI_DASHBOARD_DEFAULT_CLUSTER
except ImportError:
    BUNDLE_UI_DASHBOARD_DEFAULT_CLUSTER = ""

##################################################################


def build_dashboard(rowsets, bundle=False, role=False, host=False, only_parameters=False):
    d = Dashboard()
    for r in rowsets:
        d.add(r)

    d.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", BUNDLE_UI_DASHBOARD_DEFAULT_CLUSTER))

    if bundle:
        if not only_parameters:
            d = d.value("tablet_cell_bundle", TemplateTag("tablet_cell_bundle"))
        d.add_parameter(
            "tablet_cell_bundle",
            "Tablet cell bundle",
            MonitoringLabelDashboardParameter("yt", "tablet_cell_bundle", "default"))

    if role:
        if not only_parameters:
            d = d.value("proxy_role", TemplateTag("proxy_role"))
        d.add_parameter(
            "proxy_role",
            "Proxy role",
            MonitoringLabelDashboardParameter("yt", "proxy_role", "default"))

    if host:
        if not only_parameters:
            d = d.value(MonitoringTag("host"), TemplateTag("host"))
        d.add_parameter(
            "host",
            "Host",
            MonitoringLabelDashboardParameter("yt", "host", "Aggr"))

    return d


def build_bundle_dashboard(rowsets, *, host=False):
    return build_dashboard(rowsets, bundle=True, host=host)


def build_rpc_proxy_dashboard(rowsets, *, host=False):
    return build_dashboard(rowsets, role=True, host=host)


##################################################################

def build_bundle_ui_user_load():
    rowsets = [
        build_max_lookup_select_execute_time_per_host().aggr("#U").all(MonitoringTag("host")),
        build_user_load(),
    ]
    return build_bundle_dashboard(rowsets)


def build_bundle_ui_lsm():
    rowsets = [
        build_user_lsm(),
    ]
    return build_bundle_dashboard(rowsets)


def build_bundle_ui_downtime():
    rowsets = [
        build_user_hydra(),
        build_tablet_balancer(),
        build_bundle_controller(),
    ]
    return build_bundle_dashboard(rowsets)


def build_bundle_ui_cpu():
    rowsets = [
        build_total_cpu(),
        build_user_thread_cpu(),
    ]
    return build_bundle_dashboard(rowsets, host=True)


def build_bundle_ui_memory():
    rowsets = [
        build_user_memory(),
        build_reserved_memory()
    ]
    return build_bundle_dashboard(rowsets, host=True)


def build_bundle_ui_network():
    rowsets = [
        build_user_network(),
        build_throttling(),
        build_rpc_request_rate(),
        build_tablet_network(),
        # build_user_bus_cpu(),
        build_lookup_select_ack_time(),
        build_rpc_message_size_stats_per_host(
            TabNodeRpcClient, "client", "Rpc client (yt_node)"),
        build_rpc_message_size_stats_per_host(
            TabNodeRpc, "server", "Rpc server (yt_node)"),
    ]
    return build_bundle_dashboard(rowsets, host=True)


def build_bundle_ui_disk():
    rowsets = [
        build_user_disk(),
        build_user_background_disk(),
        build_user_caches(),
        build_block_cache_planning()
    ]
    return build_bundle_dashboard(rowsets, host=True)


def build_bundle_ui_resource_overview():
    rowsets = [
        build_user_resource_overview_rowset(),
    ]
    return build_bundle_dashboard(rowsets)


def build_bundle_ui_rpc_resource_overview():
    rowsets = [
        build_rpc_proxy_resource_overview_rowset(),
    ]
    return build_rpc_proxy_dashboard(rowsets)


def build_bundle_rpc_proxy_dashboard():
    rowsets = [
        build_rpc_proxy_cpu(),
        build_rpc_proxy_network(),
        build_rpc_proxy_rpc_request_rate(),
        build_rpc_proxy_rpc_request_wait(),
        build_rpc_proxy_maintenance(),
    ]
    return build_rpc_proxy_dashboard(rowsets, host=True)


def build_bundle_ui_efficiency():
    rowsets = [
        build_efficiency_rowset(),
    ]
    return build_dashboard(rowsets, bundle=True, only_parameters=True)


def build_bundle_capacity_planning():
    rowsets = [
        build_tablet_static_planning(),
        build_node_resource_capacity_planning(),
        build_block_cache_planning(),
        build_rpc_proxy_resource_capacity_planning(),
    ]

    return build_dashboard(rowsets, bundle=True, role=True, only_parameters=True)


def build_bundle_ui_key_filter():
    rowsets = [
        build_key_filter_rowset(),
        build_bundle_key_filter_rowset(),
    ]
    return (build_bundle_dashboard(rowsets)
        .aggr("user", "table_path", "table_tag", MonitoringTag("host")))
