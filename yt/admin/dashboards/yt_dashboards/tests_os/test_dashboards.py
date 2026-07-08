from yt.admin.dashboards.yt_dashboards.testlib import canonize_dashboard, grafana_serializer

import yt_dashboards.master as master

from yt_dashboards.cluster_resources import build_cluster_resources
from yt_dashboards.scheduler_internal import build_scheduler_internal
from yt_dashboards.scheduler_gpu import build_scheduler_gpu
from yt_dashboards.scheduler_pool import build_scheduler_pool
from yt_dashboards.scheduler_operation import build_scheduler_operation
from yt_dashboards.queue_and_consumer_metrics import build_queue_metrics, build_queue_consumer_metrics
from yt_dashboards.http_proxies import build_http_proxies

import yt_dashboards.bundle_ui.bundle_ui_dashboard as bundle_ui_dashboard


def test_bundle_ui_cpu():
    return canonize_dashboard(
        bundle_ui_dashboard.build_bundle_ui_cpu(False), grafana_serializer(), "bundle-ui-cpu.json"
    )


def test_bundle_ui_disk():
    return canonize_dashboard(bundle_ui_dashboard.build_bundle_ui_disk(), grafana_serializer(), "bundle-ui-disk.json")


def test_bundle_ui_efficiency():
    return canonize_dashboard(
        bundle_ui_dashboard.build_bundle_ui_efficiency(), grafana_serializer(), "bundle-ui-efficiency.json"
    )


def test_bundle_ui_lsm():
    return canonize_dashboard(bundle_ui_dashboard.build_bundle_ui_lsm(), grafana_serializer(), "bundle-ui-lsm.json")


def test_bundle_ui_memory():
    return canonize_dashboard(
        bundle_ui_dashboard.build_bundle_ui_memory(False), grafana_serializer(), "bundle-ui-memory.json"
    )


def test_bundle_ui_network():
    return canonize_dashboard(
        bundle_ui_dashboard.build_bundle_ui_network(), grafana_serializer(), "bundle-ui-network.json"
    )


def test_bundle_ui_resource():
    return canonize_dashboard(
        bundle_ui_dashboard.build_bundle_ui_resource_overview(False), grafana_serializer(), "bundle-ui-resource.json"
    )


def test_bundle_ui_rpc_proxy_overview():
    return canonize_dashboard(
        bundle_ui_dashboard.build_bundle_ui_rpc_resource_overview(),
        grafana_serializer(),
        "bundle-ui-rpc-proxy-overview.json",
    )


def test_bundle_ui_rpc_proxy():
    return canonize_dashboard(
        bundle_ui_dashboard.build_bundle_rpc_proxy_dashboard(False), grafana_serializer(), "bundle-ui-rpc-proxy.json"
    )


def test_bundle_ui_user_load():
    return canonize_dashboard(
        bundle_ui_dashboard.build_bundle_ui_user_load(), grafana_serializer(), "bundle-ui-user-load.json"
    )


def test_cluster_resources():
    return canonize_dashboard(build_cluster_resources(False), grafana_serializer(), "cluster-resources.json")


def test_http_proxies():
    return canonize_dashboard(build_http_proxies(False), grafana_serializer(), "http-proxies.json")


def test_master_accounts():
    return canonize_dashboard(master.build_master_accounts(), grafana_serializer(), "master-accounts.json")


def test_master_global():
    return canonize_dashboard(master.build_master_global(False, False), grafana_serializer(), "master-global.json")


def test_master_local():
    return canonize_dashboard(master.build_master_local(), grafana_serializer(), "master-local.json")


def test_queue_consumer_metrics():
    return canonize_dashboard(
        build_queue_consumer_metrics("grafana"), grafana_serializer(), "queue-consumer-metrics.json"
    )


def test_queue_metrics():
    return canonize_dashboard(build_queue_metrics("grafana"), grafana_serializer(), "queue-metrics.json")


def test_scheduler_internal():
    return canonize_dashboard(
        build_scheduler_internal(False, "grafana"), grafana_serializer(), "scheduler-internal.json"
    )


def test_scheduler_gpu():
    return canonize_dashboard(build_scheduler_gpu(False, "grafana"), grafana_serializer(), "scheduler-gpu.json")


def test_scheduler_operation():
    return canonize_dashboard(build_scheduler_operation(False), grafana_serializer(), "scheduler-operation.json")


def test_scheduler_pool():
    return canonize_dashboard(build_scheduler_pool(), grafana_serializer(), "scheduler-pool.json")
