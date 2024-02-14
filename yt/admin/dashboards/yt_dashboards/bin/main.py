from yt_dashboards.common.runner import run

from yt_dashboards.master import (
    build_master_global, build_master_local, build_master_merge_jobs)
from yt_dashboards.artemis import (
    build_local_artemis, build_bundle_artemis, build_global_artemis, build_local_artemis_container)
from yt_dashboards.scheduler_internal import build_scheduler_internal
from yt_dashboards.scheduler_pool import build_scheduler_pool
from yt_dashboards.cluster_resources import build_cluster_resources
from yt_dashboards.cache import build_cache_with_ghosts
from yt_dashboards.chyt import build_chyt_monitoring
from yt_dashboards.key_filter import build_key_filter

from yt_dashboards.bundle_ui_dashboard import (
    build_bundle_ui_user_load, build_bundle_ui_lsm, build_bundle_ui_rpc_resource_overview,
    build_bundle_ui_cpu, build_bundle_ui_downtime, build_bundle_ui_memory, build_bundle_ui_network, build_bundle_rpc_proxy_dashboard,
    build_bundle_ui_disk, build_bundle_ui_resource_overview, build_bundle_ui_efficiency, build_bundle_capacity_planning,
    build_bundle_ui_key_filter)

from yt_dashboards import lsm


dashboards = {
    "cache": {
        "func": build_cache_with_ghosts,
        "monitoring": {},
    },
    "artemis-local-container": {
        "func": build_local_artemis_container,
        "monitoring": {},
    },
    "artemis": {
        "func": build_global_artemis,
        "monitoring": {},
    },
    "artemis-local": {
        "func": build_local_artemis,
        "monitoring": {},
        "grafana": {},
    },
    "artemis-bundle": {
        "func": build_bundle_artemis,
        "monitoring": {},
    },
    "scheduler-internal": {
        "func": lambda has_porto=True: build_scheduler_internal(has_porto),
        "monitoring": {
            "args": [True],
        },
        "grafana": {
            "args": [False],
        },
    },
    "scheduler-pool": {
        "func": build_scheduler_pool,
        "monitoring": {},
        "grafana": {},
    },
    "cluster-resources": {
        "func": build_cluster_resources,
        "monitoring": {},
    },
    "per-table-compaction": {
        "func": lsm.build_per_table_compaction,
        "monitoring": {},
    },
    "bundle-ui-user-load": {
        "func": build_bundle_ui_user_load,
        "monitoring": {},
    },
    "bundle-ui-lsm": {
        "func": build_bundle_ui_lsm,
        "monitoring": {},
    },
    "bundle-ui-cpu": {
        "func": build_bundle_ui_cpu,
        "monitoring": {},
    },
    "bundle-ui-downtime": {
        "func": build_bundle_ui_downtime,
        "monitoring": {},
    },
    "bundle-ui-memory": {
        "func": build_bundle_ui_memory,
        "monitoring": {},
    },
    "bundle-ui-network": {
        "func": build_bundle_ui_network,
        "monitoring": {},
    },
    "bundle-ui-disk": {
        "func": build_bundle_ui_disk,
        "monitoring": {},
    },
    "bundle-ui-resource": {
        "func": build_bundle_ui_resource_overview,
        "monitoring": {},
    },
    "bundle-ui-efficiency": {
        "func": build_bundle_ui_efficiency,
        "monitoring": {},
    },
    "bundle-capacity-planning": {
        "func": build_bundle_capacity_planning,
        "monitoring": {},
    },
    "bundle-ui-rpc-proxy-overview": {
        "func": build_bundle_ui_rpc_resource_overview,
        "monitoring": {},
    },
    "bundle-ui-rpc-proxy": {
        "func": build_bundle_rpc_proxy_dashboard,
        "monitoring": {},
    },
    "bundle-ui-key-filter": {
        "func": build_bundle_ui_key_filter,
        "monitoring": {},
    },
    "key-filter": {
        "func": build_key_filter,
        "monitoring": {},
    },
    "master-global": {
        "func": build_master_global,
        "monitoring": {},
    },
    "master-local": {
        "func": build_master_local,
        "monitoring": {},
        "grafana": {},
    },
    "master-merge-jobs": {
        "func": build_master_merge_jobs,
        "monitoring": {},
    },
    "chyt-monitoring-test": {
        "func": build_chyt_monitoring,
        "monitoring": {},
    },
    "chyt-monitoring": {
        "func": build_chyt_monitoring,
        "monitoring": {},
    },
}


def main():
    run(dashboards)


if __name__ == "__main__":
    main()
