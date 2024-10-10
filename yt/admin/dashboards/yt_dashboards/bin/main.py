from yt_dashboards.common.runner import run
from yt_dashboards.common.postprocessors import combine_postprocessors, AddTagPostprocessor, MapTagPostprocessor
from yt_dashboards.common.sensors import GrafanaTag, GrafanaServiceTag

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
from yt_dashboards.exe_nodes import build_exe_nodes

from yt_dashboards.bundle_ui_dashboard import (
    build_bundle_ui_user_load, build_bundle_ui_lsm, build_bundle_ui_rpc_resource_overview,
    build_bundle_ui_cpu, build_bundle_ui_downtime, build_bundle_ui_memory, build_bundle_ui_network, build_bundle_rpc_proxy_dashboard,
    build_bundle_ui_disk, build_bundle_ui_resource_overview, build_bundle_ui_efficiency, build_bundle_capacity_planning,
    build_bundle_ui_key_filter)

from yt_dashboards import lsm

from yt_dashboards import flow

# This is an extensive list of all available dashboards.
# You should create a similar configuration file with your configuration specifics
# and maybe even custom dashboards.
#
# Not all dashboards have been optimized for Grafana usage.
# Let's make a common effort to label supported backends for each dashboard with a comment.
#
# NB: Grafana installations can be supported by different backends. We aim to be as generic as
# possible, but it is hard to test anything than your own installation, so some specifics may
# creep in. We welcome issues and pull requests!
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
    # Supported backends: monitoring, grafana.
    "scheduler-pool": {
        "func": build_scheduler_pool,
        "monitoring": {},
        "grafana": {
            "id": "<your-dashboard-id>",
            "postprocessor": combine_postprocessors(
                # This postprocessor can be used to add tags to all sensors within a dashboard, which might be useful for your Grafana installation.
                AddTagPostprocessor(extra_tags={"__workspace__": "yt", "__bucket__": "<your-cluster-name>"}, mode="prepend"),
                # This postprocessor can be used to relabel tags. It is useful for changing the service label depending on your k8s installation.
                # For now the most relevant tag set by the k8s operator is yt_component (looks like <cluster-name>-yt-schedulers[-instance-group-name]),
                # or whatever the label of your scraping job is. We are working on improving this from the operator perspective.
                # You can also configure solomon_exporter to export your favorite style of component tags manually.
                MapTagPostprocessor(old_key=GrafanaServiceTag, new_key=GrafanaTag("<your-component-tag>"), value_mapping={"yt-scheduler": "<new-value-for-scheduler>"}))
        },
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
    "exe-nodes": {
        "func": build_exe_nodes,
        "monitoring": {},
        "grafana": {},
    },
    "flow-general": {
        "func": flow.build_pipeline,
        "monitoring": {},
    }
}


def main():
    run(dashboards)


if __name__ == "__main__":
    main()
