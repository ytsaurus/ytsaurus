import logging

from yt_dashboards.common.runner import run

from yt_dashboards.master import (
    build_master_global, build_master_local, build_master_node_tracker, build_master_cpu, build_master_merge_jobs,
    build_master_accounts)
from yt_dashboards.cypress_proxies import build_cypress_proxies
from yt_dashboards.artemis import (
    build_local_artemis, build_bundle_artemis, build_global_artemis, build_local_artemis_container)
from yt_dashboards.scheduler_internal import build_scheduler_internal
from yt_dashboards.scheduler_pool import build_scheduler_pool
from yt_dashboards.scheduler_operation import build_scheduler_operation
from yt_dashboards.jobs_monitor import build_jobs_monitor
from yt_dashboards.cluster_resources import build_cluster_resources
from yt_dashboards.cache import build_cache_with_ghosts
from yt_dashboards.chyt import build_chyt_monitoring
from yt_dashboards.key_filter import build_key_filter
from yt_dashboards.exe_nodes import build_exe_nodes
from yt_dashboards.data_nodes import build_data_nodes_common
from yt_dashboards.data_node_local import build_data_node_local
from yt_dashboards.user_load import build_user_load
from yt_dashboards.http_proxies import build_http_proxies

from yt_dashboards.bundle_ui import (
    build_bundle_ui_user_load, build_bundle_ui_lsm, build_bundle_ui_rpc_resource_overview,
    build_bundle_ui_cpu, build_bundle_ui_downtime, build_bundle_ui_memory, build_bundle_ui_network, build_bundle_rpc_proxy_dashboard,
    build_bundle_ui_disk, build_bundle_ui_resource_overview, build_bundle_ui_efficiency, build_bundle_capacity_planning,
    build_bundle_ui_key_filter)

from yt_dashboards import lsm

from yt_dashboards import flow

from yt_dashboards import queue_and_consumer_metrics


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


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
        "func": build_scheduler_internal,
        "monitoring": {
            "args": [True, "monitoring"],
        },
        "grafana": {
            "args": [False, "grafana"],
        },
    },
    "scheduler-pool": {
        "func": build_scheduler_pool,
        "monitoring": {},
        "grafana": {},
    },
    "scheduler-operation": {
        "func": build_scheduler_operation,
        "monitoring": {
            "args": [True],
        },
        "grafana": {
            "args": [False],
        },
    },
    "jobs-monitor": {
        "func": lambda: build_jobs_monitor(mode="plain"),
        "monitoring": {},
    },
    "jobs-monitor-top-and-bottom": {
        "func": lambda: build_jobs_monitor(mode="top_and_bottom"),
        "monitoring": {},
    },
    "http-proxies": {
        "func": lambda has_porto=True: build_http_proxies(has_porto),
        "monitoring": {
            "args": [True],
        },
        "grafana": {
            "args": [False]
        }
    },
    "cluster-resources": {
        "func": build_cluster_resources,
        "monitoring": {
            "args": [True],
        },
        "grafana": {
            "args": [False],
        },
    },
    "per-table-compaction": {
        "func": lsm.build_per_table_compaction,
        "monitoring": {},
    },
    "bundle-ui-user-load": {
        "func": build_bundle_ui_user_load,
        "monitoring": {},
        "grafana": {},
    },
    "bundle-ui-lsm": {
        "func": build_bundle_ui_lsm,
        "monitoring": {},
        "grafana": {},
    },
    "bundle-ui-cpu": {
        "func": build_bundle_ui_cpu,
        "monitoring": {
            "args": [True],
        },
        "grafana": {
            "args": [False],
        },
    },
    "bundle-ui-downtime": {
        "func": build_bundle_ui_downtime,
        "monitoring": {},
    },
    "bundle-ui-memory": {
        "func": build_bundle_ui_memory,
        "monitoring": {
            "args": [True]
        },
        "grafana": {
            "args": [False]
        },
    },
    "bundle-ui-network": {
        "func": build_bundle_ui_network,
        "monitoring": {},
        "grafana": {},
    },
    "bundle-ui-disk": {
        "func": build_bundle_ui_disk,
        "monitoring": {},
        "grafana": {},
    },
    "bundle-ui-resource": {
        "func": lambda has_porto=True: build_bundle_ui_resource_overview(has_porto),
        "monitoring": {
            "args": [True],
        },
        "grafana": {
            "args": [False],
        },
    },
    "bundle-ui-efficiency": {
        "func": build_bundle_ui_efficiency,
        "monitoring": {},
        "grafana": {},
    },
    "bundle-capacity-planning": {
        "func": build_bundle_capacity_planning,
        "monitoring": {},
    },
    "bundle-ui-rpc-proxy-overview": {
        "func": build_bundle_ui_rpc_resource_overview,
        "monitoring": {},
        "grafana": {},
    },
    "bundle-ui-rpc-proxy": {
        "func": build_bundle_rpc_proxy_dashboard,
        "monitoring": {
            "args": [True],
        },
        "grafana": {
            "args": [False]
        },
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
        "monitoring": {
            "args": [True, True],
        },
        "grafana": {
            "args": [False, False],
        },
    },
    "master-local": {
        "func": build_master_local,
        "monitoring": {},
        "grafana": {},
    },
    "master-node-tracker": {
        "func": build_master_node_tracker,
        "monitoring": {},
    },
    "master-cpu": {
        "func": build_master_cpu,
        "monitoring": {},
    },
    "master-merge-jobs": {
        "func": build_master_merge_jobs,
        "monitoring": {},
        "grafana": {},
    },
    "master-accounts": {
        "func": build_master_accounts,
        "monitoring": {},
        "grafana": {},
    },
    "cypress-proxy": {
        "func": build_cypress_proxies,
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
    "data-nodes-common": {
        "func": build_data_nodes_common,
        "monitoring": {},
    },
    "data-node-local": {
        "func": build_data_node_local,
        "monitoring": {
            # has_porto = True
            "args": [True],
        },
        "grafana": {
            # has_porto = False
            "args": [False],
        },
    },
    "flow-general": {
        "func": flow.build_flow_general,
        "monitoring": {},
        "grafana": {},
    },
    "flow-diagnostics": {
        "func": flow.build_flow_diagnostics,
        "monitoring": {},
    },
    "flow-event-time": {
        "func": flow.build_flow_event_time,
        "monitoring": {},
    },
    "flow-controller": {
        "func": flow.build_flow_controller,
        "monitoring": {},
    },
    "flow-worker": {
        "func": flow.build_flow_worker,
        "monitoring": {},
    },
    "flow-computation": {
        "func": flow.build_flow_computation,
        "monitoring": {},
    },
    "flow-message-transfering": {
        "func": flow.build_flow_message_transfering,
        "monitoring": {},
    },
    "flow-one-worker": {
        "func": flow.build_flow_one_worker,
        "monitoring": {},
    },
    "flow-state-cache": {
        "func": flow.build_flow_state_cache,
        "monitoring": {},
    },
    "queue-metrics": {
        "func": queue_and_consumer_metrics.build_queue_metrics,
        "monitoring": {
            "args": ["monitoring"]
        },
        "grafana": {
            "args": ["grafana"]
        },
    },
    "queue-consumer-metrics": {
        "func": queue_and_consumer_metrics.build_queue_consumer_metrics,
        "monitoring": {
            "args": ["monitoring"]
        },
        "grafana": {
            "args": ["grafana"]
        },
    },
    "user-load": {
        "func": build_user_load,
        "monitoring": {},
    },
}


def main():
    run(dashboards)


if __name__ == "__main__":
    main()
