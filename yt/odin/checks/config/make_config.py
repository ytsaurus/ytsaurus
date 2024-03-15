from copy import deepcopy
import json
import sys


def deep_merge(*dicts):
    dicts = [deepcopy(dict_) for dict_ in dicts]
    return _do_deep_merge(*dicts)


def _do_deep_merge(*dicts):
    for dict_ in dicts:
        if not isinstance(dict_, dict):
            raise TypeError("{} is not a dict".format(dict_))
    lhs = dicts[0]
    for rhs in dicts[1:]:
        for key, value in rhs.items():
            if key not in lhs:
                lhs[key] = value
            elif isinstance(value, dict):
                lhs[key] = _do_deep_merge(value, lhs[key])
            else:
                raise ValueError("conflicting key '{}' values: lhs {}, rhs {}".format(key, lhs, rhs))
    return lhs


def get_checks_config():
    # Juggler does not provide a guarantee of processing all the given events: JUGGLERSUPPORT-1174.
    escalated_first_crit = {"simple": {"period": 5, "threshold": 5, "partially_available_strategy": "skip"}}
    instant = {"simple": {"period": 2, "threshold": 1}}
    instant_force_ok = {"simple": {"period": 2, "threshold": 1, "partially_available_strategy": "force_ok"}}

    critical_check_alerts = {
        "simple": {
            "period": 5,
            "threshold": 3,
            "partially_available_strategy": "skip",
            "time_period_overrides": [
                {
                    "start_time": "00:00:00",
                    "end_time": "10:00:00",
                    "threshold": 1,
                    "period": 6,
                },
            ],
        },
        "long": {
            "period": 15,
            "threshold": 8,
            "partially_available_strategy": "skip"
        }
    }

    sort_result_hourly = {
        "sort_result": {
            "alerts": {
                "hourly": {
                    "period": 60,
                    "threshold": 45,
                }
            }
        }
    }
    clouds = {
        "sort_result_cloud": {"enable": True},
        "map_result_cloud": {"enable": True},
        "suspicious_jobs_cloud": {"enable": True},
        "register_watcher_cloud": {"enable": True},
        "cloud_cpu": {"enable": True},
    }
    codicils = {
        "codicil_operations": {"enable": True}
    }
    operations_count_hume = {"operations_count": {"options": {"operations_count_threshold": 10000}}}
    operations_count_mr_large = {"operations_count": {"options": {
        "operations_count_threshold": 25000,
        "recoursive_node_count_crit": 175000,
        "recoursive_node_count_warn": 145000}}}
    controller_agent_operation_memory_consumption_hahn_arnold = {
        "controller_agent_operation_memory_consumption": {"options": {"memory_threshold": 30 * 1024 ** 3}}
    }
    destroyed_replicas_size_hahn_arnold = {
        "destroyed_replicas_size": {
            "options": {
                "node_threshold": 1000000,
                "total_threshold": 20000000
            }
        },
    }
    suspicious_jobs_options = {"critical_suspicious_job_inactivity_timeout": 420}
    sort_result_options = {
        "soft_sort_timeout": 70,
        "temp_tables_path": "//sys/admin/odin/sort_result"
    }
    map_result_options = {
        "soft_map_timeout": 105,
        "temp_tables_path": "//sys/admin/odin/map_result"
    }
    map_with_ssd_sandbox_result_check = {
        "map_with_ssd_sandbox_result": {
            "enable": True,
            "check_timeout": 300,
            "options": {
                "soft_map_timeout": 105,
                "temp_tables_path": "//sys/admin/odin/map_with_ssd_sandbox_result",
            },
            "alerts": critical_check_alerts,
        }
    }
    chyt_clique_liveness_options = {
        "soft_select_timeout": 30,
        "temp_tables_path": "//sys/admin/odin/chyt_clique_liveness"
    }
    dynamic_table_replication_stable = {
        "dynamic_table_replication": {
            "enable": True,
            "options": {
                "metacluster": "markov",
                "replica_clusters": ["seneca-vla", "markov", "seneca-sas", "hahn", "arnold"],
            },
        },
    }
    dynamic_table_replication_prestable = {
        "dynamic_table_replication": {
            "enable": True,
            "options": {
                "metacluster": "pythia",
                "replica_clusters": ["zeno", "pythia", "hume"],
            },
        },
    }
    skynet_manager = {
        "skynet_integration": {
            "check_timeout": 600,
            "enable": True,
            "options": {
                "temp_tables_path": "//sys/admin/odin/skynet_integration",
            }
        },
        "skynet_resources_availability": {
            "check_timeout": 600,
            "enable": True
        }
    }
    snapshot_validation = {
        "snapshot_validation": {
            "enable": True
        }
    }
    tablet_stress_test = {
        "tablet_stress_test": {
            "enable": True
        }
    }
    enable_tablet_cell_snapshot_convergence = {
        "tablet_cell_snapshot_convergence": {
            "enable": True
        }
    }
    clock_quorum_health = {
        "clock_quorum_health": {
            "enable": True,
            "options": {"monitoring_port": 10016}
        },
    }
    wide_window_quorum_health = {
        "quorum_health": {
            "alerts": {
                "simple": {
                    "period": 4,
                }
            },
        },
    }
    disable_tablet_balancer_alerts = {
        "tablet_balancer_alerts": {
            "enable": False
        }
    }
    disable_remote_copy = {
        "remote_copy": {
            "enable": False
        }
    }
    disable_sys_clusters_sync = {
        "sys_clusters_sync": {
            "enable": False
        }
    }
    enable_remote_copy = {
        "remote_copy": {
            "enable": True
        }
    }
    enable_nightly_compression = {
        "nightly_compression": {
            "enable": True
        }
    }
    enable_nightly_compression_hahn = {
        "nightly_compression": {
            "enable": True,
            "options": {
                "expected_compression_delta_sec": 60 * 60 * 24,  # 1d
            }
        }
    }
    enable_nightly_compression_arnold = {
        "nightly_compression": {
            "enable": True,
            "options": {
                "expected_compression_delta_sec": 60 * 60 * 16,  # 16h
            }
        }
    }
    spare_tablet_nodes_big = {
        "spare_tablet_nodes": {
            "options": {
                "tablet_common_nodes_threshold": 5,
                "balancer_disabled_nodes_threshold": 2
            }
        }
    }
    spare_tablet_nodes_small = {
        "spare_tablet_nodes": {
            "options": {
                "tablet_common_nodes_threshold": 1,
                "balancer_disabled_nodes_threshold": 1
            }
        }
    }
    bundle_controller = {
        "spare_tablet_nodes": { "enable": False },
    }
    allow_unaware_nodes = {
        "unaware_nodes": {
            "options": {
                "allow_unaware_nodes": True
            }
        }
    }
    nochyt = {
        "chyt_public_clique_liveness": { "enable": False },
        "chyt_prestable_clique_liveness": { "enable": False },
        "chyt_datalens_clique_liveness": { "enable": False },
        "chyt_prestable_datalens_clique_liveness": { "enable": False },
    }
    enable_discovery = {
        "discovery": {
            "enable": True
        }
    }

    cluster_name_to_query_tracker_stage = { "pythia": "experimental", "ada": "production", "markov": "production" }

    enable_query_tracker_alerts = {
        "query_tracker_alerts": {
            "enable": True,
            "alerts": instant_force_ok,
        },
        "query_tracker_yql_liveness": {
            "enable": True,
            "alerts": instant_force_ok,
            "options": {
                "cluster_name_to_query_tracker_stage": cluster_name_to_query_tracker_stage,
            }
        },
        "query_tracker_ql_liveness": {
            "enable": True,
            "alerts": instant_force_ok,
            "options": {
                "cluster_name_to_query_tracker_stage": cluster_name_to_query_tracker_stage,
            }
        }
    }

    enable_query_tracker_with_chyt_alerts = deep_merge({
            "query_tracker_chyt_liveness": {
                "enable": True,
                "alerts": instant_force_ok,
                "options": {
                    "cluster_name_to_query_tracker_stage": cluster_name_to_query_tracker_stage,
                    "chyt_cluster_name": "hume"
                }
            }
        }, enable_query_tracker_alerts)

    scheduler_alerts_update_fair_share_gpu = {
        "scheduler_alerts_update_fair_share_gpu": {
            "alerts": instant_force_ok
        }
    }

    nightly_wide_window_alerts = {
        "simple": {
            "time_period_overrides": [
                {
                    "start_time": "00:00:00",
                    "end_time": "10:00:00",
                    "threshold": 1,
                    "period": 20,
                },
            ],
        },
        "long": {
            "time_period_overrides": [
                {
                    "start_time": "00:00:00",
                    "end_time": "10:00:00",
                    "threshold": 1,
                    "period": 20,
                },
            ],
        }
    }

    sort_result_nightly_wide_window = {
        "sort_result": {
            "alerts": nightly_wide_window_alerts
        }
    }

    map_result_nightly_wide_window = {
        "map_result": {
            "alerts": nightly_wide_window_alerts
        }
    }

    system_quotas_with_non_critical_yp_account = {
        "system_quotas": {
            "options": {
                "accounts": {
                    "per_cluster_not_so_critical_names": ["yp"],
                }
            }
        }
    }

    system_quotas_only_per_account_tablet_resources = {
        "system_quotas": {
            "options": {
                "accounts": {
                    "enable_tablet_resource_validation": True,
                },
                "bundles": {
                    "enable_tablet_resource_validation": False,
                }
            }
        }
    }

    system_quotas_with_per_account_tablet_resources = {
        "system_quotas": {
            "options": {
                "accounts": {
                    "enable_tablet_resource_validation": True,
                }
            }
        }
    }

    YP_TRANSPORT_TO_PORT = {
        "grpc": 8090,
        "http": 8443,
    }

    def get_yp_address(cluster, port):
        return "{}.yp.yandex.net:{}".format(cluster, port)

    def get_yp_addresses(cluster):
        config = {}
        for transport in ["grpc", "http"]:
            config["yp_{}_address".format(transport)] = get_yp_address(cluster, YP_TRANSPORT_TO_PORT[transport])
        return config

    def get_yp_options(cluster):
        config = {"yp_config": dict(enable_ssl=True)}
        config.update(get_yp_addresses(cluster))
        return config

    def get_yp_alerts(check_name):
        return dict(
            short=dict(
                period=5,
                threshold=1,
                partially_available_strategy="warn",
                juggler_service_name="{}_short".format(check_name),
            ),
            long=dict(
                period=60,
                threshold=50,
                partially_available_strategy="warn",
                juggler_service_name="{}_long".format(check_name),
            ),
        )

    def get_yp_event_log_config(cluster):
        return {
            "yp_event_log": {
                "enable": True,
                "options": get_yp_options(cluster),
            }
        }

    def get_yp_pod_disruption_budget_controller_config(cluster):
        return {
            "yp_pod_disruption_budget_controller": {
                "enable": True,
                "options": dict(
                    timeout_schedule_pods=180,
                    timeout_update_pod_disruption_budgets=180,
                    **get_yp_options(cluster)
                ),
            }
        }

    def get_yp_pod_set_controller_config(cluster):
        return {
            "yp_pod_set_controller": {
                "enable": True,
                "options": dict(
                    update_timeout=360,
                    schedule_timeout=360,
                    **get_yp_options(cluster),
                )
            }
        }

    def get_yp_service_controller_check_config(cluster):
        return {
            "yp_service_controller": {
                "enable": True,
                "options": get_yp_options(cluster),
            }
        }

    def get_yp_accounting_check_config(cluster):
        return {
            "yp_accounting": {
                "enable": True,
                "options": dict(
                    timeout=90,
                    **get_yp_options(cluster)
                ),
            }
        }

    def get_yp_object_counts_config(cluster):
        return {
            "yp_object_counts": {
                "enable": True,
                "options": dict(
                    **get_yp_options(cluster)
                ),
            }
        }

    def get_yp_schedule_pod_check_config(cluster):
        return {
            "yp_schedule_pod": {
                "enable": True,
                "options": dict(
                    schedule_timeout=180,
                    **get_yp_options(cluster)
                ),
            }
        }

    def get_yp_ip4_manager_check_config(cluster):
        return {
            "yp_ip4_manager": {
                "enable": True,
                "options": get_yp_options(cluster),
            }
        }

    def get_yp_master_check_config(cluster):
        return {
            "yp_master": {
                "enable": True,
                "options": get_yp_options(cluster),
                "env": {
                    "YT_ODIN_FRONTEND_LOG_LEVEL": "DEBUG",
                }
            }
        }

    def get_yp_masters_available_check_config(cluster):
        return {
            "yp_masters_available": {
                "enable": True,
                "options": get_yp_options(cluster),
                "env": {
                    "YT_ODIN_FRONTEND_LOG_LEVEL": "DEBUG",
                }
            }
        }

    def get_yp_garbage_collector_check_config(cluster):
        return {
            "yp_garbage_collector": {
                "enable": True,
                "options": get_yp_options(cluster),
            }
        }

    def get_yp_heavy_scheduler_check_config():
        return {
            "yp_heavy_scheduler": {
                "enable": True,
                "options": {},
            }
        }

    def get_yp_tablet_errors_check_config():
        return {
            "yp_tablet_errors": {
                "enable": True,
                "options": {},
            }
        }

    def get_yp_access_control_check_config(cluster):
        return {
            "yp_access_control": {
                "enable": True,
                "options": get_yp_options(cluster),
                "env": {
                    "YT_ODIN_FRONTEND_LOG_LEVEL": "DEBUG",
                },
            }
        }

    def get_yp_available_timestamps_config(cluster):
        return {
            "yp_available_timestamps": {
                "enable": True,
                "options": get_yp_options(cluster),
            }
        }

    def get_yp_pod_scaler_config(cluster):
        return {
            "yp_pod_scaler": {
                "enable": True,
                "options": get_yp_options(cluster),
            }
        }

    def get_yp_pod_scaler_liveness_config(cluster):
        return {
            "yp_pod_scaler_liveness": {
                "enable": True,
                "options": get_yp_options(cluster),
            }
        }

    def get_yp_actual_allocations_sync_config(cluster):
        return {
            "yp_actual_allocations_sync": {
                "enable": True,
                "options": get_yp_options(cluster),
            }
        }

    def get_yp_config(cluster):
        return deep_merge(
            get_yp_schedule_pod_check_config(cluster),
            get_yp_ip4_manager_check_config(cluster),
            get_yp_master_check_config(cluster),
            get_yp_masters_available_check_config(cluster),
            get_yp_garbage_collector_check_config(cluster),
            get_yp_service_controller_check_config(cluster),
            get_yp_heavy_scheduler_check_config(),
            get_yp_tablet_errors_check_config(),
            get_yp_access_control_check_config(cluster),
            get_yp_event_log_config(cluster),
            get_yp_pod_disruption_budget_controller_config(cluster),
            get_yp_pod_set_controller_config(cluster),
            get_yp_available_timestamps_config(cluster),
            get_yp_accounting_check_config(cluster),
            get_yp_object_counts_config(cluster),
            get_yp_pod_scaler_config(cluster),
            get_yp_pod_scaler_liveness_config(cluster),
            get_yp_actual_allocations_sync_config(cluster),
        )

    config = {
        "checks": {
            "sort_result": {
                "options": sort_result_options,
                "check_timeout": 120,
                "alerts": critical_check_alerts,
            },
            "map_result": {
                "options": map_result_options,
                "check_timeout": 180,
                "alerts": critical_check_alerts,
            },
            "sort_result_cloud": {
                "enable": False,
                "options": deep_merge(sort_result_options, {
                    "pool_config": {}
                })
            },
            "map_result_cloud": {
                "enable": False,
                "options": deep_merge(map_result_options, {
                    "pool_config": {}
                })
            },

            "broken_gpu_nodes": {
                "enable": False,
                "alerts": {
                    "simple": {
                        "period": 120,
                        "threshold": 30,
                        "partially_available_strategy": "force_ok",
                    }
                },
                "options": {
                    "broken_node_count_threshold": 4,
                }
            },

            "chyt_public_clique_liveness": {
                "alerts": {
                    "simple": {
                        "period": 10,
                        "threshold": 1,
                    }
                },
                "options": chyt_clique_liveness_options
            },
            "chyt_prestable_clique_liveness": {
                "alerts": {
                    "simple": {
                        "period": 10,
                        "threshold": 1,
                        "partially_available_strategy": "force_ok",
                    }
                },
                "options": chyt_clique_liveness_options
            },
            "chyt_datalens_clique_liveness": {
                "alerts": {
                    "simple": {
                        "period": 5,
                        "threshold": 1,
                    }
                },
                "options": chyt_clique_liveness_options
            },
            "chyt_prestable_datalens_clique_liveness": {
                "alerts": {
                    "simple": {
                        "period": 10,
                        "threshold": 1,
                        "partially_available_strategy": "force_ok",
                    }
                },
                "options": chyt_clique_liveness_options
            },
            "dynamic_table_commands": {
                "options": {
                    "temp_tables_path": "//sys/admin/odin/dynamic_table_commands",
                    "tablet_cell_bundle": "sys",
                },
                "alerts": critical_check_alerts,
            },
            "suspicious_jobs_cloud": {
                "enable": False,
                "options": suspicious_jobs_options,
            },
            "suspicious_jobs": {
                "options": suspicious_jobs_options,
                "alerts": {
                    "simple": {
                        "period": 120,
                        "threshold": 60,
                        "partially_available_strategy": "force_ok",
                        "time_period_overrides": [
                            {
                                "start_time": "00:00:00",
                                "end_time": "10:00:00",
                                "threshold": 120,
                                "period": 240,
                            },
                        ],
                    },
                },
            },
            "lost_vital_chunks": {
                "alerts": instant,
                "options": {
                    "max_size": 10
                }
            },
            "clock_quorum_health": {
                "enable": False,
                "alerts": {
                    "simple": {
                        "period": 3,
                        "threshold": 2,
                        "partially_available_strategy": "force_ok",
                    }
                },
                "options": {"monitoring_port": 10016}
            },
            "quorum_health": {
                "alerts": {
                    "simple": {
                        "period": 3,
                        "threshold": 2,
                        "partially_available_strategy": "force_ok",
                    }
                },
                "options": {"monitoring_port": 10010}
            },
            "discovery": {
                "enable": False,
                "alerts": {
                    "simple": {
                        "period": 3,
                        "threshold": 2,
                        "partially_available_strategy": "force_ok",
                    }
                },
            },
            "system_quotas": {
                "alerts": {
                    "simple": {
                        "period": 2,
                        "threshold": 1,
                        "partially_available_strategy": "force_crit",
                        "time_period_overrides": [
                            {
                                "start_time": "00:00:00",
                                "end_time": "10:00:00",
                                "partially_available_strategy": "skip",
                            },
                        ],
                    },
                },
                "options": {
                    "accounts": {
                        "all_possible_names": [
                            "sys", "tmp", "intermediate", "tmp_files", "tmp_jobs", "yt-skynet-m5r",
                            "operations_archive", "cron_compression", "clickhouse-kolkhoz", "yp",
                            "yt-integration-tests", "logfeller-yt",
                        ],
                        # These accounts should lead only to partially available (yellow) state on all clusters
                        "not_so_critical_names": [
                            # This account contains only logs, in worst-case scenario we
                            # will just lose some night activity history.
                            "clickhouse-kolkhoz"
                        ],
                        "per_cluster_not_so_critical_names": [],
                        # For some accounts threshold may be overridden
                        "custom_thresholds": {},
                        # For some accounts threshold depends on the time
                        "threshold_time_period_overrides": {
                            "logfeller-yt": {
                                "start_time": "00:00:00",
                                "end_time": "10:00:00",
                                "threshold": 95,
                            },
                            "yt-integration-tests": {
                                "start_time": "00:00:00",
                                "end_time": "10:00:00",
                                "threshold": 95,
                            },
                        },
                        "enable_tablet_resource_validation": False
                    },
                    "bundles": {
                        "all_possible_names": [
                            "sys", "sys_blobs", "sys_operations",
                        ],
                        "enable_tablet_resource_validation": True
                    }
                },
            },
            "master_alerts": {
                "alerts": {
                    "simple": {
                        "period": 10,
                        "threshold": 2,
                        "partially_available_strategy": "force_ok"
                    }
                }
            },
            "master_chunk_management": {
                "alerts": instant
            },
            "master_snapshot_processing": {
                "alerts": instant
            },
            "nightly_compression": {
                "enable" : False,
                "alerts": instant,
                "options": {
                    "table_path": "//home/yt-nightly-compression",
                    "table_ttl_sec": 60 * 60 * 48,  # 2d
                    "table_creation_delta_sec": 60 * 30,  # 30m
                    "max_tables_cnt": 120,
                    "expected_compression_delta_sec": 60 * 60 * 10,  # 10h
                }
            },
            "tablet_cells": {
                "check_timeout": 120,
                "alerts": {
                    "simple": {
                        "period": 10,
                        "threshold": 5,
                        "partially_available_strategy": "force_ok"
                    }
                }
            },
            "tablet_cell_gossip": {
                "check_timeout": 120,
                "alerts": {
                    "simple": {
                        "period": 10,
                        "threshold": 3,
                        "partially_available_strategy": "force_ok"
                    }
                }
            },
            "tablet_cell_snapshots": {
                "check_timeout": 120,
                "alerts": {
                    "simple": {
                        "period": 10,
                        "threshold": 5,
                        "partially_available_strategy": "force_ok"
                    }
                }
            },
            "chaos_cells": {
                "check_timeout": 120,
                "alerts": {
                    "simple": {
                        "period": 10,
                        "threshold": 5,
                        "partially_available_strategy": "force_ok"
                    }
                }
            },
            "oauth_health": {
                "alerts": instant_force_ok
            },
            "planned_maintenance": {
                "alerts": instant_force_ok,
                "options": {
                    "warn_threshold": 24 * 60 * 60,
                    "crit_threshold": 12 * 60 * 60,
                    "infra_backend_url": "infra-api.yandex-team.ru",
                    "infra_service_id": 65,
                    "cluster_name_to_infra_environment_id": {
                        "ada": 2395,
                        "arnold": 81,
                        "arnold-gnd": 9626,
                        "bohr": 87,
                        "freud": 84,
                        "freud-gnd": 9552,
                        "hahn": 83,
                        "hume": 85,
                        "landau": 138,
                        "locke": 96,
                        "markov": 89,
                        "nash": 5183,
                        "navier": 8647,
                        "ofd-xdc": 2157,
                        "pythia": 91,
                        "seneca-man": 92,
                        "seneca-sas": 192,
                        "seneca-vla": 193,
                        "seneca-vlx": 7383,
                        "seneca-klg": 8891,
                        "socrates": 86,
                        "vanga": 93,
                        "testing-gnd": 8999,
                        "yp-iva": 550,
                        "yp-man": 98,
                        "yp-man-pre": 196,
                        "yp-vlx": 7294,
                        "yp-klg": 8705,
                        "yp-myt": 528,
                        "yp-sas": 198,
                        "yp-sas-test": 99,
                        "yp-vla": 199,
                        "yp-xdc": 200,
                        "yp-adm-pre": 9023,
                        "zeno": 94,
                        "lyapunov-alpha": 6889,
                        "lyapunov-beta": 6890,
                        "poincare-alpha": 7046,
                        "poincare-beta": 7047,
                        "vapnik": 6707,
                    },
                },
            },
            "scheduler": {},
            "controller_agent_alerts": {
                "alerts": instant_force_ok
            },
            "scheduler_alerts": {
                "alerts": instant_force_ok
            },
            "scheduler_alerts_jobs_archivation": {
                "alerts": instant_force_ok
            },
            "scheduler_alerts_update_fair_share": {
                "alerts": instant_force_ok
            },
            "scheduler_alerts_update_fair_share_physical": {
                "alerts": instant_force_ok
            },
            "scheduler_alerts_update_fair_share_gpu": {},
            "scheduler_uptime": {
                "alerts": escalated_first_crit,
                "check_timeout": 120,
            },
            "scheduler_metering_statistics": {
                "alerts": escalated_first_crit,
                "options": {
                    "max_allowed_lag_in_hours": 4,
                },
            },
            "controller_agent_count": {
                "alerts": {
                    "simple": {
                        "period": 5,
                        "threshold": 1,
                    }
                },
                "check_timeout": 120,
            },
            "controller_agent_uptime": {
                "alerts": escalated_first_crit,
                "check_timeout": 120,
            },
            "controller_agent_operation_memory_consumption": {},
            "operations_satisfaction": {
                "options": {
                    "state_path": "//sys/admin/odin/operations_satisfaction_state",
                    "min_satisfaction_ratio": 0.9,
                    "critical_unsatisfied_minute_count": 10
                }
            },
            "operations_snapshots": {
                "enable": False,
                "options": {
                    "critical_time_without_snapshot_threshold": 3600
                }
            },
            "operations_count": {
                "alerts": instant,
                "options": {
                    "operations_count_threshold": 5000,
                    "recoursive_node_count_warn": 40000,
                    "recoursive_node_count_crit": 60000
                }
            },
            "operations_archive_tablet_store_preload": {
                "alerts": {
                    "simple": {
                        "period": 10,
                        "threshold": 1,
                        "partially_available_strategy": "force_ok"
                    }
                }
            },
            "codicil_operations": {
                "enable": False,
                "alerts": escalated_first_crit,
            },
            "dynamic_table_replication": {
                "enable": False,
                "alerts": {
                    "fast": {
                        "period": 5,
                        "threshold": 1,
                    },
                    "slow": {
                        "period": 120,
                        "threshold": 60,
                    },
                },
            },
            "skynet_integration": {
                "enable": False,
                "check_timeout": 120,
                # Hahn tablet cells recover up to 5 minutes
                "alerts": {"simple": {"period": 30, "threshold": 10, "partially_available_strategy": "force_ok"}}
            },
            "skynet_resources_availability": {
                "enable": False,
                # Hahn tablet cells recover up to 5 minutes
                "alerts": {"simple": {"period": 30, "threshold": 10, "partially_available_strategy": "force_ok"}}
            },
            "missing_part_chunks": {
                "alerts": instant_force_ok
            },
            "stuck_missing_part_chunks": {
                "alerts": instant_force_ok
            },
            "snapshot_validation": {
                "enable": False,
                "check_timeout": 120,
                "alerts": instant
            },
            "tablet_cell_snapshot_convergence": {
                "enable": False,
                "check_timeout": 120,
                "alerts": {
                    "simple": {
                        "period": 2,
                        "threshold": 2,
                        "partially_available_strategy": "force_ok",
                    },
                }
            },
            "tablet_stress_test": {
                "enable": False,
                "check_timeout": 120,
                "alerts": {
                    "simple": {
                        "period": 2,
                        "threshold": 2,
                        "partially_available_strategy": "force_ok",
                    },
                },
            },
            "unaware_nodes": {
                "alerts": {"simple": {"period": 45, "threshold": 1}}
            },
            "yp_schedule_pod": {
                "enable": False,
                "check_timeout": 60 * 3 + 10,
                "alerts": get_yp_alerts("yp_schedule_pod"),
            },
            "yp_service_controller": {
                "enable": False,
                "check_timeout": 300,
                "alerts": get_yp_alerts("yp_service_controller"),
            },
            "yp_master": {
                "enable": False,
                "alerts": get_yp_alerts("yp_master"),
            },
            "yp_masters_available": {
                "enable": False,
                "alerts": get_yp_alerts("yp_masters_available"),
            },
            "yp_ip4_manager": {
                "enable": False,
                "alerts": get_yp_alerts("yp_ip4_manager"),
                "check_timeout": 120,
            },
            "yp_garbage_collector": {
                "enable": False,
                "alerts": {
                    "hourly": {
                        "period": 60,
                        "threshold": 1,  # succeed at least once per hour
                    }
                }
            },
            "yp_heavy_scheduler": {
                "enable": False,
                "alerts": get_yp_alerts("yp_heavy_scheduler"),
            },
            "yp_tablet_errors": {
                "enable": False,
                "alerts": get_yp_alerts("yp_tablet_errors"),
            },
            "yp_access_control": {
                "enable": False,
                "alerts": get_yp_alerts("yp_access_control"),
            },
            "yp_accounting": {
                "enable": False,
                "check_timeout": 120,
                "alerts": get_yp_alerts("yp_accounting"),
            },
            "yp_event_log": {
                "enable": False,
                "check_timeout": 120,
                "alerts": get_yp_alerts("yp_event_log"),
            },
            "yp_object_counts": {
                "enable": False,
                "check_timeout": 120,
                "alerts": get_yp_alerts("yp_object_counts"),
            },
            "yp_pod_disruption_budget_controller": {
                "enable": False,
                "check_timeout": 360,
                "alerts": get_yp_alerts("yp_pod_disruption_budget_controller"),
            },
            "yp_pod_set_controller": {
                "enable": False,
                "check_timeout": 540,
                "alerts": get_yp_alerts("yp_pod_set_controller"),
            },
            "yp_available_timestamps": {
                "enable": False,
                "alerts": get_yp_alerts("yp_available_timestamps"),
            },
            "yp_pod_scaler": {
                "enable": False,
                "check_timeout": 120,
                "alerts": get_yp_alerts("yp_pod_scaler"),
            },
            "yp_pod_scaler_liveness": {
                "enable": False,
                "check_timeout": 300,
                "alerts": get_yp_alerts("yp_pod_scaler_liveness"),
            },
            "yp_actual_allocations_sync": {
                "enable": False,
                "check_timeout": 300,
                "alerts": get_yp_alerts("yp_actual_allocations_sync"),
            },
            "register_watcher": {
                "alerts": instant_force_ok,
                "options": {
                    "crit_threshold": 7,
                    "warn_threshold": 5,
                }
            },
            "register_watcher_cloud": {
                "enable": False,
                "alerts": instant_force_ok,
                "options": {
                    "crit_threshold": 10,
                    "warn_threshold": 5,
                }
            },
            "remote_copy": {
                "enable": True,
                "alerts": instant_force_ok,
                "check_timeout": 120,
                "options": {
                    "excluded": ["ofd-xdc", "vapnik", "seneca-vlx", "seneca-man"],
                    "pool": "odin-remote-copy",
                    "temp_tables_path": "//sys/admin/odin/remote_copy",
                },
            },
            "wrapper_files_count": {
                "alerts": instant_force_ok,
                "options": {
                    "files_count_threshold": 40000,
                }
            },
            "spare_tablet_nodes": {
                "alerts": instant_force_ok,
                "options": {
                    "tablet_common_nodes_threshold": 2,
                    "balancer_disabled_nodes_threshold": 1
                }
            },
            "bundle_balancer_alerts": {
                "alerts": instant_force_ok,
            },
            "medium_balancer_alerts": {
                "alerts": instant_force_ok,
            },
            "cloud_cpu": {
                "enable": False,
                "alerts": instant_force_ok,
                "options": {
                    "min_cpu_count": 1000
                }
            },
            "sys_clusters_sync": {
                "alerts": instant_force_ok
            },
            "destroyed_replicas_size": {
                "alerts": {
                    "simple": {
                        "period": 60,
                        "threshold": 10,
                        "time_period_overrides": [
                            {
                                "start_time": "00:00:00",
                                "end_time": "10:00:00",
                                "threshold": 0,
                            },
                        ],
                    }
                },
                "options": {
                    "node_threshold": 100000,
                    "total_threshold": 1000000
                }
            },
            "tablet_balancer_alerts": {
                "alerts": instant_force_ok,
            },
            "queue_agent_alerts": {
                "alerts": instant_force_ok,
                "options": {
                    "queue_agent_stage_clusters": {
                        "hume": "hume",
                        "pythia": "pythia",
                        "markov": "markov",
                        "ada": "ada",
                    }
                }
            },
            "query_tracker_alerts": {
                "enable": False,
            },
            "query_tracker_yql_liveness": {
                "enable": False,
            },
            "query_tracker_chyt_liveness": {
                "enable": False,
            },
            "query_tracker_ql_liveness": {
                "enable": False,
            },
            "master": {},
            "proxy": {}
        },
        "cluster_overrides": {
            "ada": deep_merge(
                snapshot_validation,
                allow_unaware_nodes,
                wide_window_quorum_health,
                enable_discovery,
                enable_remote_copy,
                bundle_controller,
                system_quotas_only_per_account_tablet_resources,
                enable_tablet_cell_snapshot_convergence,
                tablet_stress_test,
            ),
            "arnold": deep_merge(
                skynet_manager,
                snapshot_validation,
                operations_count_mr_large,
                clouds,
                spare_tablet_nodes_big,
                controller_agent_operation_memory_consumption_hahn_arnold,
                scheduler_alerts_update_fair_share_gpu,
                clock_quorum_health,
                map_with_ssd_sandbox_result_check,
                destroyed_replicas_size_hahn_arnold,
                enable_discovery,
                enable_nightly_compression_arnold,
                bundle_controller,
                dynamic_table_replication_stable,
                system_quotas_with_non_critical_yp_account,
            ),
            "arnold-gnd": deep_merge(
                snapshot_validation,
                allow_unaware_nodes,
                clock_quorum_health,
                nochyt,
            ),
            "landau": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                bundle_controller,
                system_quotas_with_per_account_tablet_resources,
            ),
            "bohr": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                sort_result_nightly_wide_window,
                map_result_nightly_wide_window,
                bundle_controller,
                system_quotas_with_per_account_tablet_resources,
            ),
            "socrates": deep_merge(
                snapshot_validation,
                allow_unaware_nodes,
                enable_discovery,
                enable_remote_copy,
                bundle_controller,
                system_quotas_with_non_critical_yp_account,
                system_quotas_only_per_account_tablet_resources,
                enable_tablet_cell_snapshot_convergence,
            ),
            "hume": deep_merge(
                dynamic_table_replication_prestable,
                skynet_manager,
                codicils,
                operations_count_hume,
                snapshot_validation,
                wide_window_quorum_health,
                enable_discovery,
                bundle_controller,
                tablet_stress_test,
                enable_tablet_cell_snapshot_convergence,
            ),
            "freud": deep_merge(
                clouds,
                skynet_manager,
                codicils,
                snapshot_validation,
                clock_quorum_health,
                map_with_ssd_sandbox_result_check,
                enable_discovery,
                enable_nightly_compression,
                bundle_controller,
            ),
            "freud-gnd": deep_merge(
                snapshot_validation,
                allow_unaware_nodes,
                clock_quorum_health,
                nochyt,
            ),
            "seneca-sas": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                bundle_controller,
                dynamic_table_replication_stable,
            ),
            "seneca-vla": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                bundle_controller,
                dynamic_table_replication_stable,
            ),
            "seneca-klg": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                bundle_controller,
            ),
            "seneca-vlx": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                bundle_controller,
                system_quotas_with_per_account_tablet_resources,
            ),
            "seneca-man": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                bundle_controller,
            ),
            "pythia": deep_merge(
                dynamic_table_replication_prestable,
                snapshot_validation,
                clock_quorum_health,
                enable_query_tracker_with_chyt_alerts,
                wide_window_quorum_health,
                bundle_controller,
            ),
            "markov": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                bundle_controller,
                dynamic_table_replication_stable,
            ),
            "nash": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                system_quotas_only_per_account_tablet_resources,
            ),
            "navier": deep_merge(
                snapshot_validation,
                allow_unaware_nodes,
                nochyt,
                system_quotas_only_per_account_tablet_resources,
            ),
            "hahn": deep_merge(
                sort_result_hourly,
                snapshot_validation,
                operations_count_mr_large,
                controller_agent_operation_memory_consumption_hahn_arnold,
                clouds,
                codicils,
                skynet_manager,
                scheduler_alerts_update_fair_share_gpu,
                spare_tablet_nodes_big,
                map_with_ssd_sandbox_result_check,
                clock_quorum_health,
                destroyed_replicas_size_hahn_arnold,
                enable_discovery,
                enable_nightly_compression_hahn,
                dynamic_table_replication_stable,
                system_quotas_with_non_critical_yp_account,
            ),
            "zeno": deep_merge(
                allow_unaware_nodes,
                dynamic_table_replication_prestable,
                snapshot_validation,
                clock_quorum_health,
                wide_window_quorum_health,
                bundle_controller,
                tablet_stress_test,
                enable_tablet_cell_snapshot_convergence,
            ),
            "locke": deep_merge(
                snapshot_validation,
                system_quotas_only_per_account_tablet_resources,
            ),
            "vanga": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                bundle_controller,
            ),
            "ofd-xdc": deep_merge(disable_remote_copy, snapshot_validation),
            "testing-gnd": deep_merge(
                snapshot_validation,
                allow_unaware_nodes,
                nochyt,
            ),
            "yp-sas-test": spare_tablet_nodes_small,
            "yp-adm-pre": deep_merge(
                spare_tablet_nodes_small,
                nochyt,
                disable_remote_copy,
                disable_sys_clusters_sync,
                disable_tablet_balancer_alerts,
            ),
            "yp-man-pre": spare_tablet_nodes_small,
            "yp-vlx": spare_tablet_nodes_small,
            "lyapunov-alpha": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                allow_unaware_nodes,
                nochyt,
            ),
            "lyapunov-beta": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                allow_unaware_nodes,
                nochyt,
            ),
            "poincare-alpha": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                allow_unaware_nodes,
                nochyt,
                system_quotas_only_per_account_tablet_resources,
            ),
            "poincare-beta": deep_merge(
                snapshot_validation,
                clock_quorum_health,
                allow_unaware_nodes,
                nochyt,
                system_quotas_only_per_account_tablet_resources,
            ),
            "vapnik": deep_merge(
                snapshot_validation,
                allow_unaware_nodes,
                nochyt,
            ),
        }
    }

    yp_clusters = ["yp-sas-test", "yp-man-pre", "yp-sas", "yp-man", "yp-myt", "yp-iva",
                   "yp-vla", "yp-xdc", "yp-vlx", "yp-klg", "yp-adm-pre"]
    for cluster in yp_clusters:
        config["cluster_overrides"][cluster] = deep_merge(
            config["cluster_overrides"].get(cluster, {}),
            snapshot_validation,
            get_yp_config(cluster[len("yp-"):]),
            allow_unaware_nodes,
            skynet_manager,
            system_quotas_only_per_account_tablet_resources,
        )

    return config


def main():
    config = get_checks_config()
    json.dump(config, sys.stdout, indent=4, sort_keys=True)


if __name__ == "__main__":
    main()
