#!/usr/bin/env python3

from copy import deepcopy
import argparse
import json
import os


ODIN_CONFIG_TEMPLATE = {
    "checks": {
        "check_log_messages_max_size": 16384,
        "check_timeout": 65,
        # "path": "{CHECKS_PATH}"
    },
    "clusters": {
        # "my_cluster": {
        #     "db_config": {
        #         "options": {
        #             "cluster": "my_cluster",
        #         }
        #     },
        #     "yt_config": {
        #         "proxy": "my_cluster.dev:8080"
        #     }
        # },
    },
    "logging": {
        # "filename": "{LOG_FILE}",
        "level": "DEBUG",
        "port": 9001,
    },
    # "secrets": {
    #     "yt_token": "{YT_TOKEN}"
    # },
    "service_config": {
        "path": "//sys/odin",
        # "pids_file": "{PIDS_FILE}",
        # "proxy": "my_coordination_cluster.dev:8080",
        "shard_count": 1,
        "timeout": 1,
        # "token": "{YT_TOKEN}"
    },
    "template": {
        "db_config": {
            "options": {
                "cluster": "0",
                "config": {
                    "dynamic_table_retries": {
                        "backoff": {
                            "exponential_policy": {
                                "base": 2,
                                "decay_factor_bound": 0.3,
                                "max_timeout": 40000,
                                "start_timeout": 10000
                            },
                            "policy": "exponential"
                        },
                        "count": 3,
                        "total_timeout": 80000
                    },
                    "proxy": {
                        "heavy_request_timeout": 5000,
                        "request_timeout": 5000,
                        "retries": {
                            "backoff": {
                                "exponential_policy": {
                                    "base": 2,
                                    "decay_factor_bound": 0.3,
                                    "max_timeout": 40000,
                                    "start_timeout": 10000
                                },
                                "policy": "exponential"
                            },
                            "count": 3,
                            "total_timeout": 80000
                        }
                    }
                },
                # "proxy": "my_coordination_cluster.dev:8080",
                "table": "//sys/odin/checks",
                # "token": "{YT_TOKEN}"
            },
            "type": "yt"
        },
        "log_server_config": {
            "socket_path_pattern": "./yt_odin.{cluster_name}.sock",
            "storage_writer": {
                "max_write_batch_size": 256
            }
        },
        "yt_config": {
            "heavy_request_retry_timeout": 15000,
            "proxy": 0,
            "request_retry_count": 3,
            "request_retry_timeout": 10000,
            "request_timeout": 30000,
            # "token": "{YT_TOKEN}"
        }
    }
}

ODIN_WEBSERVICE_CONFIG_TEMPLATE = {
    "clusters": {
        # "my_cluster": {
        #     "db_config": {
        #         "options": {
        #             "cluster": "my_cluster"
        #         }
        #     }
        # },
    },
    "db_config": {
        "options": {
            # "proxy": "my_coordination_cluster.dev:8080",
            "table": "//sys/odin/checks",
            # "token": "{YT_TOKEN}"
        },
        "type": "yt"
    },
    "debug": 0,
    "host": "::",
    "logging": {
        # "filename": "{LOG_FILE}"
    },
    "port": 9002,
    "services": [
        {
            "display_name": "Sort Result",
            "name": "sort_result"
        },
    ],
    "thread_count": 4,
}


CHECKS_LIST = [
    "clock_quorum_health",
    "controller_agent_alerts",
    "controller_agent_count",
    "controller_agent_uptime",
    "controller_agent_operation_memory_consumption",
    "destroyed_replicas_size",
    "discovery",
    "dynamic_table_commands",
    "dynamic_table_replication",
    "lost_vital_chunks",
    "quorum_health",
    "map_result",
    "master",
    "master_alerts",
    "master_chunk_management",
    "medium_balancer_alerts",
    "missing_part_chunks",
    "oauth_health",
    "operations_archive_tablet_store_preload",
    "operations_count",
    "operations_satisfaction",
    "operations_snapshots",
    "proxy",
    "queue_agent_alerts",
    "query_tracker_alerts",
    "query_tracker_yql_liveness",
    "query_tracker_chyt_liveness",
    "query_tracker_ql_liveness",
    "quorum_health",
    "register_watcher",
    "scheduler",
    "scheduler_alerts",
    "scheduler_alerts_jobs_archivation",
    "scheduler_alerts_update_fair_share",
    "scheduler_uptime",
    "sort_result",
    "stuck_missing_part_chunks",
    "suspicious_jobs",
    "tablet_cells",
    "tablet_cell_gossip",
    "tablet_cell_snapshots",
    "chaos_cells",
    "unaware_nodes",
    "wrapper_files_count",
]


def create_odin_checks_config():
    suspicious_jobs_options = {"critical_suspicious_job_inactivity_timeout": 420}
    sort_result_options = {
        "soft_sort_timeout": 70,
        "temp_tables_path": "//sys/admin/odin/sort_result"
    }
    map_result_options = {
        "soft_map_timeout": 105,
        "temp_tables_path": "//sys/admin/odin/map_result"
    }

    config = {
        "checks": {
            "sort_result": {
                "options": sort_result_options,
                "check_timeout": 120,
            },
            "map_result": {
                "options": map_result_options,
                "check_timeout": 180,
            },
            "dynamic_table_commands": {
                "options": {
                    "temp_tables_path": "//sys/admin/odin/dynamic_table_commands",
                    "tablet_cell_bundle": "sys",
                },
            },
            "suspicious_jobs": {
                "options": suspicious_jobs_options,
            },
            "lost_vital_chunks": {
                "options": {
                    "max_size": 10
                }
            },
            "clock_quorum_health": {
                "enable": False,
                "options": {"monitoring_port": 10016}
            },
            "quorum_health": {
                "options": {"monitoring_port": 10010}
            },
            "tablet_cells": {
                "check_timeout": 120,
            },
            "tablet_cell_gossip": {
                "check_timeout": 120,
            },
            "tablet_cell_snapshots": {
                "check_timeout": 120,
            },
            "scheduler_uptime": {
                "check_timeout": 120,
            },
            "controller_agent_count": {
                "check_timeout": 120,
            },
            "controller_agent_uptime": {
                "check_timeout": 120,
            },
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
                "options": {
                    "operations_count_threshold": 5000,
                    "recoursive_node_count_warn": 40000,
                    "recoursive_node_count_crit": 60000
                }
            },
            "dynamic_table_replication": {
                "enable": False,
            },
            "register_watcher": {
                "options": {
                    "crit_threshold": 7,
                    "warn_threshold": 5,
                }
            },
            "wrapper_files_count": {
                "options": {
                    "files_count_threshold": 40000,
                }
            },
            "destroyed_replicas_size": {
                "options": {
                    "node_threshold": 100000,
                    "total_threshold": 1000000
                }
            },
        },
    }

    for check in CHECKS_LIST:
        if check not in config["checks"]:
            config["checks"][check] = {}

    return config


def which(name):
    paths = os.environ.get("PATH", "").split(os.pathsep)
    for dir in paths:
        path = os.path.join(dir, name)
        if os.access(path, os.X_OK):
            return path
    return None


def generate_odin_services_configs(
        db_cluster_proxy,
        db_cluster_token_file,
        # List of pairs (name, proxy_address)
        check_clusters,
        check_cluster_token_file,
        odin_config_output,
        odin_webservice_config_output,
        odin_checks_directory,
        odin_log_file=None,
        odin_webservice_log_file=None,
        odin_pids_file=None):
    if odin_log_file is None:
        odin_log_file = "odin.log"
    if odin_webservice_log_file is None:
        odin_webservice_log_file = "odin_webservice.log"
    if odin_pids_file is None:
        odin_pids_file = "odin.pids"

    db_cluster_token = open(db_cluster_token_file).read().strip()
    check_cluster_token = open(check_cluster_token_file).read().strip()

    odin_config = deepcopy(ODIN_CONFIG_TEMPLATE)
    odin_config["logging"]["filename"] = odin_log_file

    odin_webservice_config = deepcopy(ODIN_WEBSERVICE_CONFIG_TEMPLATE)
    odin_webservice_config["logging"]["filename"] = odin_webservice_log_file

    odin_config["checks"]["path"] = odin_checks_directory

    odin_config["service_config"]["pids_file"] = odin_pids_file
    odin_config["service_config"]["proxy"] = db_cluster_proxy
    odin_config["service_config"]["token"] = db_cluster_token

    odin_config["secrets"] = {"yt_token": db_cluster_token}

    odin_config["template"]["yt_config"]["token"] = check_cluster_token
    odin_config["template"]["db_config"]["options"]["proxy"] = db_cluster_proxy
    odin_config["template"]["db_config"]["options"]["token"] = db_cluster_token

    odin_webservice_config["db_config"]["options"]["proxy"] = db_cluster_proxy
    odin_webservice_config["db_config"]["options"]["token"] = db_cluster_token

    for name, proxy in check_clusters:
        odin_config["clusters"][name] = {
            "db_config": {
                "options": {
                    "cluster": name,
                }
            },
            "yt_config": {
                "proxy": proxy,
            }
        }
        odin_webservice_config["clusters"][name] = {
            "db_config": {
                "options": {
                    "cluster": name,
                }
            },
        }

    odin_checks_config = create_odin_checks_config()
    for check in odin_checks_config["checks"]:
        path = which(check)
        if path is None:
            raise RuntimeError("Failed to find check " + check)

    with open(odin_config_output, "w") as fout:
        json.dump(odin_config, fout, indent=4)

    with open(odin_webservice_config_output, "w") as fout:
        json.dump(odin_webservice_config, fout, indent=4)

    os.makedirs(odin_checks_directory)
    for check in odin_checks_config["checks"]:
        os.makedirs(os.path.join(odin_checks_directory, check))
        os.symlink(which(check), os.path.join(odin_checks_directory, check, check))

    with open(os.path.join(odin_checks_directory, "config.json"), "w") as fout:
        json.dump(odin_checks_config, fout, indent=4)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-cluster-proxy", required=True)
    parser.add_argument("--db-cluster-token-file", required=True)
    parser.add_argument("--check-cluster-token-file", required=True)
    parser.add_argument("--check-cluster-proxy", nargs="+")
    parser.add_argument("--odin-config-output", required=True)
    parser.add_argument("--odin-webservice-config-output", required=True)
    parser.add_argument("--odin-checks-directory", required=True)
    parser.add_argument("--odin-log-file")
    parser.add_argument("--odin-webservice-log-file")
    parser.add_argument("--odin-pids-file")
    args = parser.parse_args()

    func_args = dict(vars(args))
    func_args["check_clusters"] = [(cluster, cluster) for cluster in func_args.pop("check_cluster_proxy")]

    generate_odin_services_configs(**func_args)


if __name__ == "__main__":
    main()
