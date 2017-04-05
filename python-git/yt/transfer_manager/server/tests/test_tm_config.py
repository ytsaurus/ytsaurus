config = {
    "thread_count": 5,
    "execute_task_grace_termination_sleep_timeout": 500,
    "tasks_cache_expiration_timeout": 4000,
    "availability_graph": {
        "clusterA": [
            "clusterB"
        ],
        "clusterB": [
            "clusterA"
        ],
    },
    "port": None,
    "running_jobs_limit_per_direction": 1000,
    "enable_detailed_traceback": False,
    "yt_client_config": {
        "api_version": "v3",
        "operation_tracker": {
            "ignore_stderr_if_download_failed": True
        }
    },
    "clusters": {
        "clusterA": {
            "type": "yt",
            "version": 7,
            "table_url_pattern": "https://localhost/",
            "options": {
                "proxy": None
            },
        },
        "clusterB": {
            "type": "yt",
            "version": 7,
            "table_url_pattern": "https://localhost/",
            "options": {
                "proxy": None
            },
        }
    },
    "backend_tag": "dev",
    "running_tasks_limit_per_direction": 30,
    "profiling": {
        "queue_limit": 1000
    },
    "incoming_requests_limit": 200,
    "yt_backend_options": {
        "config": {
            "api_version": "v3"
        },
        "proxy": None
    },
    "mutating_requests_cache_expiration_timeout": 300000,
    "clusters_config_reload_timeout": 10000,
    "default_kiwi_user": "gemini",
    "mutating_requests_cache_size": 1000,
    "small_table_size_threshold": 200000000,
    "logging": {
        "filename": None,
        "port": None
    },
    "pending_tasks_limit": 500,
    "message_reader_sleep_timeout": 300,
    "max_tasks_to_run_per_scheduling_round": 10,
    "hadoop_transmitter": {
        "operation_pattern": "https://localhost/",
        "type": "airflow",
        "name": "raccoon",
        "options": {
            "protocol": "https",
            "address": "localhost"
        }
    },
    "kiwi_transmitter": "clusterA",
    "path": "//transfer_manager",
    "error_details_length_limit": 50000,
    "tasks_limit_per_user": 40,
    "task_executor": {
        "port": None
    },
    "pack_yt_wrapper": False,
    "pack_yson_bindings": False
}
