from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode


def run_yt_sync(cluster, folder, queue_tablet_count=1, with_external_state=False, with_swift_state=False):
    queues = {
        "input_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "key", "type": "string"},
                    {"name": "payload", "type": "string"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": queue_tablet_count}},
                },
            },
        },
        "output_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "key", "type": "string"},
                    {"name": "payload", "type": "string"},
                    {"name": "visit_index", "type": "int64"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": queue_tablet_count}},
                },
            },
        },
    }

    consumers = {
        "consumer": {
            "default": {
                "$merge_presets": ["builtin:consumer_preset"],
                "in_stage_queues": {"input_queue": {"vital": True}},
            },
        },
    }

    pipelines = {
        "pipeline": {
            "default": {
                "$merge_presets": ["builtin:pipeline_preset"],
                "monitoring_project": "",
                "monitoring_cluster": "",
            },
        },
    }

    stages = {
        "default": {},
        "test": {
            "folder": folder,
            "presets": {
                "builtin:storage_preset": {"clusters": {cluster: {"attributes": {"primary_medium": "default"}}}},
                "builtin:table_preset": {"clusters": {cluster: {"attributes": {"tablet_cell_bundle": "default"}}}},
            },
        },
    }

    tables = dict(queues)
    if with_external_state:
        tables["user_state"] = {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "hash", "expression": "farm_hash(key)", "type": "uint64", "sort_order": "ascending"},
                    {"name": "key", "type": "string", "sort_order": "ascending"},
                    {"name": "payload", "type": "string"},
                    {"name": "visit_index", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {
                            "optimize_for": "scan",
                            "chunk_format": "table_versioned_columnar",
                            "in_memory_mode": "uncompressed",
                            "enable_dynamic_store_read": True,
                        },
                    },
                },
            },
        }

    if with_swift_state:
        tables["state"] = {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "hash", "expression": "farm_hash(key)", "type": "uint64", "sort_order": "ascending"},
                    {"name": "key", "type": "string", "sort_order": "ascending"},
                    {"name": "payload", "type": "string"},
                    {"name": "visit_count", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {
                            "in_memory_mode": "uncompressed",
                            "enable_dynamic_store_read": True,
                        },
                    },
                },
            },
        }

    stages_spec = StagesSpec(
        stages=stages,
        tables=tables,
        consumers=consumers,
        pipelines=pipelines,
    )

    run_yt_sync_easy_mode(
        name="key_visitor_test",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
