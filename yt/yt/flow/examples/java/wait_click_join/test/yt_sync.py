from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode


def run_yt_sync(cluster, folder, queue_tablet_count):
    queues = {
        "action_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "hit_id", "type": "string"},
                    {"name": "hit_time", "type": "uint64"},
                    {"name": "action_time", "type": "uint64"},
                    {"name": "is_click", "type": "boolean"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": queue_tablet_count}},
                },
            },
        },
        "hit_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "hit_id", "type": "string"},
                    {"name": "hit_time", "type": "uint64"},
                    {"name": "hit_payload", "type": "string"},
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
                    {"name": "hit_id", "type": "string"},
                    {"name": "hit_time", "type": "uint64"},
                    {"name": "is_click", "type": "boolean"},
                    {"name": "show_time", "type": "uint64"},
                    {"name": "click_time", "type": "uint64"},
                    {"name": "hit_payload", "type": "string"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": queue_tablet_count}},
                },
            },
        },
    }

    tables = {
        "join_state": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "hash", "expression": "farm_hash(hit_id)", "type": "uint64", "sort_order": "ascending"},
                    {"name": "hit_id", "type": "string", "sort_order": "ascending"},
                    {"name": "hit_time", "type": "uint64", "sort_order": "ascending"},
                    {"name": "show_time", "type": "uint64"},
                    {"name": "click_time", "type": "uint64"},
                    {"name": "hit_payload", "type": "string"},
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
        },
    }

    consumers = {
        "consumer": {
            "default": {
                "$merge_presets": ["builtin:consumer_preset"],
                "in_stage_queues": {
                    "action_queue": {"vital": True},
                    "hit_queue": {"vital": True},
                },
            },
        },
    }

    producers = {
        "producer": {"default": {"$merge_presets": ["builtin:producer_preset"]}},
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
                # For consumer and pipeline in fact.
                "builtin:storage_preset": {"clusters": {cluster: {"attributes": {"primary_medium": "default"}}}},
                "builtin:table_preset": {"clusters": {cluster: {"attributes": {"tablet_cell_bundle": "default"}}}},
            },
        },
    }

    stages_spec = StagesSpec(
        stages=stages,
        tables={**queues, **tables},
        consumers=consumers,
        producers=producers,
        pipelines=pipelines,
    )

    run_yt_sync_easy_mode(
        name="java_wait_click_join",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
