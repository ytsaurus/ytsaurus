from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode


def run_yt_sync(cluster, folder, queue_tablet_count):
    tables = {
        "word_counts": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "hash", "expression": "farm_hash(word)", "type": "uint64", "sort_order": "ascending"},
                    {"name": "word", "type": "string", "sort_order": "ascending"},
                    {"name": "count", "type": "int64"},
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

    queues = {
        "input_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "text", "type": "string"},
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

    stages_spec = StagesSpec(
        stages=stages,
        tables={**tables, **queues},
        consumers=consumers,
        pipelines=pipelines,
    )

    run_yt_sync_easy_mode(
        name="word_count",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
