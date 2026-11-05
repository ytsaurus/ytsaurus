from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode


def run_yt_sync(cluster, folder, queue_tablet_count=1):
    tables = {
        # Pre-built sorted dynamic table that the multiplexer iterates over.
        # Schema: [hash farm_hash(key), key, secondary_key] (key columns) + region (value).
        "secondary_index": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {
                        "name": "hash",
                        "type": "uint64",
                        "sort_order": "ascending",
                        "expression": "farm_hash(key)",
                    },
                    {"name": "key", "type": "string", "required": True, "sort_order": "ascending"},
                    {"name": "secondary_key", "type": "int64", "required": True, "sort_order": "ascending"},
                    {"name": "region", "type": "string"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": 1}},
                },
            },
        },
    }

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
                    {"name": "secondary_key", "type": "int64"},
                    {"name": "region", "type": "string"},
                    {"name": "payload", "type": "string"},
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
        name="multiplexer_test",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
