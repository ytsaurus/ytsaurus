from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode


def run_yt_sync(cluster, folder):
    tables = {
        # Shared keyed state populated by the loader from the static reference
        # table and read by the enricher to join the realtime stream.
        "reference_state": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {
                        "name": "hash",
                        "type": "uint64",
                        "expression": "farm_hash(key)",
                        "sort_order": "ascending",
                    },
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "normalized_name", "type": "string"},
                ],
            },
        },
    }

    queues = {
        "event_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "key", "type": "uint64"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": 1}},
                },
            },
        },
        "output_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "key", "type": "uint64"},
                    {"name": "name", "type": "string"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": 1}},
                },
            },
        },
    }

    consumers = {
        "event_consumer": {
            "default": {
                "$merge_presets": ["builtin:consumer_preset"],
                "in_stage_queues": {"event_queue": {"vital": True}},
            },
        },
    }

    producers = {
        "output_producer": {"default": {"$merge_presets": ["builtin:producer_preset"]}},
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
        tables={**queues, **tables},
        consumers=consumers,
        producers=producers,
        pipelines=pipelines,
    )

    run_yt_sync_easy_mode(
        name="cpp_static_table_join",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
