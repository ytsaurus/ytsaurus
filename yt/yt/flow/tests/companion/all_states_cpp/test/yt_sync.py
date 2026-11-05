from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode


def run_yt_sync(cluster, folder, queue_tablet_count):
    queues = {
        "input_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "word", "type": "string"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": queue_tablet_count}},
                },
            },
        },
        "source_output_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "word", "type": "string"},
                    {"name": "source_sequence", "type": "int64"},
                    {"name": "tag", "type": "string"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": queue_tablet_count}},
                },
            },
        },
        "unique_words_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "word", "type": "string"},
                    {"name": "tag_weight", "type": "int64"},
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
        "word_metadata": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "hash", "expression": "farm_hash(word)", "type": "uint64", "sort_order": "ascending"},
                    {"name": "word", "type": "string", "sort_order": "ascending"},
                    {"name": "tag", "type": "string"},
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
        "tag_metadata": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "tag_hash", "expression": "farm_hash(tag)", "type": "uint64", "sort_order": "ascending"},
                    {"name": "tag", "type": "string", "sort_order": "ascending"},
                    {"name": "weight", "type": "int64"},
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
        "word_state": {
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
        tables={**queues, **tables},
        consumers=consumers,
        pipelines=pipelines,
    )

    run_yt_sync_easy_mode(
        name="wordcount",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
    )
