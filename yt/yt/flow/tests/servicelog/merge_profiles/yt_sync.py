from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode


def run_yt_sync(cluster, folder):
    tables = {
        "data_state": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "hash", "type": "uint64", "sort_order": "ascending"},
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"},
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
        "profiles": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "hash", "expression": "farm_hash(key)", "type": "uint64", "sort_order": "ascending"},
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"},
                    {"name": "second_value", "type": "int64"},
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
        "another_profiles": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "hash", "expression": "farm_hash(key)", "type": "uint64", "sort_order": "ascending"},
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"},
                    {"name": "second_value", "type": "int64"},
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

    pipelines = {
        "pipeline": {
            "default": {
                "$merge_presets": ["builtin:pipeline_preset"],
                "monitoring_project": "",
                "monitoring_cluster": "flow-dev",
            },
        },
    }

    stages = {
        "default": {},
        "main": {
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
        tables={**tables},
        pipelines=pipelines,
    )

    run_yt_sync_easy_mode(
        name="test",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
