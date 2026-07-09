from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode


def run_yt_sync(cluster, folder):
    tables = {
        "output_table": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {
                        "name": "Hash",
                        "type": "uint64",
                        "sort_order": "ascending",
                        "expression": "farm_hash(UserId)",
                    },
                    {"name": "UserId", "type": "string", "required": True, "sort_order": "ascending"},
                    {"name": "Total", "type": "int64", "required": True},
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
                    {"name": "UserId", "type": "string"},
                    {"name": "Amount", "type": "int64"},
                    {"name": "flow_queue_meta", "type": "any"},
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
                "builtin:table_preset": {
                    "clusters": {cluster: {"main": True, "attributes": {"tablet_cell_bundle": "default"}}}
                },
            },
        },
    }

    stages_spec = StagesSpec(
        stages=stages,
        tables={**tables, **queues},
        consumers=consumers,
        producers={},
        pipelines=pipelines,
    )

    run_yt_sync_easy_mode(
        name="test",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
