from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode

INPUT_QUEUE_SCHEMA = [
    {"name": "data", "type": "string"},
    {"name": "key", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]


def run_yt_sync(cluster, folder, skip_input_queue=False):
    queues = {
        "t_output": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "data", "type": "string"},
                    {"name": "key", "type": "string"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
            },
        },
    }

    # Add input queue only if not skipped
    if not skip_input_queue:
        queues["t_input"] = {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": INPUT_QUEUE_SCHEMA,
            },
        }

    consumers = {
        "t_input_consumer": {
            "default": {
                "$merge_presets": ["builtin:consumer_preset"],
                "in_stage_queues": {"t_input": {"vital": True}} if not skip_input_queue else {},
            },
        },
    }

    producers = {
        "t_output_producer": {"default": {"$merge_presets": ["builtin:producer_preset"]}},
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
        tables=queues,
        consumers=consumers,
        producers=producers,
        pipelines=pipelines,
    )

    run_yt_sync_easy_mode(
        name="test",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
