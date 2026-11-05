from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode


def run_primary_yt_sync(cluster, folder, queue_tablet_count, pipeline_replica_cluster=None):
    queues = {
        "input_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "data", "type": "string"},
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
    producers = {}
    pipelines = {
        "pipeline": {
            "default": {
                "$merge_presets": ["builtin:pipeline_preset"],
                "monitoring_project": "",
                "monitoring_cluster": "",
            },
        },
    }

    stages_spec = StagesSpec(
        tables={**queues},
        consumers=consumers,
        producers=producers,
        pipelines=pipelines,
    )
    run_yt_sync(cluster, folder, stages_spec, pipeline_replica_cluster)


def run_remote_yt_sync(cluster, folder, queue_tablet_count):
    queues = {
        "output_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "data", "type": "string"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": queue_tablet_count}},
                },
            },
        },
        "output_queue_multi": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "data", "type": "string"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": queue_tablet_count}},
                },
            },
        },
    }
    consumers = {}
    producers = {
        "producer": {"default": {"$merge_presets": ["builtin:producer_preset"]}},
    }
    pipelines = {}

    stages_spec = StagesSpec(
        tables={**queues},
        consumers=consumers,
        producers=producers,
        pipelines=pipelines,
    )
    run_yt_sync(cluster, folder, stages_spec)


def run_yt_sync(cluster, folder, stages_spec, pipeline_replica_cluster=None):
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
    if pipeline_replica_cluster is not None:
        for preset in ("builtin:pipeline_sorted_table_preset", "builtin:pipeline_ordered_table_preset"):
            stages["test"]["presets"][preset] = {
                "chaos": False,
                "clusters": {
                    cluster: {"main": True},
                    pipeline_replica_cluster: {
                        "preferred_sync": True,
                        "replicated_table_tracker_enabled": False,
                        "attributes": {"tablet_cell_bundle": "default", "primary_medium": "default"},
                    },
                },
            }
    stages_spec.stages = stages

    run_yt_sync_easy_mode(
        name="test",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
