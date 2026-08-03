from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode
from yt.yt_sync.core.constants import QUEUE_META_COLUMNS


def run_yt_sync(cluster, folder, queue_tablet_count):
    queues = {
        "input_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "reduce_id", "type": "uint64"},
                    {"name": "event_id", "type": "int64"},
                    {"name": "data", "type": "string"},
                    *QUEUE_META_COLUMNS,
                ],
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {
                            "tablet_count": queue_tablet_count,
                            "commit_ordering": "strong",
                        }
                    },
                },
            },
        },
        "output_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "reduce_id", "type": "uint64"},
                    {"name": "event_id", "type": "int64"},
                    {"name": "copy_index", "type": "int64"},
                    {"name": "worker_pid", "type": "int64"},
                    *QUEUE_META_COLUMNS,
                ],
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {
                            "tablet_count": 1,
                            "commit_ordering": "strong",
                        }
                    },
                },
            },
        },
        "proto_events_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "reduce_id", "type": "uint64"},
                    {"name": "event_id", "type": "int64"},
                    *QUEUE_META_COLUMNS,
                ],
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {
                            "tablet_count": 1,
                            "commit_ordering": "strong",
                        }
                    },
                },
            },
        },
        "counts_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "reduce_id", "type": "uint64"},
                    {"name": "count", "type": "int64"},
                    *QUEUE_META_COLUMNS,
                ],
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {
                            "tablet_count": 1,
                            "commit_ordering": "strong",
                        }
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
        tables=queues,
        consumers=consumers,
        pipelines=pipelines,
    )

    run_yt_sync_easy_mode(
        name="test",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
