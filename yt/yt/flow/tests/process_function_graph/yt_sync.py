from yt.yt_sync.core.constants import QUEUE_META_COLUMNS
from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode


def run_yt_sync(cluster: str, folder: str) -> None:
    queues = {}
    for name in ("left_queue", "right_queue"):
        queues[name] = {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "key", "type": "uint64"},
                    *QUEUE_META_COLUMNS,
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": 1}},
                },
            },
        }

    queues["output_queue"] = {
        "default": {
            "$merge_presets": ["builtin:table_preset"],
            "schema": [
                {"name": "key", "type": "uint64"},
                {"name": "count", "type": "int64"},
                {"name": "branch_count", "type": "int64"},
                {"name": "kind", "type": "int64"},
                *QUEUE_META_COLUMNS,
            ],
            "clusters": {
                "_all_data_clusters": {"attributes": {"tablet_count": 1}},
            },
        },
    }

    consumers = {}
    for name in ("left", "right"):
        consumers[f"{name}_consumer"] = {
            "default": {
                "$merge_presets": ["builtin:consumer_preset"],
                "in_stage_queues": {f"{name}_queue": {"vital": True}},
            },
        }

    stages_spec = StagesSpec(
        stages={
            "default": {},
            "test": {
                "folder": folder,
                "presets": {
                    "builtin:storage_preset": {
                        "clusters": {cluster: {"attributes": {"primary_medium": "default"}}},
                    },
                    "builtin:table_preset": {
                        "clusters": {cluster: {"attributes": {"tablet_cell_bundle": "default"}}},
                    },
                },
            },
        },
        tables=queues,
        consumers=consumers,
        pipelines={
            "pipeline": {
                "default": {
                    "$merge_presets": ["builtin:pipeline_preset"],
                    "monitoring_project": "",
                    "monitoring_cluster": "",
                },
            },
        },
    )
    run_yt_sync_easy_mode(
        name="process_function_graph",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
