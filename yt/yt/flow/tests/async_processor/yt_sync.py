from yt.yt_sync.core.constants import QUEUE_META_COLUMNS
from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode

INPUT_TABLET_COUNT = 2


def make_queue(schema, tablet_count):
    return {
        "default": {
            "$merge_presets": ["builtin:table_preset"],
            "schema": [*schema, *QUEUE_META_COLUMNS],
            "clusters": {
                "_all_data_clusters": {"attributes": {"tablet_count": tablet_count}},
            },
        },
    }


def run_yt_sync(cluster: str, folder: str) -> None:
    queues = {
        "input_queue": make_queue([{"name": "event_id", "type": "int64"}], INPUT_TABLET_COUNT),
        "transform_output": make_queue(
            [
                {"name": "event_id", "type": "int64"},
                {"name": "result", "type": "int64"},
                {"name": "attempt", "type": "int64"},
            ],
            tablet_count=1,
        ),
    }

    consumers = {
        "consumer": {
            "default": {
                "$merge_presets": ["builtin:consumer_preset"],
                "in_stage_queues": {"input_queue": {"vital": True}},
            },
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
        name="async_processor",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
