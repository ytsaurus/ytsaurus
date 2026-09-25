from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode


def run_yt_sync(cluster, folder):
    # The noop pipeline reads no tables: yt_sync creates only the pipeline node.
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

    run_yt_sync_easy_mode(
        name="go_noop",
        stages_spec=StagesSpec(stages=stages, pipelines=pipelines),
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
