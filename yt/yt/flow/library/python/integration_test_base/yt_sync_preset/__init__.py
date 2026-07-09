from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode


def run_yt_sync(
    cluster,
    folder,
    tablet_cell_bundle="default",
    primary_medium="default",
    add_input_queue_and_consumer=False,
    input_queue_schema=None,
    input_queue_tablet_count=1,
    add_output_queue=False,
    output_queue_schema=None,
    output_queue_tablet_count=1,
):
    """Run yt_sync that bootstraps Cypress nodes for a flow integration test.

    Always creates a single pipeline named ``pipeline``.

    If ``add_input_queue_and_consumer`` is set, additionally creates a queue
    named ``input_queue`` and a consumer named ``consumer`` attached to it.

    If ``add_output_queue`` is set, additionally creates a queue named
    ``output_queue`` (no consumer attached).
    """
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
                "builtin:storage_preset": {"clusters": {cluster: {"attributes": {"primary_medium": primary_medium}}}},
                "builtin:table_preset": {
                    "clusters": {cluster: {"attributes": {"tablet_cell_bundle": tablet_cell_bundle}}}
                },
            },
        },
    }

    tables = {}
    consumers = {}
    if add_input_queue_and_consumer:
        if input_queue_schema is None:
            raise ValueError("input_queue_schema is required when add_input_queue_and_consumer is True")

        tables["input_queue"] = {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": input_queue_schema,
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": input_queue_tablet_count}},
                },
            },
        }
        consumers["consumer"] = {
            "default": {
                "$merge_presets": ["builtin:consumer_preset"],
                "in_stage_queues": {"input_queue": {"vital": True}},
            },
        }

    if add_output_queue:
        if output_queue_schema is None:
            raise ValueError("output_queue_schema is required when add_output_queue is True")

        tables["output_queue"] = {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": output_queue_schema,
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": output_queue_tablet_count}},
                },
            },
        }

    stages_spec = StagesSpec(
        stages=stages,
        pipelines=pipelines,
        tables=tables,
        consumers=consumers,
    )

    run_yt_sync_easy_mode(
        name="test",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )
