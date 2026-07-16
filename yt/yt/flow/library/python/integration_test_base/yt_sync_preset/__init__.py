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
    output_queues=None,
):
    """Run yt_sync that bootstraps Cypress nodes for a flow integration test.

    Always creates a single pipeline named ``pipeline``.

    If ``add_input_queue_and_consumer`` is set, additionally creates a queue
    named ``input_queue`` and a consumer named ``consumer`` attached to it.

    Output queues (no consumers attached) are requested either as the single
    queue named ``output_queue`` via ``add_output_queue``, or as arbitrarily
    named ones via ``output_queues``: a list of dicts, each ``{"name": str,
    "schema": list, "tablet_count": int}`` (``tablet_count`` defaults to 1).
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

    requested_queues = list(output_queues or [])
    if add_output_queue:
        if output_queue_schema is None:
            raise ValueError("output_queue_schema is required when add_output_queue is True")

        requested_queues.append(
            {"name": "output_queue", "schema": output_queue_schema, "tablet_count": output_queue_tablet_count}
        )

    tables = {}
    consumers = {}
    if add_input_queue_and_consumer:
        if input_queue_schema is None:
            raise ValueError("input_queue_schema is required when add_input_queue_and_consumer is True")

        requested_queues.append(
            {"name": "input_queue", "schema": input_queue_schema, "tablet_count": input_queue_tablet_count}
        )
        consumers["consumer"] = {
            "default": {
                "$merge_presets": ["builtin:consumer_preset"],
                "in_stage_queues": {"input_queue": {"vital": True}},
            },
        }

    for queue in requested_queues:
        if queue["name"] in tables:
            raise ValueError(f"duplicate queue name: {queue['name']}")

        tables[queue["name"]] = {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": queue["schema"],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": queue.get("tablet_count", 1)}},
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
