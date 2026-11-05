import yt.wrapper as yt

from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode

# Schema of every reference snapshot: a sorted dynamic table keyed exactly like
# the joiner's group_by_schema (hash = farm_hash(key), key) plus the looked-up
# name column. A sorted dynamic table requires unique_keys.
REFERENCE_SCHEMA = yt.yson.to_yson_type(
    [
        {"name": "hash", "type": "uint64", "expression": "farm_hash(key)", "sort_order": "ascending"},
        {"name": "key", "type": "uint64", "sort_order": "ascending"},
        {"name": "name", "type": "string"},
    ],
    attributes={"unique_keys": True},
)


def run_yt_sync(cluster, folder):
    queues = {
        "event_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "key", "type": "uint64"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": 1}},
                },
            },
        },
        "output_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "key", "type": "uint64"},
                    {"name": "name", "type": "string"},
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
        "event_consumer": {
            "default": {
                "$merge_presets": ["builtin:consumer_preset"],
                "in_stage_queues": {"event_queue": {"vital": True}},
            },
        },
    }

    producers = {
        "output_producer": {"default": {"$merge_presets": ["builtin:producer_preset"]}},
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
        name="python_external_state_join",
        stages_spec=stages_spec,
        args=["--scenario", "ensure", "--parallel-factor", "0", "--commit"],
        exit_on_finish=False,
        setup_logging=False,
    )


def build_reference_table(client, path, rows):
    """Builds one sorted, mounted dynamic reference snapshot at `path`."""
    client.create("table", path, attributes={"schema": REFERENCE_SCHEMA})
    client.alter_table(path, dynamic=True)
    client.mount_table(path, sync=True)
    client.insert_rows(path, rows)


def repoint_current(client, link_path, target_path):
    """Atomically repoints the "current version" symlink to a new snapshot."""
    client.link(target_path, link_path, force=True)
