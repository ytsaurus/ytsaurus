"""Create the YT objects a Flow pipeline needs: the pipeline node with its
inner dynamic tables, plus user tables/queues, consumers and producers.

The local preset-merger (:func:`_resolve_attributes`) is a small subset of
yt_sync's spec_merger:

* Walks ``$merge_presets`` depth-first against :data:`LOCAL_PRESETS`.
* Fetches preset names from the local registry.
* ``clusters.<name>.attributes`` is collapsed across all cluster keys
  (wildcards ``_all_clusters`` / ``_all_data_clusters`` and concrete cluster
  names are treated identically); the bootstrap helper talks to a single
  cluster, so the cluster taxonomy is meaningless here.
* Nested dicts are deep-merged, scalars are replaced.

Usage::

    create_pipeline(client, "//tmp/my_pipeline")
"""

import copy
from typing import Any

import yt.yson as yson

from yt.yt.flow.library.python.pipeline_tables import PIPELINE_FILE_PRESET
from yt.yt.flow.library.python.pipeline_tables import PIPELINE_ORDERED_TABLE_PRESET
from yt.yt.flow.library.python.pipeline_tables import PIPELINE_QUEUES
from yt.yt.flow.library.python.pipeline_tables import PIPELINE_QUEUES_PRESET
from yt.yt.flow.library.python.pipeline_tables import PIPELINE_SORTED_TABLE_PRESET
from yt.yt.flow.library.python.pipeline_tables import PIPELINE_TABLES
from yt.yt.flow.library.python.pipeline_tables import PIPELINE_TABLES_PRESET

# Pipeline-map-node attribute.
PIPELINE_FORMAT_VERSION_ATTRIBUTE = "pipeline_format_version"
CURRENT_PIPELINE_FORMAT_VERSION = 1

# Queue/consumer/producer constants mirrored from yt_sync. The source of
# truth for Yandex-internal deployments is
# ``yt/yt_sync/core/constants/__init__.py``; these copies make the
# opensource bootstrap self-contained. Keep them identical (guarded by a
# drift test in ``tests/``).

QUEUE_META_COLUMNS: list[dict[str, Any]] = [
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

CONSUMER_SCHEMA: list[dict[str, Any]] = [
    {"name": "queue_cluster", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "queue_path", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "partition_index", "type": "uint64", "sort_order": "ascending", "required": True},
    {"name": "offset", "type": "uint64", "required": True},
    {"name": "meta", "type": "any", "required": False},
]

CONSUMER_ATTRS: dict[str, Any] = {
    "dynamic": True,
    "enable_dynamic_store_read": True,
    "mount_config": {
        "merge_rows_on_flush": True,
        "min_compaction_store_count": 2,
        "min_data_ttl": 0,
        "min_partitioning_data_size": 1,
    },
    "treat_as_queue_consumer": True,
}

PRODUCER_SCHEMA: list[dict[str, Any]] = [
    {"name": "queue_cluster", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "queue_path", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "session_id", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "sequence_number", "type": "int64", "required": True},
    {"name": "epoch", "type": "int64", "required": True},
    {"name": "user_meta", "type": "any", "required": False},
    {"name": "system_meta", "type": "any", "required": False},
]

PRODUCER_ATTRS: dict[str, Any] = {
    "dynamic": True,
    "enable_dynamic_store_read": True,
    "mount_config": {
        "merge_rows_on_flush": True,
        "min_compaction_store_count": 2,
        "min_data_ttl": 0,
        "min_partitioning_data_size": 1,
    },
    "treat_as_queue_producer": True,
}

# Key under which a spec dict lists parent presets to merge in first.
PRESET_MERGE_KEY = "$merge_presets"

# Local registry consumed by :func:`_resolve_attributes`, and the single
# list of preset names known to yt_sync_mini (easy_mode validates every
# referenced preset name against it). Any ``$merge_presets`` name referenced
# by a spec must be present here; unknown names raise ``KeyError``.
#
# ``builtin:storage_preset`` and ``builtin:table_preset`` mirror yt_sync.
LOCAL_PRESETS = {
    "builtin:storage_preset": {},
    "builtin:table_preset": {
        "$merge_presets": ["builtin:storage_preset"],
        "clusters": {"_all_clusters": {"attributes": {"dynamic": True}}},
    },
    "builtin:pipeline_sorted_table_preset": PIPELINE_SORTED_TABLE_PRESET,
    "builtin:pipeline_ordered_table_preset": PIPELINE_ORDERED_TABLE_PRESET,
    "builtin:pipeline_file_preset": PIPELINE_FILE_PRESET,
    "builtin:consumer_preset": {
        "$merge_presets": ["builtin:table_preset"],
        "clusters": {"_all_clusters": {"attributes": CONSUMER_ATTRS}},
    },
    "builtin:producer_preset": {
        "$merge_presets": ["builtin:table_preset"],
        "clusters": {"_all_clusters": {"attributes": PRODUCER_ATTRS}},
    },
    # The pipeline itself is created by :func:`create_pipeline`; the preset
    # only needs to be known by name.
    "builtin:pipeline_preset": {},
}


def _deep_merge(into, src):
    """Recursively merge ``src`` into ``into`` in place; ``src`` wins on conflicts.

    Two value kinds are recognised: ``dict`` (recursive merge) and scalar
    (replace). Any other shape (list, set, tuple, ...) and any kind mismatch
    on a shared key is rejected.

    Values pulled from ``src`` are deep-copied so the caller is free to mutate
    the result without affecting shared registry entries (e.g.
    :data:`LOCAL_PRESETS`).
    """
    for key, value in src.items():
        existing = into.get(key)
        src_is_dict = isinstance(value, dict)
        existing_is_dict = isinstance(existing, dict)

        if isinstance(value, list) or isinstance(existing, list):
            raise TypeError(
                f"refusing to merge list-valued attribute {key!r}: "
                "Flow pipeline presets are not expected to contain lists; "
                "extend _deep_merge with an explicit policy if this changes"
            )
        if key in into and src_is_dict != existing_is_dict:
            raise TypeError(
                f"refusing to merge attribute {key!r} of mismatched shape: "
                f"{type(existing).__name__} vs {type(value).__name__}; "
                "preset authors must keep the value kind (dict vs scalar) "
                "consistent across the merge chain"
            )

        if src_is_dict and existing_is_dict:
            _deep_merge(existing, value)
        else:
            into[key] = copy.deepcopy(value)
    return into


def _resolve_attributes(spec, registry):
    """Flatten a yt_sync-style spec dict into a single attribute dict.

    :param spec: dict that may contain ``$merge_presets`` and ``clusters``.
    :param registry: ``{preset_name: spec_dict}`` mapping. Every name listed
        in ``$merge_presets`` must be present.
    :return: merged ``attributes`` dict (deep merge, depth-first parents-first).
    """
    merged: dict = {}

    for parent_name in spec.get(PRESET_MERGE_KEY, []):
        if parent_name not in registry:
            raise KeyError(
                f"unknown preset {parent_name!r} referenced via "
                f"{PRESET_MERGE_KEY!r}; known presets: "
                f"{sorted(registry)}"
            )
        _deep_merge(merged, _resolve_attributes(registry[parent_name], registry))

    # Collapse ``clusters.<any>.attributes`` (wildcards and concrete names
    # alike) into ``merged``.
    for cluster_overlay in spec.get("clusters", {}).values():
        attrs = cluster_overlay.get("attributes")
        if attrs:
            _deep_merge(merged, attrs)

    return merged


def _build_schema(columns):
    """Wrap schema columns in a YSON list carrying ``strict`` / ``unique_keys``.

    All Flow inner *tables* are strict + unique-keyed; the *queue*
    (``controller_logs``) is strict but not unique-keyed (no sort columns).
    """
    has_key_columns = any(column.get("sort_order") for column in columns)
    return yson.to_yson_type(
        list(columns),
        attributes={"strict": True, "unique_keys": has_key_columns},
    )


def _table_attributes(name, schema_columns, table_preset):
    """Build the final ``attributes`` dict for a single inner table."""
    attrs = _resolve_attributes(table_preset, LOCAL_PRESETS)
    attrs["schema"] = _build_schema(schema_columns)
    return attrs


def create_table(client, path, schema, attributes=None):
    """Create a dynamic table and mount it; a schema without key columns
    makes it a queue.

    Idempotent: an existing table is left untouched.

    :param client: ``yt.wrapper.YtClient`` instance.
    :param schema: list of column dicts (yt_sync spec format).
    :param attributes: extra table attributes (e.g. ``tablet_count``).
    """
    attrs = {"dynamic": True}
    if attributes:
        _deep_merge(attrs, attributes)
    attrs["schema"] = _build_schema(schema)
    client.create("table", path, recursive=True, ignore_existing=True, attributes=attrs)
    client.mount_table(path, sync=True)


def register_consumer(client, queue_path, consumer_path, vital):
    """Register the consumer for a queue (both must exist and be mounted)."""
    client.register_queue_consumer(queue_path, consumer_path, vital=vital)


def create_pipeline(client, path):
    """Create the pipeline node, its inner dynamic tables, and mount them.

    Idempotent: re-running against an existing pipeline is a no-op.

    :param client: ``yt.wrapper.YtClient`` instance.
    :param path: Cypress path of the pipeline node to create.
    """
    # 1. Pipeline map node.
    client.create(
        "map_node",
        path,
        recursive=True,
        ignore_existing=True,
        attributes={PIPELINE_FORMAT_VERSION_ATTRIBUTE: CURRENT_PIPELINE_FORMAT_VERSION},
    )

    # 2. Inner tables, all in a single master transaction (matches
    # NYT::NFlow::CreatePipelineNode semantics: either all tables are
    # created and visible to the controller, or none are).
    pipeline_root = path.rstrip("/")
    items = list(PIPELINE_TABLES.items()) + list(PIPELINE_QUEUES.items())
    presets = {**PIPELINE_TABLES_PRESET, **PIPELINE_QUEUES_PRESET}

    with client.Transaction(type="master", attributes={"title": f"Create pipeline {path}"}):
        for name, descriptor in items:
            table_preset = presets.get(name, {})
            client.create(
                "table",
                f"{pipeline_root}/{name}",
                ignore_existing=True,
                attributes=_table_attributes(name, descriptor["schema"], table_preset),
            )

    # 3. Mount.
    for name, _ in items:
        client.mount_table(f"{pipeline_root}/{name}", sync=True)
