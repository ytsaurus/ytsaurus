import copy

import pytest

from yt.yt.flow.library.python.yt_sync_mini.yt_sync_mini import (
    LOCAL_PRESETS,
    _deep_merge,
    _resolve_attributes,
)
from yt.yt.flow.library.python.pipeline_tables import (
    PIPELINE_TABLES_PRESET,
)

# ---------------------------------------------------------------------------
# _deep_merge
# ---------------------------------------------------------------------------


def test_deep_merge_scalar_replaces():
    """Scalar in ``src`` replaces scalar in ``into``."""
    result = _deep_merge({"a": 1, "b": 2}, {"b": 99})
    assert result == {"a": 1, "b": 99}


def test_deep_merge_nested_dict_merges_keys():
    """Nested dicts merge key-by-key, ``src`` wins on conflicts."""
    into = {"mount_config": {"min_data_ttl": 0, "merge_rows_on_flush": True}}
    src = {"mount_config": {"merge_rows_on_flush": False, "auto_compaction_period": 3600000}}
    _deep_merge(into, src)
    assert into == {
        "mount_config": {
            "min_data_ttl": 0,
            "merge_rows_on_flush": False,
            "auto_compaction_period": 3600000,
        }
    }


def test_deep_merge_rejects_list_in_src():
    """Lists in ``src`` are rejected — Flow presets are not expected to
    contain list values, and silent replace-vs-concatenate semantics would
    be ambiguous."""
    with pytest.raises(TypeError, match="list-valued attribute 'x'"):
        _deep_merge({}, {"x": [1, 2, 3]})


def test_deep_merge_rejects_list_in_into():
    """A list already present in ``into`` is also rejected — the asymmetric
    case (replacing a list with a scalar) would be just as surprising."""
    with pytest.raises(TypeError, match="list-valued attribute 'x'"):
        _deep_merge({"x": [1, 2, 3]}, {"x": "replacement"})


def test_deep_merge_rejects_list_in_nested_dict():
    """Lists nested inside a dict that's being recursively merged are
    rejected at the level they appear."""
    with pytest.raises(TypeError, match="list-valued attribute 'replicas'"):
        _deep_merge(
            {"mount_config": {"min_data_ttl": 0}},
            {"mount_config": {"replicas": ["a", "b"]}},
        )


def test_deep_merge_rejects_dict_replacing_scalar():
    """A dict in ``src`` cannot replace a scalar in ``into`` for the same key
    (would erase the scalar with a structured value and obscure a preset bug)."""
    with pytest.raises(TypeError, match="mismatched shape"):
        _deep_merge({"x": 0}, {"x": {"a": 1}})


def test_deep_merge_rejects_scalar_replacing_dict():
    """A scalar in ``src`` cannot replace a dict in ``into`` for the same key
    (would silently erase a parent's nested config)."""
    with pytest.raises(TypeError, match="mismatched shape"):
        _deep_merge({"mount_config": {"min_data_ttl": 0}}, {"mount_config": 42})


def test_deep_merge_inserts_new_dict_into_empty_slot():
    """If ``into`` has no entry for ``key``, ``src``'s value (scalar or dict)
    is inserted as-is. Only same-key shape mismatches are rejected."""
    result = _deep_merge({}, {"mount_config": {"min_data_ttl": 0}})
    assert result == {"mount_config": {"min_data_ttl": 0}}
    result = _deep_merge({"existing": 1}, {"new": "value"})
    assert result == {"existing": 1, "new": "value"}


def test_deep_merge_deepcopies_src_values():
    """Values pulled from ``src`` are deep-copied; mutating the result must
    not affect ``src``. This is the safeguard against ``LOCAL_PRESETS``
    being mutated across repeated bootstrap calls.
    """
    src = {"mount_config": {"min_data_ttl": 0}}
    result = _deep_merge({}, src)
    result["mount_config"]["min_data_ttl"] = 99
    assert src == {"mount_config": {"min_data_ttl": 0}}


def test_deep_merge_returns_into():
    """``_deep_merge`` returns the mutated ``into`` for fluent chaining."""
    into = {}
    assert _deep_merge(into, {"a": 1}) is into


# ---------------------------------------------------------------------------
# _resolve_attributes — base cases
# ---------------------------------------------------------------------------


def test_resolve_empty_spec_returns_empty():
    assert _resolve_attributes({}, {}) == {}


def test_resolve_clusters_only_no_parents():
    """A spec with just ``clusters`` and no ``$merge_presets`` returns that
    cluster's flattened ``attributes``."""
    spec = {"clusters": {"_all_clusters": {"attributes": {"dynamic": True}}}}
    assert _resolve_attributes(spec, {}) == {"dynamic": True}


def test_resolve_multiple_clusters_collapsed():
    """Multiple cluster keys (wildcard + concrete) collapse into one dict."""
    spec = {
        "clusters": {
            "_all_clusters": {"attributes": {"dynamic": True}},
            "markov": {"attributes": {"tablet_cell_bundle": "flow"}},
        }
    }
    assert _resolve_attributes(spec, {}) == {
        "dynamic": True,
        "tablet_cell_bundle": "flow",
    }


def test_resolve_cluster_without_attributes_is_noop():
    """A cluster overlay missing the ``attributes`` key is silently skipped."""
    spec = {
        "clusters": {
            "_all_clusters": {"attributes": {"dynamic": True}},
            "markov": {"main": True},  # no `attributes` payload
        }
    }
    assert _resolve_attributes(spec, {}) == {"dynamic": True}


# ---------------------------------------------------------------------------
# _resolve_attributes — preset chain
# ---------------------------------------------------------------------------


def test_resolve_parent_preset_pulled_in():
    """Parent preset's attrs are merged before child's overlay."""
    registry = {
        "parent": {"clusters": {"_all_clusters": {"attributes": {"dynamic": True}}}},
    }
    spec = {
        "$merge_presets": ["parent"],
        "clusters": {"_all_clusters": {"attributes": {"erasure_codec": "none"}}},
    }
    assert _resolve_attributes(spec, registry) == {
        "dynamic": True,
        "erasure_codec": "none",
    }


def test_resolve_child_overrides_parent_scalar():
    """Child's scalar in the same key wins over parent's scalar."""
    registry = {
        "parent": {"clusters": {"_all_clusters": {"attributes": {"erasure_codec": "reed_solomon_3_3"}}}},
    }
    spec = {
        "$merge_presets": ["parent"],
        "clusters": {"_all_clusters": {"attributes": {"erasure_codec": "none"}}},
    }
    assert _resolve_attributes(spec, registry)["erasure_codec"] == "none"


def test_resolve_child_deep_merges_parent_dict():
    """Child's nested dict adds keys to parent's dict instead of replacing it."""
    registry = {
        "parent": {
            "clusters": {
                "_all_clusters": {"attributes": {"mount_config": {"min_data_ttl": 0, "merge_rows_on_flush": True}}}
            },
        },
    }
    spec = {
        "$merge_presets": ["parent"],
        "clusters": {"_all_clusters": {"attributes": {"mount_config": {"enable_lookup_hash_table": True}}}},
    }
    assert _resolve_attributes(spec, registry)["mount_config"] == {
        "min_data_ttl": 0,
        "merge_rows_on_flush": True,
        "enable_lookup_hash_table": True,
    }


def test_resolve_unknown_preset_raises():
    """A ``$merge_presets`` entry absent from the registry is a hard error:
    every name must resolve."""
    spec = {
        "$merge_presets": ["builtin:table_preset", "known"],
        "clusters": {"_all_clusters": {"attributes": {"x": 1}}},
    }
    registry = {"known": {"clusters": {"_all_clusters": {"attributes": {"y": 2}}}}}
    with pytest.raises(KeyError, match="builtin:table_preset"):
        _resolve_attributes(spec, registry)


def test_resolve_chain_depth_first():
    """``$merge_presets`` chain is walked depth-first: grandparent first,
    then parent, then this spec."""
    registry = {
        "grandparent": {"clusters": {"_all_clusters": {"attributes": {"a": 1, "b": "from_grandparent"}}}},
        "parent": {
            "$merge_presets": ["grandparent"],
            "clusters": {"_all_clusters": {"attributes": {"b": "from_parent", "c": "from_parent"}}},
        },
    }
    spec = {
        "$merge_presets": ["parent"],
        "clusters": {"_all_clusters": {"attributes": {"c": "from_child"}}},
    }
    assert _resolve_attributes(spec, registry) == {
        "a": 1,
        "b": "from_parent",  # parent beats grandparent
        "c": "from_child",  # child beats parent
    }


def test_resolve_multiple_parents_left_to_right():
    """When ``$merge_presets`` lists multiple parents, later parents win
    over earlier ones (matches yt_sync's left-to-right merge order)."""
    registry = {
        "first": {"clusters": {"_all_clusters": {"attributes": {"key": "first"}}}},
        "second": {"clusters": {"_all_clusters": {"attributes": {"key": "second"}}}},
    }
    spec = {"$merge_presets": ["first", "second"]}
    assert _resolve_attributes(spec, registry) == {"key": "second"}


def test_resolve_does_not_mutate_registry():
    """Repeated resolution of the same preset must not bleed state into the
    shared registry (regression guard for the deepcopy-on-merge contract)."""
    snapshot = copy.deepcopy(LOCAL_PRESETS)
    for _ in range(3):
        for table_preset in PIPELINE_TABLES_PRESET.values():
            _resolve_attributes(table_preset, LOCAL_PRESETS)
    assert LOCAL_PRESETS == snapshot


# ---------------------------------------------------------------------------
# Real pipeline presets — end-to-end smoke
# ---------------------------------------------------------------------------


def _resolved(table_name):
    return _resolve_attributes(PIPELINE_TABLES_PRESET[table_name], LOCAL_PRESETS)


def test_real_preset_timers_inherits_sorted_base():
    """``timers`` should carry the base sorted-table preset attributes
    (compression/erasure/optimize_for/mount_config) plus its own
    ``in_memory_mode: none`` and ``mount_config.enable_lookup_hash_table: False``."""
    attrs = _resolved("timers")
    # From PIPELINE_SORTED_TABLE_PRESET.
    assert attrs["dynamic"] is True
    assert attrs["optimize_for"] == "scan"
    assert attrs["chunk_format"] == "table_versioned_columnar"
    assert attrs["erasure_codec"] == "reed_solomon_3_3"
    assert attrs["hunk_erasure_codec"] == "reed_solomon_3_3"
    assert attrs["mount_config"]["min_data_ttl"] == 0
    assert attrs["mount_config"]["merge_rows_on_flush"] is True
    assert attrs["mount_config"]["merge_deletions_on_flush"] is True
    assert attrs["hunk_chunk_reader"] == {"fragment_read_hedging_delay": 50}
    # From timers' own overlay.
    assert attrs["in_memory_mode"] == "none"
    assert attrs["mount_config"]["enable_lookup_hash_table"] is False
