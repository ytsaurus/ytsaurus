from copy import deepcopy
from dataclasses import asdict

import pytest

from yt.yt_sync.core.constants import CONSUMER_ATTRS
from yt.yt_sync.core.constants import CONSUMER_SCHEMA
from yt.yt_sync.core.constants import PIPELINE_FILES
from yt.yt_sync.core.constants import PIPELINE_QUEUES
from yt.yt_sync.core.constants import PIPELINE_TABLES
from yt.yt_sync.core.constants import PRODUCER_ATTRS
from yt.yt_sync.core.constants import PRODUCER_SCHEMA

from yt.yt_sync.core.spec_merger import StagesSpec
from yt.yt_sync.core.spec_merger.stages_merger import add_builtin_and_check_presets
from yt.yt_sync.core.spec_merger.stages_merger import apply_and_drop_in_stage_queues_of_consumers
from yt.yt_sync.core.spec_merger.stages_merger import apply_cluster_presets_to_clusters
from yt.yt_sync.core.spec_merger.stages_merger import apply_cluster_presets_to_stage
from yt.yt_sync.core.spec_merger.stages_merger import apply_stage_presets
from yt.yt_sync.core.spec_merger.stages_merger import fill_default_path_in_clusters_inplace
from yt.yt_sync.core.spec_merger.stages_merger import fill_default_paths_in_stage
from yt.yt_sync.core.spec_merger.stages_merger import get_main_cluster
from yt.yt_sync.core.spec_merger.stages_merger import make_stage_raw_specs
from yt.yt_sync.core.spec_merger.stages_merger import make_stage_specs
from yt.yt_sync.core.spec_merger.stages_spec import StageSpec

# Fake one.
PIPELINE_ENTITY_COMMON_SPEC = {
    "$merge_presets": ["builtin:storage_preset"],
    "clusters": {
        "_all_clusters": {
            "attributes": {
                "compression_codec": "lz4",
                "tablet_cell_bundle": "test_bundle",
                "primary_medium": "ssd_blobs",
            },
        },
    },
}
BUILTIN_PRESETS = {
    "builtin:storage_preset": {},
    "builtin:table_preset": {"$merge_presets": ["builtin:storage_preset"]},
    "builtin:consumer_preset": {
        "table": {
            "$merge_presets": ["builtin:table_preset"],
            "schema": CONSUMER_SCHEMA,
            "clusters": {"_all_clusters": {"attributes": CONSUMER_ATTRS}},
        },
    },
    "builtin:producer_preset": {
        "table": {
            "$merge_presets": ["builtin:table_preset"],
            "schema": PRODUCER_SCHEMA,
            "clusters": {"_all_clusters": {"attributes": PRODUCER_ATTRS}},
        },
    },
    "builtin:pipeline_sorted_table_preset": {"$merge_presets": ["builtin:table_preset"]},
    "builtin:pipeline_ordered_table_preset": {"$merge_presets": ["builtin:table_preset"]},
    "builtin:pipeline_file_preset": {"$merge_presets": ["builtin:storage_preset"]},
    "builtin:pipeline_preset": {
        "table_spec": {name: PIPELINE_ENTITY_COMMON_SPEC for name in PIPELINE_TABLES},
        "queue_spec": {name: PIPELINE_ENTITY_COMMON_SPEC for name in PIPELINE_QUEUES},
        "file_spec": {name: PIPELINE_ENTITY_COMMON_SPEC for name in PIPELINE_FILES},
    },
}


def test_add_builtin_and_check_presets():
    stages_spec = StagesSpec(
        stages={
            "default": {"presets": {"a": {}}},
        },
        builtin_presets=BUILTIN_PRESETS,
    )
    new_stages_spec = add_builtin_and_check_presets(stages_spec)
    assert "builtin:table_preset" in new_stages_spec.stages["default"]["presets"]

    # Declare preset not in default stage.
    bad_stages_spec = deepcopy(stages_spec)
    bad_stages_spec.stages["stable"] = {"presets": {"b": {}}}
    with pytest.raises(AssertionError) as ex:
        add_builtin_and_check_presets(bad_stages_spec)
    assert "allowed only in default stage" in str(ex.value)

    # Declare preset that looks like built-in preset.
    bad_stages_spec = deepcopy(stages_spec)
    bad_stages_spec.stages["default"]["presets"]["builtin:b"] = {}
    with pytest.raises(AssertionError) as ex:
        add_builtin_and_check_presets(bad_stages_spec)
    assert "builtin" in str(ex.value)


SIMPLE_SPEC_WITH_INNER_ENTITIES = StagesSpec(
    stages={
        "default": {
            "presets": {"sorted_table_preset": {"A": 1}},
            "tables": {
                "my_table": {
                    "$merge_presets": ["sorted_table_preset"],
                    "schema": [{"name": "NAME"}],
                },
            },
        },
        "stable": {
            "folder": "//stable",
            "presets": {"sorted_table_preset": {"A": 2}, "sorted_table_2_preset": {"B": 1}},
            "tables": {"my_table": {"$merge_presets": ["sorted_table_2_preset"]}},
        },
        "dev": {"folder": "//dev"},
    },
)

SIMPLE_SPEC_WITH_OUTER_ENTITIES = StagesSpec(
    stages={
        "default": {
            "presets": {"sorted_table_preset": {"A": 1}},
        },
        "stable": {
            "folder": "//stable",
            "presets": {"sorted_table_preset": {"A": 2}, "sorted_table_2_preset": {"B": 1}},
        },
        "dev": {"folder": "//dev"},
    },
    tables={
        "my_table": {
            "default": {
                "$merge_presets": ["sorted_table_preset"],
                "schema": [{"name": "NAME"}],
            },
            "stable": {
                "$merge_presets": ["sorted_table_2_preset"],
            },
        },
    },
)


def test_make_stage_raw_specs_simple_spec():
    result = make_stage_raw_specs(SIMPLE_SPEC_WITH_INNER_ENTITIES)
    assert result.keys() == {"stable", "dev"}
    assert asdict(result["stable"]) == asdict(
        StageSpec(
            folder="//stable",
            presets={"sorted_table_preset": {"A": 2}, "sorted_table_2_preset": {"B": 1}},
            tables={
                "my_table": {
                    "$merge_presets": ["sorted_table_preset", "sorted_table_2_preset"],
                    "schema": [{"name": "NAME"}],
                },
            },
        ),
    )
    assert result["dev"].folder == "//dev"
    assert result["dev"].presets == {"sorted_table_preset": {"A": 1}}
    assert result["dev"].tables == {
        "my_table": {"$merge_presets": ["sorted_table_preset"], "schema": [{"name": "NAME"}]},
    }

    assert result == make_stage_raw_specs(SIMPLE_SPEC_WITH_OUTER_ENTITIES)

    # Mix in-stage and out-stage entities specification.
    bad_spec = deepcopy(SIMPLE_SPEC_WITH_INNER_ENTITIES)
    bad_spec.tables["my_second_table"] = {}
    with pytest.raises(AssertionError) as ex:
        make_stage_raw_specs(bad_spec)
    assert "in stages" in str(ex.value) and "out of stages" in str(ex.value)


def test_apply_stage_presets():
    spec = StageSpec(
        folder="//tmp",
        presets={"presetA": {"A": 1}, "presetB": {"B": 1}},
        tables={"t1": {"$merge_presets": ["presetA"]}},
        producers={"p1": {"$merge_presets": ["presetA", "presetB"]}},
    )
    expected = StageSpec(
        folder="//tmp",
        tables={"t1": {"A": 1}},
        producers={"p1": {"A": 1, "B": 1}},
    )
    assert asdict(apply_stage_presets(spec)) == asdict(expected)


def test_apply_cluster_presets_to_clusters():
    assert apply_cluster_presets_to_clusters({}) == {}
    assert apply_cluster_presets_to_clusters({"test": {"a": 1}}) == {"test": {"a": 1}}
    assert apply_cluster_presets_to_clusters({"_all_clusters": {"a": 1}, "test": {}}) == {"test": {"a": 1}}
    assert apply_cluster_presets_to_clusters({"_all_data_clusters": {"a": 1}, "test": {}}) == {"test": {"a": 1}}
    assert apply_cluster_presets_to_clusters({"_all_replicated_clusters": {"a": 1}, "test": {}}) == {"test": {}}
    assert apply_cluster_presets_to_clusters(
        {
            "_all_replicated_clusters": {"a": 1},
            "_all_data_clusters": {"b": 1},
            "test": {"main": True},
            "replica": {},
        }
    ) == {"test": {"main": True, "a": 1}, "replica": {"b": 1}}
    assert apply_cluster_presets_to_clusters(
        {
            "_all_data_clusters": {"a": 1},
            "_all_mr_cluster": {"$merge_clusters": ["_all_data_clusters"], "b": 1},
            "test": {"main": True},
            "replica1": {},
            "replica2": {"$merge_clusters": ["_all_mr_cluster"]},
        },
        allowed_presets={"_all_mr_cluster"},
    ) == {"test": {"main": True}, "replica1": {"a": 1}, "replica2": {"a": 1, "b": 1}}
    assert apply_cluster_presets_to_clusters(
        {
            "_all_data_clusters": {"a": 1},
            "_all_mr_cluster": {"$merge_clusters": ["_all_clusters"], "b": 1},
            "test": {"main": True},
            "replica1": {},
            "replica2": {"$merge_clusters": ["_all_mr_cluster"]},
        },
        allowed_presets={"_all_mr_cluster"},
    ) == {"test": {"main": True}, "replica1": {"a": 1}, "replica2": {"b": 1}}


def test_apply_cluster_presets_to_stage():
    def get_spec(is_merged):
        def get_spec_with_clusters():
            if is_merged:
                return {"clusters": {"test": {"a": 1}}}
            return {"clusters": {"_all_clusters": {"a": 1}, "test": {}}}

        return StageSpec(
            folder="//tmp",
            tables={"TABLE": get_spec_with_clusters()},
            nodes={"NODE": get_spec_with_clusters()},
            consumers={"CONSUMER": {"table": get_spec_with_clusters()}},
            producers={"PRODUCER": {"table": get_spec_with_clusters()}},
            pipelines={
                "PIPELINE": {
                    "table_spec": {"INNER_TABLE": get_spec_with_clusters()},
                    "queue_spec": {"INNER_QUEUE": get_spec_with_clusters()},
                    "file_spec": {"INNER_FILE": get_spec_with_clusters()},
                }
            },
        )

    assert asdict(apply_cluster_presets_to_stage(get_spec(False))) == asdict(get_spec(True))


def test_fill_default_path_in_clusters_inplace():
    clusters = {"a": {"path": "//xxx"}, "b": {}}
    fill_default_path_in_clusters_inplace(clusters, "//yyy")
    expected = {"a": {"path": "//xxx"}, "b": {"path": "//yyy"}}
    assert clusters == expected


def test_fill_default_paths_in_stage():
    def get_spec(path):
        def get_spec_with_clusters(suffix):
            if path:
                return {"clusters": {"test": {"path": f"{path}/{suffix}"}}}
            return {"clusters": {"test": {}}}

        return StageSpec(
            folder="//tmp",
            tables={"TABLE": get_spec_with_clusters("TABLE")},
            nodes={"NODE": get_spec_with_clusters("NODE")},
            consumers={"CONSUMER": {"table": get_spec_with_clusters("CONSUMER")}},
            producers={"PRODUCER": {"table": get_spec_with_clusters("PRODUCER")}},
            pipelines={"PIPELINE": ({"path": f"{path}/PIPELINE"} if path else {})},
        )

    assert asdict(fill_default_paths_in_stage(get_spec(None))) == asdict(get_spec("//tmp"))


def test_get_main_cluster():
    assert get_main_cluster({"a": {}}) == "a"
    assert get_main_cluster({"a": {"main": True}}) == "a"
    assert get_main_cluster({"a": {"main": True}, "b": {}}) == "a"
    assert get_main_cluster({"a": {}, "b": {"main": True}}) == "b"
    with pytest.raises(AssertionError) as ex:
        get_main_cluster({"a": {}, "b": {}})
    assert "main" in str(ex.value)
    with pytest.raises(AssertionError) as ex:
        get_main_cluster({"a": {"main": True}, "b": {"main": True}})
    assert "main" in str(ex.value)


def test_apply_and_drop_in_stage_queues_of_consumers():
    # Marker for documentation: [BEGIN in_stage_queues_test]
    short = fill_default_paths_in_stage(
        StageSpec(
            folder="//tmp",
            tables={"QUEUE": {"schema": [], "clusters": {"primary": {}}}},
            consumers={"CONSUMER": {"table": {"clusters": {}}, "in_stage_queues": {"QUEUE": {"vital": True}}}},
        )
    )
    long = fill_default_paths_in_stage(
        StageSpec(
            folder="//tmp",
            tables={"QUEUE": {"schema": [], "clusters": {"primary": {}}}},
            consumers={
                "CONSUMER": {
                    "table": {"clusters": {}},
                    "queues": [{"cluster": "primary", "path": "//tmp/QUEUE", "vital": True}],
                }
            },
        )
    )
    assert asdict(apply_and_drop_in_stage_queues_of_consumers(short)) == asdict(long)
    # Marker for documentation: [END in_stage_queues_test]
    assert asdict(apply_and_drop_in_stage_queues_of_consumers(long)) == asdict(long)


def test_make_stage_specs():
    stages_spec = StagesSpec(
        stages={
            "default": {
                "presets": {
                    "builtin:storage_preset": {
                        "clusters": {"_all_clusters": {"attributes": {"primary_medium": "ssd_blobs"}}}
                    },
                    "builtin:table_preset": {
                        "clusters": {"_all_clusters": {"attributes": {"tablet_cell_bundle": "bundle"}}}
                    },
                    "big_sorted_table_preset": {"$merge_presets": ["builtin:table_preset"]},
                    "pipeline_prod_like_sorted_table_preset": {
                        "clusters": {
                            "_all_data_clusters": {
                                "attributes": {
                                    "tablet_balancer_config": {"min_tablet_count": 100, "desired_tablet_count": 200},
                                },
                            },
                        },
                    },
                    "pipeline_prod_like_preset": {
                        "table_spec": {
                            "input_messages": {
                                "clusters": {
                                    "_all_data_clusters": {
                                        "attributes": {"mount_config": {"min_data_versions": 0, "max_data_ttl": "3h"}},
                                    }
                                }
                            }
                        }
                    },
                },
            },
            "stable": {
                "folder": "//stable",
                "presets": {
                    # Fix markov primary medium. All entities got cluster markov.
                    "builtin:storage_preset": {"clusters": {"markov": {"attributes": {"primary_medium": "default"}}}},
                    # All tables has markov as main.
                    "builtin:table_preset": {"clusters": {"markov": {"main": True}}},
                    # Big tables has replica, not standalone.
                    "big_sorted_table_preset": {"clusters": {"seneca-sas": {}}},
                    # Override presets for pipeline.
                    "builtin:pipeline_sorted_table_preset": {
                        "$merge_presets": ["pipeline_prod_like_sorted_table_preset"]
                    },
                    "builtin:pipeline_preset": {"$merge_presets": ["pipeline_prod_like_preset"]},
                },
            },
            "dev": {
                "folder": "//dev",
                "presets": {
                    "builtin:storage_preset": {"clusters": {"zeno": {}}},
                },
            },
        },
        tables={
            "my_table": {
                "default": {
                    "$merge_presets": ["big_sorted_table_preset"],
                    "schema": [
                        {"name": "K", "type": "string", "sort_order": "ascending"},
                        {"name": "V", "type": "string"},
                    ],
                },
                "stable": {"clusters": {"_all_data_clusters": {"attributes": {"in_memory_mode": "uncompressed"}}}},
            },
            "my_queue": {
                "default": {"$merge_presets": ["builtin:table_preset"], "schema": [{"name": "V", "type": "string"}]},
            },
        },
        consumers={
            "my_consumer": {
                "default": {
                    "$merge_presets": ["builtin:consumer_preset"],
                    "in_stage_queues": {"my_queue": {"vital": True}},
                },
            },
        },
        producers={"my_producer": {"default": {"$merge_presets": ["builtin:producer_preset"]}}},
        pipelines={
            "my_pipeline": {
                "default": {"$merge_presets": ["builtin:pipeline_preset"]},
                "stable": {
                    "monitoring_project": "<project>",
                    "monitoring_cluster": "<cluster>",
                },
                "dev": {
                    "monitoring_project": "<project>",
                    "monitoring_cluster": "<cluster-dev>",
                },
            },
        },
        builtin_presets=BUILTIN_PRESETS,
    )

    stage_specs = make_stage_specs(stages_spec)
    stable_spec = stage_specs["stable"]

    stable_my_table_clusters = stable_spec.tables["my_table"]["clusters"]
    assert stable_my_table_clusters["markov"]["attributes"]["primary_medium"] == "default"
    assert stable_my_table_clusters["seneca-sas"]["attributes"]["primary_medium"] == "ssd_blobs"
    assert stable_my_table_clusters["markov"]["attributes"].get("in_memory_mode") is None
    assert stable_my_table_clusters["seneca-sas"]["attributes"]["in_memory_mode"] == "uncompressed"

    def check_stable_standalone_entity(clusters):
        assert clusters.get("seneca-sas") is None
        assert clusters["markov"]["attributes"]["primary_medium"] == "default"

    check_stable_standalone_entity(stable_spec.tables["my_queue"]["clusters"])
    check_stable_standalone_entity(stable_spec.consumers["my_consumer"]["table"]["clusters"])
    check_stable_standalone_entity(stable_spec.producers["my_producer"]["table"]["clusters"])

    stable_my_pipeline = stable_spec.pipelines["my_pipeline"]
    check_stable_standalone_entity(stable_my_pipeline["table_spec"]["input_messages"]["clusters"])
    check_stable_standalone_entity(stable_my_pipeline["queue_spec"]["controller_logs"]["clusters"])
    check_stable_standalone_entity(stable_my_pipeline["file_spec"]["important_versions"]["clusters"])

    dev_spec = stage_specs["dev"]

    dev_my_table_clusters = dev_spec.tables["my_table"]["clusters"]
    assert dev_my_table_clusters["zeno"]["attributes"]["primary_medium"] == "ssd_blobs"
    assert dev_my_table_clusters["zeno"]["attributes"].get("in_memory_mode") is None


def test_fix_priority_global():
    # Fix merging order to avoid changing it accidentally.
    # PLACE_<cluster_preset/cluster_itself><preset/table_itself><default_stage/stage_itself>
    PLACE_000 = {}
    PLACE_001 = {}
    PLACE_010 = {}
    PLACE_011 = {}
    PLACE_100 = {}  # Is not really meaningful, but anyway.
    PLACE_101 = {}
    PLACE_110 = {}
    PLACE_111 = {}

    stages_spec = StagesSpec(
        stages={
            "default": {
                "presets": {
                    "builtin:table_preset": {
                        "clusters": {"_all_clusters": {"attributes": PLACE_000}, "primary": {"attributes": PLACE_100}}
                    },
                },
            },
            "stable": {
                "folder": "//stable",
                "presets": {
                    "builtin:table_preset": {
                        "clusters": {"_all_clusters": {"attributes": PLACE_001}, "primary": {"attributes": PLACE_101}}
                    },
                },
            },
        },
        tables={
            "my_table": {
                "default": {
                    "$merge_presets": ["builtin:table_preset"],
                    "clusters": {"_all_clusters": {"attributes": PLACE_010}, "primary": {"attributes": PLACE_110}},
                },
                "stable": {
                    "clusters": {"_all_clusters": {"attributes": PLACE_011}, "primary": {"attributes": PLACE_111}}
                },
            },
        },
        builtin_presets=BUILTIN_PRESETS,
    )

    actual = {"v": 1}

    def fire_and_check(place):
        actual["v"] += 1
        place.update(actual)
        stage_specs = make_stage_specs(stages_spec)
        stable_spec = stage_specs["stable"]
        result_v = stable_spec.tables["my_table"]["clusters"]["primary"]["attributes"]["v"]
        assert actual["v"] == result_v, "Merging order is broken"

    fire_and_check(PLACE_000)
    fire_and_check(PLACE_001)
    fire_and_check(PLACE_010)
    fire_and_check(PLACE_011)
    fire_and_check(PLACE_100)
    fire_and_check(PLACE_101)
    fire_and_check(PLACE_110)
    fire_and_check(PLACE_111)

    # Self-check of test goodness.
    with pytest.raises(AssertionError) as ex:
        PLACE_110.clear()
        PLACE_111.clear()
        fire_and_check(PLACE_111)
        fire_and_check(PLACE_110)
    assert "order is broken" in str(ex.value)
