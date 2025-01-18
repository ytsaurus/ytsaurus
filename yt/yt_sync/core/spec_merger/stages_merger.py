from contextlib import contextmanager
from copy import deepcopy
from dacite import from_dict

from library.python.confmerge import merge_copy

from .preset_merger import PresetsMerger
from .preset_merger import StageMerger

from .stages_spec import ENTITY_KEYS
from .stages_spec import StageSpec
from .stages_spec import StagesSpec

MERGE_SPECIAL_PREFIX = "$"
CLUSTER_MERGE_KEY = "$merge_clusters"
# Marker for documentation: [BEGIN cluster_presets]
DEFAULT_CLUSTERS = {
    "_all_clusters": {},
    # Implicit default for standalone clusters and replicas.
    "_all_data_clusters": {CLUSTER_MERGE_KEY: ["_all_clusters"]},
    # Implicit default for replicated clusters (masters).
    "_all_replicated_clusters": {CLUSTER_MERGE_KEY: ["_all_clusters"]},
}
# Marker for documentation: [END cluster_presets]
PRESET_MERGE_KEY = "$merge_presets"
STAGE_MERGER = StageMerger({PRESET_MERGE_KEY, CLUSTER_MERGE_KEY}, special_prefix=MERGE_SPECIAL_PREFIX)


@contextmanager
def add_exception_attributes(**attributes):
    try:
        yield
    except Exception as ex:
        ex.add_note(f"In a scope with attributes: {attributes}")
        raise


def add_builtin_and_check_presets(stages_spec: StagesSpec) -> StagesSpec:
    """
    Add builtin presets under presets of default stage.
    Checks that real stages do not add new presets.
    Drop builtin presets from `stages_spec`.
    """
    stages_spec = deepcopy(stages_spec)
    builtin_presets = stages_spec.builtin_presets
    assert builtin_presets is not None, "`builtin_presets` field is mandatory in `stages_spec`"
    stages_spec.builtin_presets = None

    default_presets = STAGE_MERGER.merge(builtin_presets, stages_spec.stages.get("default", {}).get("presets", {}))
    stages_spec.stages.setdefault("default", {})["presets"] = STAGE_MERGER.merge(builtin_presets, default_presets)
    allowed_presets = set(default_presets.keys())
    for stage_name, stage_spec in stages_spec.stages.items():
        with add_exception_attributes(stage_name=stage_name):
            actual_presets = set(stage_spec.get("presets", {}).keys())
            assert actual_presets <= allowed_presets, (
                "For your safety declaring new presets is allowed only in default stage, "
                f"unknown presets are {actual_presets - allowed_presets}"
            )
            for preset in actual_presets:
                with add_exception_attributes(preset=preset):
                    if preset.startswith("builtin:"):
                        assert preset in builtin_presets.keys(), "User preset can not start with 'builtin:' prefix"
    return stages_spec


def make_stage_raw_specs(stages_spec: StagesSpec) -> dict[str, StageSpec]:
    """Make stages specs fully independent."""
    stage_names = set(stages_spec.stages.keys()) - set(("default",))
    stages = {name: STAGE_MERGER.get_section(stages_spec.stages, name) for name in stage_names}
    result = {}
    if stages_spec.has_out_stage_entities():
        for entity_key in ENTITY_KEYS:
            with add_exception_attributes(entity_key=entity_key):
                for name, entity in getattr(stages_spec, entity_key).items():
                    with add_exception_attributes(entity_name=name):
                        assert (
                            PRESET_MERGE_KEY not in entity
                        ), f"{PRESET_MERGE_KEY} is not allowed at the top level of out-stage {entity_key} description"
    for stage_name in stage_names:
        with add_exception_attributes(stage_name=stage_name):
            stage_spec = from_dict(data_class=StageSpec, data=stages[stage_name])
            assert (
                not stage_spec.has_entities() or not stages_spec.has_out_stage_entities()
            ), "Tables/consumers/etc can be provided either in stages or out of stages"
            if stages_spec.has_out_stage_entities():
                for entity_key in ENTITY_KEYS:
                    with add_exception_attributes(entity_key=entity_key):
                        entities = getattr(stages_spec, entity_key)
                        stage_entities = {
                            name: STAGE_MERGER.get_section(entity, stage_name) for name, entity in entities.items()
                        }
                        setattr(stage_spec, entity_key, stage_entities)
            result[stage_name] = stage_spec
    return result


def apply_stage_presets(stage_spec: StageSpec) -> StageSpec:
    """Apply and drop stage presets."""
    entities = {
        entity_key: PresetsMerger(PRESET_MERGE_KEY, stage_spec.presets).apply_presets(getattr(stage_spec, entity_key))
        for entity_key in ENTITY_KEYS
    }
    return StageSpec(folder=stage_spec.folder, **entities)


def apply_cluster_presets_to_clusters(clusters: dict, allowed_presets: set[str] = frozenset()) -> dict:
    """Apply and drop cluster presets."""
    allowed_presets = set(DEFAULT_CLUSTERS.keys()) | allowed_presets
    clusters = merge_copy(DEFAULT_CLUSTERS, clusters)
    presets = {k: v for k, v in clusters.items() if k.startswith("_")}
    assert set(presets.keys()) <= allowed_presets, (
        f"Cluster presets {set(presets.keys()) - allowed_presets} are not allowed. "
        f"Allowed cluster presets: {allowed_presets}. "
        "You can add them to `allowed_cluster_presets`"
    )
    real_clusters = {k: v for k, v in clusters.items() if not k.startswith("_")}

    for cluster_name, cluster in real_clusters.items():
        if CLUSTER_MERGE_KEY not in cluster:
            is_data_cluster = len(real_clusters) == 1 or not cluster.get("main", False)
            cluster_preset = "_all_data_clusters" if is_data_cluster else "_all_replicated_clusters"
            cluster[CLUSTER_MERGE_KEY] = [cluster_preset]

    return PresetsMerger(CLUSTER_MERGE_KEY, presets).apply_presets(real_clusters)


def apply_cluster_presets_to_stage(stage_spec: StageSpec) -> StageSpec:
    assert len(stage_spec.presets) == 0, "Cluster presets must be applied after stage presets"
    allowed_presets = set(stage_spec.allowed_cluster_presets)

    def apply_presets(entity: dict):
        entity["clusters"] = apply_cluster_presets_to_clusters(entity["clusters"], allowed_presets)

    stage_spec = deepcopy(stage_spec)
    for entity_key in ["tables", "nodes"]:
        with add_exception_attributes(entity_key=entity_key):
            for name, entity in getattr(stage_spec, entity_key).items():
                with add_exception_attributes(entity_name=name):
                    apply_presets(entity)
    for entity_key in ["consumers", "producers"]:
        with add_exception_attributes(entity_key=entity_key):
            for name, entity in getattr(stage_spec, entity_key).items():
                with add_exception_attributes(entity_name=name):
                    apply_presets(entity["table"])
    for name, entity in stage_spec.pipelines.items():
        with add_exception_attributes(entity_key="pipelines", entity_name=name):
            for inner_entity_key in ("table_spec", "queue_spec", "file_spec"):
                with add_exception_attributes(inner_entity_key=inner_entity_key):
                    for inner_name, inner_entity in entity[inner_entity_key].items():
                        with add_exception_attributes(inner_entity_name=inner_name):
                            apply_presets(inner_entity)
    return stage_spec


def fill_default_path_in_clusters_inplace(clusters: dict, default_path: str):
    for cluster_name, cluster_spec in clusters.items():
        with add_exception_attributes(cluster_name=cluster_name):
            assert not cluster_name.startswith("_"), "Default path must be filled after applying cluster presets"
            cluster_spec.setdefault("path", default_path)


def fill_default_paths_in_stage(stage_spec: StageSpec) -> StageSpec:
    stage_spec = deepcopy(stage_spec)
    for entity_key in ["tables", "nodes"]:
        with add_exception_attributes(entity_key=entity_key):
            for name, entity in getattr(stage_spec, entity_key).items():
                with add_exception_attributes(entity_name=name):
                    fill_default_path_in_clusters_inplace(entity["clusters"], f"{stage_spec.folder}/{name}")
    for entity_key in ["consumers", "producers"]:
        with add_exception_attributes(entity_key=entity_key):
            for name, entity in getattr(stage_spec, entity_key).items():
                with add_exception_attributes(entity_name=name):
                    fill_default_path_in_clusters_inplace(entity["table"]["clusters"], f"{stage_spec.folder}/{name}")
    for name, entity in stage_spec.pipelines.items():
        with add_exception_attributes(entity_key="pipelines", entity_name=name):
            entity.setdefault("path", f"{stage_spec.folder}/{name}")
    return stage_spec


def get_main_cluster(clusters: dict) -> str:
    """Return replicated or standalone cluster. Raise exception otherwise."""
    assert len(clusters) >= 1, "`clusters` can not be empty"
    main_clusters = [name for name, spec in clusters.items() if spec.get("main", False)]
    assert len(main_clusters) <= 1, "Only one main cluster allowed"
    if len(main_clusters) == 1:
        return main_clusters[0]
    assert len(clusters) == 1, "There more than 1 cluster, but none of them is marked as main"
    return list(clusters.keys())[0]


def apply_and_drop_in_stage_queues_of_consumers(stage_spec: StageSpec) -> StageSpec:
    stage_spec = deepcopy(stage_spec)
    for consumer_name, consumer_spec in stage_spec.consumers.items():
        with add_exception_attributes(entity_key="consumers", entity_name=consumer_name):
            for queue_name, short_registration_spec in consumer_spec.get("in_stage_queues", {}).items():
                with add_exception_attributes(entity_key="queues", entity_name=queue_name):
                    assert queue_name in stage_spec.tables, "Unknown queue in consumer in stage registrations"
                    queue_clusters = stage_spec.tables[queue_name]["clusters"]
                    main_queue_cluster = get_main_cluster(queue_clusters)
                    registration = {
                        "cluster": main_queue_cluster,
                        "path": queue_clusters[main_queue_cluster]["path"],
                        **short_registration_spec,
                    }
                    consumer_spec.setdefault("queues", []).append(registration)
            consumer_spec.pop("in_stage_queues", None)
    return stage_spec


def make_stage_specs(stages_spec: StagesSpec) -> dict[str, StageSpec]:
    """Apply all merges."""
    stages_spec = add_builtin_and_check_presets(stages_spec)
    stage_raw_specs = make_stage_raw_specs(stages_spec)
    stage_specs = {}
    for stage_name, stage_raw_spec in stage_raw_specs.items():
        with add_exception_attributes(stage_name=stage_name):
            spec = apply_stage_presets(stage_raw_spec)
            spec = apply_cluster_presets_to_stage(spec)
            spec = fill_default_paths_in_stage(spec)
            spec = apply_and_drop_in_stage_queues_of_consumers(spec)
            stage_specs[stage_name] = spec
    return stage_specs
