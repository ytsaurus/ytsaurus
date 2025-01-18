from contextlib import contextmanager
from copy import deepcopy

from yt.yt_sync.runner.core import Description
from yt.yt_sync.runner.core import StageDescription

from yt.yt_sync.core.spec_merger import make_stage_specs
from yt.yt_sync.core.spec_merger import ENTITY_KEYS
from yt.yt_sync.core.spec_merger import StageSpec
from yt.yt_sync.core.spec_merger import StagesSpec

from yt.yt_sync.runner.builtin_presets import BUILTIN_PRESETS


@contextmanager
def add_exception_attributes(**attributes):
    try:
        yield
    except Exception as ex:
        ex.add_note(f"In a scope with attributes: {attributes}")
        raise


def ensure_builtin_presets(stages_spec: StagesSpec) -> StagesSpec:
    """If `builtin_presets` is None, fill it with default builtin presets."""
    stages_spec = deepcopy(stages_spec)
    if stages_spec.builtin_presets is None:
        stages_spec.builtin_presets = BUILTIN_PRESETS
    return stages_spec


def make_runner_description_from_merged_stage_specs(stage_specs: dict[str, StageSpec]) -> Description:
    methods = {
        "tables": StageDescription.add_table,
        "consumers": StageDescription.add_consumer,
        "producers": StageDescription.add_producer,
        "nodes": StageDescription.add_node,
        "pipelines": StageDescription.add_pipeline,
    }
    description: Description = Description()
    for stage_name, stage_spec in stage_specs.items():
        with add_exception_attributes(stage_name=stage_name):
            stage_description: StageDescription = StageDescription()
            for entity_key in ENTITY_KEYS:
                with add_exception_attributes(entity_key=entity_key):
                    for name, entity in getattr(stage_spec, entity_key).items():
                        with add_exception_attributes(entity_name=name):
                            methods[entity_key](stage_description, name, entity)
            description.stages[stage_name] = stage_description
    return description


def make_runner_description(stages_spec: StagesSpec) -> Description:
    stage_specs = make_stage_specs(ensure_builtin_presets(stages_spec))
    return make_runner_description_from_merged_stage_specs(stage_specs)
