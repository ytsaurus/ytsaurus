from copy import deepcopy
from dataclasses import dataclass
from enum import auto
from enum import StrEnum
import os

import yt.wrapper as yt
from yt.yt_sync.core.spec.details import ConsumerSpec
from yt.yt_sync.core.spec.details import NodeSpec
from yt.yt_sync.core.spec.details import PipelineSpec
from yt.yt_sync.core.spec.details import ProducerSpec
from yt.yt_sync.core.spec.details import TableSpec

from .description import Description
from .description import StageDescription
from .description import StageEntityType


class SpecialNames(StrEnum):
    ALL = auto()
    NONE = auto()

    @classmethod
    def values(cls) -> list[str]:
        return [v.value for v in cls]


@dataclass
class DescriptionDetails:
    description: Description

    def get_all_stages(self) -> list[str]:
        return sorted(self.description.stages.keys())

    def get_all(self, entity_type: StageEntityType) -> list[str]:
        result = set()
        for stage in self.description.stages.values():
            result.update(stage.get_entities(entity_type).keys())
        return sorted(result)

    def _get_stage(self, stage_name: str) -> StageDescription:
        if stage_name not in self.description.stages:
            raise RuntimeError(f"Unknown stage {stage_name}")
        return self.description.stages[stage_name]

    def _validate_special_names(self, filter_by: list[str]):
        if SpecialNames.ALL in filter_by and len(filter_by) != 1:
            raise RuntimeError(f"'{SpecialNames.ALL}' should be single value in filter")
        if SpecialNames.NONE in filter_by and len(filter_by) != 1:
            raise RuntimeError(f"'{SpecialNames.NONE}' should be single value in filter")

    def validate_filter(
        self,
        entity_type: StageEntityType,
        stage_name: str,
        filter_by: list[str],
    ):
        self._validate_special_names(filter_by)
        stage = self._get_stage(stage_name)
        for item in filter_by:
            if item in SpecialNames.values():
                continue
            keys = stage.get_entities(entity_type).keys()
            if item not in keys:
                raise RuntimeError(f"Unknown {entity_type.value} {item} for stage {stage_name}")

    def get_prepared_stage_descriptions(
        self,
        stage_name: str,
        entities_names: dict[StageEntityType, set[str]],
    ) -> list[StageDescription]:
        def _is_ok(name: str, filter_by: set[str]) -> bool:
            if SpecialNames.ALL in filter_by:
                return True
            if SpecialNames.NONE in filter_by:
                return False
            return name in filter_by

        def _get_path(spec: TableSpec | NodeSpec) -> str:
            item_path = spec.main_cluster_spec.path
            return yt.ypath_dirname(item_path)

        stage = self._get_stage(stage_name)

        stages_by_cluster: dict[str, StageDescription] = dict()
        paths_by_cluster: dict[str, set[str]] = dict()

        for entity_type, spec_type in [
            (StageEntityType.TABLE, TableSpec),
            (StageEntityType.NODE, NodeSpec),
        ]:
            for name, spec in stage.get_entities(entity_type).items():
                if not _is_ok(name, entities_names.get(entity_type, set())):
                    continue
                parsed_spec = spec_type(spec)
                parsed_spec.ensure()
                stage_by_cluster = stages_by_cluster.setdefault(parsed_spec.main_cluster, StageDescription())
                stage_by_cluster.add_entity(entity_type, name, spec)
                paths_by_cluster.setdefault(parsed_spec.main_cluster, set()).add(_get_path(parsed_spec))

        for entity_type, spec_type in [
            (StageEntityType.CONSUMER, ConsumerSpec),
            (StageEntityType.PRODUCER, ProducerSpec),
        ]:
            for name, spec in stage.get_entities(entity_type).items():
                if not _is_ok(name, entities_names.get(entity_type, set())):
                    continue
                parsed_spec = spec_type(spec)
                parsed_spec.ensure()
                stage_by_cluster = stages_by_cluster.setdefault(parsed_spec.table.main_cluster, StageDescription())
                stage_by_cluster.add_entity(entity_type, name, spec)
                paths_by_cluster.setdefault(parsed_spec.table.main_cluster, set()).add(_get_path(parsed_spec.table))

        for name, spec in stage.pipelines.items():
            if not _is_ok(name, entities_names.get(StageEntityType.PIPELINE, set())):
                continue
            parsed_spec = PipelineSpec(spec)
            parsed_spec.ensure()
            stage_by_cluster = stages_by_cluster.setdefault(parsed_spec.main_cluster(), StageDescription())
            stage_by_cluster.add_pipeline(name, spec)
            paths_by_cluster.setdefault(parsed_spec.main_cluster(), set()).add(parsed_spec.path)

        result = list()
        for cluster, cluster_stage in stages_by_cluster.items():
            if stage.locks:
                cluster_stage.locks = deepcopy(stage.locks)
            else:

                def _fix_path(path: str) -> str:
                    # Ugly hack to restore YT absolute path after os.path.commonpath
                    if path.startswith("//"):
                        return path
                    elif path.startswith("/"):
                        return "/" + path
                    else:
                        return path

                cluster_stage.locks = [_fix_path(os.path.commonpath(paths_by_cluster[cluster]))]
            result.append(cluster_stage)
        return result

    def get_main_cluster(self, stage: StageDescription) -> str:
        if stage.tables:
            table = next(iter(stage.tables.values()))
            return TableSpec(table).main_cluster
        if stage.consumers:
            consumer = next(iter(stage.consumers.values()))
            return TableSpec(consumer.table).main_cluster
        if stage.producers:
            producer = next(iter(stage.producers.values()))
            return TableSpec(producer.table).main_cluster
        if stage.nodes:
            node = next(iter(stage.nodes.values()))
            return NodeSpec(node).main_cluster
        if stage.pipelines:
            pipeline = next(iter(stage.pipelines.values()))
            return PipelineSpec(pipeline).main_cluster()
        raise RuntimeError("Can't fetch main cluster for empty stage")
