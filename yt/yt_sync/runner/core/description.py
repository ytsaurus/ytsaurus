from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import auto
from enum import StrEnum
from typing import Union

from yt.yt_sync.core.spec import Consumer
from yt.yt_sync.core.spec import Node
from yt.yt_sync.core.spec import Pipeline
from yt.yt_sync.core.spec import Producer
from yt.yt_sync.core.spec import Table
from yt.yt_sync.core.spec.details import ConsumerSpec
from yt.yt_sync.core.spec.details import NodeSpec
from yt.yt_sync.core.spec.details import PipelineSpec
from yt.yt_sync.core.spec.details import ProducerSpec
from yt.yt_sync.core.spec.details import TableSpec


class StageEntityType(StrEnum):
    TABLE = auto()
    CONSUMER = auto()
    PRODUCER = auto()
    NODE = auto()
    PIPELINE = auto()


@dataclass
class StageDescription:
    """Stage description for YtSync."""

    tables: dict[str, Table] = dataclass_field(default_factory=dict)
    """Tables by names."""

    consumers: dict[str, Consumer] = dataclass_field(default_factory=dict)
    """Consumers by names."""

    producers: dict[str, Producer] = dataclass_field(default_factory=dict)
    """Consumers by names."""

    nodes: dict[str, Node] = dataclass_field(default_factory=dict)
    """Nodes by names."""

    pipelines: dict[str, Pipeline] = dataclass_field(default_factory=dict)
    """Pipelines by names."""

    locks: list[str] | None = None
    """Custom lock paths on main cluster."""

    def add_table(self, name: str, table: Table):
        """Add table checking that it is not duplicate"""
        self.add_entity(StageEntityType.TABLE, name, table)

    def add_consumer(self, name: str, consumer: Consumer):
        """Add consumer checking that it is not duplicate"""
        self.add_entity(StageEntityType.CONSUMER, name, consumer)

    def add_producer(self, name: str, producer: Producer):
        """Add producer checking that it is not duplicate"""
        self.add_entity(StageEntityType.PRODUCER, name, producer)

    def add_node(self, name: str, node: Node):
        """Add node checking that it is not duplicate"""
        self.add_entity(StageEntityType.NODE, name, node)

    def add_pipeline(self, name: str, pipeline: Pipeline):
        """Add pipeline checking that it is not duplicate"""
        self.add_entity(StageEntityType.PIPELINE, name, pipeline)

    def add_entity(
        self,
        entity_type: StageEntityType,
        name: str,
        spec: Union[Table, Consumer, Producer, Node, Pipeline],
    ):
        """Add entity checking that it is not duplicate"""
        object_type, object_spec_type, object_map = self._get_entity_spec_type_and_map(entity_type)
        assert name not in object_map
        parsed_spec = (
            object_spec_type(spec=spec)
            if isinstance(spec, object_type)
            else object_spec_type.parse(spec, with_ensure=False)
        )
        parsed_spec.ensure()
        assert not isinstance(parsed_spec.spec, object_spec_type)
        object_map[name] = parsed_spec.spec

    def get_entities(self, entity_type: StageEntityType) -> dict[str, Union[Table, Consumer, Producer, Node, Pipeline]]:
        return self._get_entity_spec_type_and_map(entity_type)[2]

    def _get_entity_spec_type_and_map(self, entity_type: StageEntityType):
        return {
            StageEntityType.TABLE: (Table, TableSpec, self.tables),
            StageEntityType.CONSUMER: (Consumer, ConsumerSpec, self.consumers),
            StageEntityType.PRODUCER: (Producer, ProducerSpec, self.producers),
            StageEntityType.NODE: (Node, NodeSpec, self.nodes),
            StageEntityType.PIPELINE: (Pipeline, PipelineSpec, self.pipelines),
        }[entity_type]


@dataclass
class Description:
    """Description for all stages under YtSync."""

    stages: dict[str, StageDescription] = dataclass_field(default_factory=dict)
    """Stages by name."""
