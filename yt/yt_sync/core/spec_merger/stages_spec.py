from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Optional

ENTITY_KEYS = ("tables", "consumers", "producers", "nodes", "pipelines")


@dataclass
class StageSpec:
    """Description for one stage."""

    folder: str
    """Root folder of stage."""

    presets: dict[str, dict] = dataclass_field(default_factory=dict)
    """Named presets"""

    allowed_cluster_presets: list[str] = dataclass_field(default_factory=list)
    """Extra special cluster names that will be allowed in 'clusters' in table and node specs."""

    tables: dict[str, dict] = dataclass_field(default_factory=dict)
    """
    Tables. Path of resulting table is computing as f'{folder}/{table_key}'.
    Format: {table_key: table_description, ...}
    """

    consumers: dict[str, dict] = dataclass_field(default_factory=dict)
    """ Consumers. Path of resulting table is computing as f'{folder}/{consumer_key}'."""

    producers: dict[str, dict] = dataclass_field(default_factory=dict)
    """ Producers. Path of resulting table is computing as f'{folder}/{producer_key}'."""

    nodes: dict[str, dict] = dataclass_field(default_factory=dict)
    """ Nodes. Path of resulting table is computing as f'{folder}/{node_key}'."""

    pipelines: dict[str, dict] = dataclass_field(default_factory=dict)
    """ YT Flow pipelines. Path of resulting table is computing as f'{folder}/{pipeline_key}'."""

    def has_entities(self):
        return sum(len(getattr(self, entity_key)) for entity_key in ENTITY_KEYS) > 0


@dataclass
class StagesSpec:
    """Description of all stages."""

    stages: dict[str, dict] = dataclass_field(default_factory=dict)
    """
    All stages. Values must be dicts in StageSpec format.
    Can have `default` key, default section will be merged under all other sections.
    `$merge_presets` lists will be concatenated.
    """

    tables: dict[str, dict[str, dict]] = dataclass_field(default_factory=dict)
    """
    Alternative place to configure tables.
    Format: {table_key: {stage_key: table_description, ...}, ...}
    """

    consumers: dict[str, dict[str, dict]] = dataclass_field(default_factory=dict)
    producers: dict[str, dict[str, dict]] = dataclass_field(default_factory=dict)
    nodes: dict[str, dict[str, dict]] = dataclass_field(default_factory=dict)
    pipelines: dict[str, dict[str, dict]] = dataclass_field(default_factory=dict)

    builtin_presets: Optional[dict[str, dict]] = None
    """Dict that describes 'builtin:*' presets."""

    def has_out_stage_entities(self):
        return sum(len(getattr(self, entity_key)) for entity_key in ENTITY_KEYS) > 0
