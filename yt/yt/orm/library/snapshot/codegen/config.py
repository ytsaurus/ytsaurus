from .name import SnapshotEntityName

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import dacite
import yaml


@dataclass
class SnapshotManyToOneReferenceConfig:
    name: SnapshotEntityName
    local_field_paths: list[str]
    remote_object_name: str


@dataclass
class SnapshotOneToManyReferenceConfig:
    name: SnapshotEntityName
    remote_object_name: str
    remote_many_to_one_reference_name: str


@dataclass
class SnapshotFieldConfig:
    path: str
    name: Optional[SnapshotEntityName] = None
    cpp_type: Optional[str] = None
    is_key: bool = False
    treat_falsy_value_as_invalid_reference: bool = False


@dataclass
class SnapshotObjectConfig:
    name: SnapshotEntityName
    type_value: Optional[int] = None
    fields: list[SnapshotFieldConfig] = field(default_factory=list)
    many_to_one_references: list[SnapshotManyToOneReferenceConfig] = field(default_factory=list)
    one_to_many_references: list[SnapshotOneToManyReferenceConfig] = field(default_factory=list)


@dataclass
class SnapshotConfig:
    name: SnapshotEntityName
    cpp_path: Optional[str] = None
    cpp_namespace: Optional[str] = None
    cpp_includes: list[str] = field(default_factory=list)
    cpp_dependencies: list[str] = field(default_factory=list)
    inherits: list[str] = field(default_factory=list)
    objects: list[SnapshotObjectConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict):
        return dacite.from_dict(cls, d, dacite.Config(type_hooks={SnapshotEntityName: SnapshotEntityName.from_value}))

    @classmethod
    def from_file(cls, path: Path):
        if path.suffix != ".yaml":
            raise ValueError(
                f"Can't load snapshot config from unsupported file format: \"{path}\". "
                f"The only supported file format for now is YAML (.yaml)."
            )

        with path.open() as f:
            return cls.from_dict(yaml.safe_load(f))
