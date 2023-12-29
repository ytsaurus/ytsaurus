from dataclasses import dataclass

from library.python import resource
from dacite import from_dict
from typing import List, Dict, Any

import yaml

from yt.yt.tools.record_codegen.helpers import Field, Manifest


################################################################################


@dataclass(frozen=True)
class TableDescriptor:
    name: str
    group: str
    schema: List[Dict[str, Any]]

    def get_default_path(self) -> str:
        return "//sys/sequoia/" + self.name


@dataclass
class TableDescriptors:
    def as_dict(self) -> Dict[str, TableDescriptor]:
        result = dict()
        for attr in dir(self):
            if not attr.startswith('__') and not callable(getattr(self, attr)):
                result[attr] = getattr(self, attr)
        return result

    def get_group(self, group_name) -> List[TableDescriptor]:
        result = list()
        descriptors = self.as_dict().values()
        for descriptor in descriptors:
            if descriptor.group == group_name:
                result.append(descriptor)
        return result


################################################################################


DESCRIPTORS = TableDescriptors()


def _build_column_from_field(field: Field) -> Dict[str, Any]:
    column = dict()
    column["name"] = field.column_name.lower()
    column["type"] = field.column_type.lower()
    if field.sort_order is not None:
        column["sort_order"] = field.sort_order.lower()
    if field.aggregate is not None:
        column["aggregate"] = field.aggregate.lower()
    return column


def _build_schema_from_fields(fields: List[Field]) -> List[Dict[str, Any]]:
    schema = []
    for field in fields:
        schema.append(_build_column_from_field(field))
    return schema


for src in resource.resfs_files("yt/yt/ytlib/sequoia_client/records"):
    text = resource.resfs_read(src)
    manifest_dict = yaml.safe_load(text)
    manifest = from_dict(Manifest, manifest_dict)
    for record in manifest.types:
        name = record.table_name
        group = record.table_group
        schema = _build_schema_from_fields(record.fields)
        setattr(DESCRIPTORS, name, TableDescriptor(name, group, schema))
