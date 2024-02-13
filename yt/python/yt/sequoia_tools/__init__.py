from dataclasses import dataclass

from dacite import from_dict
from typing import List, Dict, Any

import yaml
import os

try:
    from yt.record_codegen_helpers import Field, Manifest
except ImportError:
    # Doing this because open-source build forced my hand.
    from record_codegen_helpers import Field, Manifest


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


resource_file_prefix = os.path.join("yt", "yt", "ytlib", "sequoia_client", "records")
records_text = []
try:
    from library.python import resource
    for src in resource.resfs_files(resource_file_prefix):
        records_text.append(resource.resfs_read(src))
except ImportError:
    # Package resource is missing in open-source build, so I am forced to use env vars here.
    source_root = os.environ['SOURCE_ROOT']
    records_dir_path = os.path.join(source_root, resource_file_prefix)
    for record_name in os.listdir(records_dir_path):
        record_path = os.path.join(records_dir_path, record_name)
        with open(record_path, "rb") as file:
            records_text.append(file.read())

for text in records_text:
    manifest_dict = yaml.safe_load(text)
    manifest = from_dict(Manifest, manifest_dict)
    for record in manifest.types:
        name = record.table_name
        group = record.table_group
        schema = _build_schema_from_fields(record.fields)
        setattr(DESCRIPTORS, name, TableDescriptor(name, group, schema))
