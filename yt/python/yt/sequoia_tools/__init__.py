from dacite import from_dict
import dataclasses
import os
from typing import Any,  Dict, List, Optional
import yaml

from yt_record_codegen.lib import Field, Manifest, RecordType
import yt_record_render.lib


################################################################################


@dataclasses.dataclass(frozen=True)
class TableDescriptor:
    name: str
    group: str
    schema: List[Dict[str, Any]]

    def get_default_path(self) -> str:
        return "//sys/sequoia/" + self.name


@dataclasses.dataclass
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


def _build_column_from_field(field: Field) -> Dict[str, Any]:
    column = dict()
    column["name"] = field.column_name.lower()
    column["type"] = field.column_type.lower()
    if field.sort_order is not None:
        column["sort_order"] = field.sort_order.lower()
    if field.aggregate is not None:
        column["aggregate"] = field.aggregate
    if field.expression is not None:
        column["expression"] = field.expression
    return column


def _build_schema_from_fields(fields: List[Field]) -> List[Dict[str, Any]]:
    schema = []
    for field in fields:
        # To remove $tablet_index and other system columns from schema, as they are magically created on their own.
        if field.column_name.lower()[0] == "$":
            continue
        schema.append(_build_column_from_field(field))
    return schema


def _build_descriptor_from_record(record: RecordType) -> TableDescriptor:
    name = record.table_name
    if name is None:
        raise RuntimeError("'table_name' field is required for Sequoia tables")
    group = record.table_group
    schema = _build_schema_from_fields(record.fields)
    return TableDescriptor(name, group, schema)


def get_table_descriptors(version: Optional[int]) -> TableDescriptors:
    result = TableDescriptors()
    for text in records_text:
        rendered_text = yt_record_render.lib.render(text.decode(), version)
        manifest_dict = yaml.safe_load(rendered_text)
        manifest = from_dict(Manifest, manifest_dict)
        for record in manifest.types:
            descriptor = _build_descriptor_from_record(record)
            name = descriptor.name
            if name in dataclasses.fields(result):
                raise ValueError(f"Multiple records with name '{name}' exist")
            setattr(result, name, descriptor)
    return result


# Use the latest version for test environment setup.
DESCRIPTORS = get_table_descriptors(None)
