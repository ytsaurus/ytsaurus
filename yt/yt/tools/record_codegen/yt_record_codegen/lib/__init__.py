from dataclasses import dataclass

from typing import Any, Optional, List

import json


@dataclass
class Field:
    cpp_name: str
    cpp_type: str
    column_name: str
    column_type: Optional[str]
    column_type_v3: Optional[Any]
    required: Optional[bool]
    sort_order: Optional[str]
    lock: Optional[str]
    aggregate: Optional[str]
    expression: Optional[str]

    def get_type_v3_string(self):
        if self.column_type_v3 is None:
            raise ValueError('can\'t get type_v3 string: "column_type_v3" field is not set')
        column_type_v3_json_string = json.dumps(self.column_type_v3)
        # Add quotes (jinja2 does not have something ready)
        return json.dumps(column_type_v3_json_string)

    def __post_init__(self):
        if self.column_type is None and self.column_type_v3 is None:
            raise ValueError('one of "column_type" and "column_type_v3" fields must be specified')
        if self.column_type is not None and self.column_type_v3 is not None:
            raise ValueError('fields "column_type" and "column_type_v3" are mutually exclusive')
        if self.required is not None and self.column_type_v3 is not None:
            raise ValueError('field "required" is incompatible with field "column_type_v3"')


@dataclass
class RecordType:
    type_name: str
    fields: List[Field]
    table_name: Optional[str]
    table_group: Optional[str]
    verbatim: Optional[str]
    record_verbatim: Optional[str]
    key_verbatim: Optional[str]
    descriptor_verbatim: Optional[str]
    sorted: bool = True


@dataclass
class Manifest:
    namespace: str
    includes: Optional[List[str]]
    types: List[RecordType]
    h_verbatim: Optional[str]
    cpp_verbatim: Optional[str]
    h_path: Optional[str]
