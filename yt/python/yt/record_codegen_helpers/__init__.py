from dataclasses import dataclass

from typing import Optional, List


@dataclass
class Field:
    cpp_name: str
    cpp_type: str
    column_name: str
    column_type: str
    sort_order: Optional[str]
    lock: Optional[str]
    aggregate: Optional[str]


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


@dataclass
class Manifest:
    namespace: str
    includes: Optional[List[str]]
    types: List[RecordType]
    h_verbatim: Optional[str]
    cpp_verbatim: Optional[str]
    h_path: Optional[str]
