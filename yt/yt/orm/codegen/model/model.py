from .index import OrmIndex
from .message import OrmEnum, OrmMessage
from .object import OrmObject
from .package import OrmDatabaseOptions, OrmModule
from .reference import OrmReferencesTable

from dataclasses import dataclass

from yt.yt.orm.library.snapshot.codegen.model import Snapshot


@dataclass
class OrmModel:
    source_modules: dict[str, OrmModule]
    source_package_paths: set[str]
    imports: list[str]
    public_imports: list[str]
    unexported_module_paths: set[str]
    objects: list[OrmObject]
    public_enums: list[OrmEnum]
    references_tables: list[OrmReferencesTable]
    indexes: list[OrmIndex]
    database_options: OrmDatabaseOptions
    objects_by_source: dict[str, list[OrmObject]]
    sourced_enums: dict[str, list[OrmEnum]]
    sourced_messages: dict[str, list[OrmMessage]]
    etc_messages_by_output: dict[str, list[OrmMessage]]
    acl_actions_enum: OrmEnum
    acl_permissions_enum: OrmEnum
    acl_entry_message: OrmMessage
    tags_enum: OrmEnum
    snapshots: dict[str, Snapshot]
