from yt_proto.yt.orm.client.proto import object_pb2

from . import compat
from .common import OrmColumn, PRIMARY_DATA_GROUP, ETC_NAME_SEPARATOR, object_key_constructor_params
from .enum import OrmEnum, OrmEnumValue, enum_value_name_to_value
from .field import FIELD_PATH_SEP, OrmField
from .filters import foreign_key_prefix, table_name, collective_name, snake_to_camel_case
from .index import OrmIndex
from .message import OrmMessage
from .reference import OrmReference
from .tags import resolve_tags

from copy import copy
from dataclasses import dataclass, field as dataclass_field
from functools import cached_property
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .package import OrmModule, OrmPackage

META_FIELD = "meta"
SPEC_FIELD = "spec"
STATUS_FIELD = "status"
CONTROL_FIELD = "control"
TOP_LEVEL_FIELDS = (META_FIELD, SPEC_FIELD, STATUS_FIELD, CONTROL_FIELD)

DEFAULTS_BY_KEY_FIELD_TYPE: dict[str, str] = {
    "i64": "0",
    "ui64": "0",
    "double": "0.0",
    "bool": "false",
    "TString": "",
}


@dataclass
class OrmAttribute:
    fields: list[OrmField]
    # Last explicit (i.e. represented by C++ attribute) field on the path from the object root to
    # the |field|.
    column_field_index: int
    explicit_suffix_parts: list[str] = dataclass_field(default_factory=list)
    etc_name_parts: list[str] | None = None

    @property
    def field(self) -> Optional[OrmField]:
        if self.explicit_suffix_parts:
            return None
        return self.fields[-1]

    @property
    def column_field(self) -> OrmField:
        return self.fields[self.column_field_index]

    @property
    def prefix_fields(self) -> list[OrmField]:
        return self.fields[: self.column_field_index]

    @property
    def suffix_fields(self) -> list[OrmField]:
        return self.fields[self.column_field_index + 1 :]

    @property
    def suffix_path_parts(self) -> list[str]:
        return [field.snake_case_name for field in self.suffix_fields] + self.explicit_suffix_parts

    @property
    def suffix(self) -> str:
        return "/".join([""] + self.suffix_path_parts)

    @property
    def prefix(self) -> str:
        if not self.fields:
            return "/"
        return "/".join([""] + [field.snake_case_name for field in self.fields[:-1]])

    @property
    def full_path(self) -> str:
        return "/".join([""] + [field.snake_case_name for field in self.fields] + self.explicit_suffix_parts)

    @property
    def etc_name(self) -> str:
        return "_".join(self.etc_name_parts or [])

    @property
    def full_path_with_etc(self) -> str:
        if self.etc_name_parts:
            return self.full_path + ETC_NAME_SEPARATOR + self.etc_name
        else:
            return self.full_path

    def __hash__(self):
        return hash(self.full_path_with_etc)


@dataclass
class OrmWatchLog:
    snake_case_name: str
    filter: str
    selectors: list[str] = dataclass_field(default_factory=list)
    required_tags_strs: list[str] = dataclass_field(default_factory=list)
    excluded_tags_strs: list[str] = dataclass_field(default_factory=list)
    tablet_count: int = 6
    min_data_ttl: int = 600000
    max_data_ttl: int = 600000
    min_data_versions: int = 0
    max_data_versions: int = 1
    _required_tags: list[OrmEnumValue] = dataclass_field(default_factory=list)
    _excluded_tags: list[OrmEnumValue] = dataclass_field(default_factory=list)
    custom_options: Optional[dict[str, Any]] = None

    @property
    def required_tags(self):
        return ", ".join("/*{}*/ {}".format(tag.enum_value_name, tag.number) for tag in self._required_tags)

    @property
    def excluded_tags(self):
        return ", ".join("/*{}*/ {}".format(tag.enum_value_name, tag.number) for tag in self._excluded_tags)


@dataclass
class OrmHistoryAttribute:
    path: str
    indexed: bool = False
    allowed_in_filter: bool = False


@dataclass
class OrmHistoryOptions:
    attributes: list[OrmHistoryAttribute] = dataclass_field(default_factory=list)
    filter: Optional[str] = None


@dataclass
class OrmRevisionTracker:
    tracked_paths: list[str]
    path: str
    name: str
    lock_group_restriction_enabled: bool = False
    tracker_field: Optional["OrmField"] = None
    excluded_attributes: list["OrmAttribute"] = dataclass_field(default_factory=list)


@dataclass
class OrmIndexedGeoPoint:
    geohash_length: int
    geohash_field: Optional["OrmField"] = None
    geo_point: Optional["OrmField"] = None


@dataclass
class OrmAttributeSensor:
    policy: object_pb2.EAttributeSensorPolicy


@dataclass
class OrmAttributeMigration:
    source: OrmAttribute
    target: OrmAttribute
    custom_migrator: bool
    on_conflict_action: str
    reverse_write: bool
    custom_reverse_writer: bool


class ObjectFieldsVisitor:
    def __init__(self, obj):
        self.object = obj
        self.visited = set()

    def visit(self):
        self.visit_message_fields_recursive(self.object.meta, "/meta")
        self.visit_message_fields_recursive(self.object.spec, "/spec")
        self.visit_message_fields_recursive(self.object.status, "/status")
        self.visit_message_fields_recursive(self.object.control, "/control")

    def visit_message_fields_recursive(
        self,
        message: OrmMessage,
        path: str,
    ):
        if not message or message.full_name in self.visited:
            return

        message_name = message.full_name

        def process_field(name: str, field: OrmField):
            field_path = path + "/" + name
            descend = self.on_field(field_path, field)
            if descend and field.value_message:
                self.visit_message_fields_recursive(field.value_message, field_path)

        self.visited.add(message_name)
        for name, field in message.fields_by_name.items():
            process_field(name, field)
        for field in message.reference_fields_deprecated:
            process_field(field.snake_case_name, field)
        self.visited.remove(message_name)

    def on_field(self, field_path: str, field: "OrmField") -> bool:  # type: ignore
        pass


class FieldPathSetter(ObjectFieldsVisitor):
    def __init__(self, obj):
        super().__init__(obj)

    def on_field(self, field_path: str, field: "OrmField") -> bool:
        field.path = field_path
        return not field.is_view and not (field.value_message and field.value_message.is_object_message)


class ReferenceDeprecatedCollector(ObjectFieldsVisitor):
    def __init__(self, obj):
        super().__init__(obj)
        self.result = {}

    def on_field(self, field_path: str, field: "OrmField") -> bool:
        if field.foreign_object_type and not field.is_view:
            self.result[field_path] = field
        return not field.is_view and not (field.value_message and field.value_message.is_object_message)


class ReferenceCollector(ObjectFieldsVisitor):
    def __init__(self, obj):
        super().__init__(obj)
        self.result = []

    def on_field(self, field_path: str, field: "OrmField") -> bool:
        if field.reference:
            self.result.append(field.reference)
        return not field.is_view and not (field.value_message and field.value_message.is_object_message)


class ChildrenViewsCollector(ObjectFieldsVisitor):
    def __init__(self, obj):
        super().__init__(obj)
        self.result = []

    def on_field(self, field_path: str, field: "OrmField") -> bool:
        if field.children_view:
            self.result.append(field_path)
        return not field.is_view and not (field.value_message and field.value_message.is_object_message)


class StrictEnumChecksEvaluator(ObjectFieldsVisitor):
    def __init__(self, obj, default_strict_enum_value_check: bool):
        super().__init__(obj)
        self.default_strict_enum_value_check = default_strict_enum_value_check

    def _get_effective_strict_enum_value_check(self, field: "OrmField") -> bool:
        assert field.value_enum
        field_strict_check = field.strict_enum_value_check
        if field_strict_check is not None:
            return field_strict_check
        return self.default_strict_enum_value_check

    def _set_strict_check(self, field_path: str, field: "OrmField"):
        effective_strict_value_check = self._get_effective_strict_enum_value_check(field)
        assert not field.is_column or effective_strict_value_check, (
            f"Columnar enum attribute {field_path} of object {self.object.snake_case_name} "
            "must have strict value checks enabled"
        )
        field.set_effective_strict_enum_value_check(effective_strict_value_check)

    def on_field(self, field_path: str, field: "OrmField") -> bool:
        if field.value_enum:
            self._set_strict_check(field_path, field)
        return not field.is_view and not (field.value_message and field.value_message.is_object_message)


class GeoPointsCollector(ObjectFieldsVisitor):
    def __init__(self, obj):
        super().__init__(obj)
        self.result = []

    def on_field(self, field_path: str, field: "OrmField") -> bool:
        geohash_option = field.get_option(object_pb2.geohash)
        if geohash_option:
            assert (
                field.proto_type == "string"
            ), f"Fields marked with geohash option must have string proto_type, found {field.proto_type} for {field_path}"
            field.path = field_path
            geo_point_field = self.object.resolve_attribute_by_path(geohash_option.source_geo_point_path).field
            geo_point_field.path = geohash_option.source_geo_point_path
            assert (
                geo_point_field
            ), f"Icorrect field path of source_geo_point_path for geohash option of field {field_path}"
            assert (
                geo_point_field.proto_value_type == "NYT.NOrm.NDataModel.TGeoPoint"
            ), "Field specified in source_geo_point_path must have NYT.NOrm.NDataModel.TGeoPoint type"

            hash_length = self.object.package_desc.database_options.default_geohash_length
            if geohash_option.length:
                hash_length = geohash_option.length
            self.result.append(
                OrmIndexedGeoPoint(
                    geohash_length=hash_length,
                    geo_point=geo_point_field,
                    geohash_field=field,
                )
            )
        return not field.is_view and not (field.value_message and field.value_message.is_object_message)


@dataclass
class OrmObject:
    root: OrmMessage
    package_desc: "OrmPackage"
    object_type_option: object_pb2.TObjectTypeOption
    owning_module: "OrmModule"

    camel_case_name: Optional[str] = None
    snake_case_name: Optional[str] = None
    lower_camel_case_name: Optional[str] = None
    hash_policy: Optional[object_pb2.EHashPolicy] = None
    type_value: Optional[int] = None
    add_touch_history: Optional[bool] = None
    builtin: Optional[bool] = None
    custom_type_handler: Optional[bool] = None
    custom_cpp_object: Optional[bool] = None
    enable_tags: Optional[bool] = None
    has_embedded_semaphores: Optional[bool] = None
    name_supported: Optional[bool] = None
    skip_store_without_changes: Optional[bool] = None
    force_zero_key_evaluation: Optional[bool] = False
    parent: Optional["OrmObject"] = None
    access_control_parent: Optional[OrmField] = None
    parent_key_storage_policy: Optional[object_pb2.EParentKeyStoragePolicy] = None
    _camel_case_foreign_name: Optional[str] = None
    _snake_case_foreign_name: Optional[str] = None
    parent_field_number_offset: Optional[int] = None
    null_key_values: Optional[list[str]] = None
    revision_trackers: list[OrmRevisionTracker] = dataclass_field(default_factory=list)
    nested_object_field_messages: bool = False
    use_custom_default_acl: bool = False
    default_acl: list = dataclass_field(default_factory=list)
    hashed_geo_points: list[OrmIndexedGeoPoint] = dataclass_field(default_factory=list)

    children: list["OrmObject"] = dataclass_field(default_factory=list)
    history: OrmHistoryOptions = dataclass_field(default_factory=OrmHistoryOptions)
    indexes: list[OrmIndex] = dataclass_field(default_factory=list)
    primary_key: list[OrmField] = dataclass_field(default_factory=list)
    watch_logs: list[OrmWatchLog] = dataclass_field(default_factory=list)

    custom_options: list[dict] = dataclass_field(default_factory=list)
    attribute_sensors: dict[str, OrmAttributeSensor] = dataclass_field(default_factory=dict)

    table_columns: list["OrmColumn"] = dataclass_field(default_factory=list)
    instantiated: bool = False

    references_deprecated: dict[str, "OrmField"] = dataclass_field(default_factory=dict)
    references: list["OrmReference"] = dataclass_field(default_factory=list)

    foreign_objects: list["OrmObject"] = dataclass_field(default_factory=list)

    attribute_migrations: list[OrmAttributeMigration] = dataclass_field(default_factory=list)

    forbid_parent_removal: Optional[bool] = None
    remove_legacy_meta_account_id: Optional[bool] = None

    def __repr__(self) -> str:
        return f"({self.snake_case_name})"

    def __hash__(self):
        return hash(self.snake_case_name)

    @property
    def meta(self) -> OrmMessage:
        return self.root.fields_by_name["meta"].value_message

    @property
    def spec(self) -> OrmMessage:
        return self.root.fields_by_name["spec"].value_message

    @property
    def status(self) -> OrmMessage:
        return self.root.fields_by_name["status"].value_message

    @property
    def control(self) -> OrmMessage:
        return self.root.fields_by_name["control"].value_message

    @classmethod
    def make(
        cls,
        object_type_option: object_pb2.TObjectTypeOption,
        root: OrmMessage,
        package_desc: "OrmPackage",
        owning_module: "OrmModule",
    ):
        result = cls(root, package_desc, object_type_option, owning_module)

        for field in root.fields_by_name.values():
            if field.value_message:
                field.value_message.is_object_message = True

        result.camel_case_name = object_type_option.camel_case_name
        result.snake_case_name = object_type_option.snake_case_name
        result.lower_camel_case_name = result.camel_case_name[:1].lower() + result.camel_case_name[1:]
        result.human_readable_name = getattr(object_type_option, "human_readable_name", None)
        if not result.human_readable_name:
            result.human_readable_name = result.snake_case_name.replace("_", " ")
        result._camel_case_foreign_name = object_type_option.camel_case_foreign_name
        result._snake_case_foreign_name = object_type_option.snake_case_foreign_name
        result.hash_policy = package_desc.database_options.hash_policy
        if object_type_option.HasField("hash_policy"):
            result.hash_policy = object_type_option.hash_policy
        result.type_value = object_type_option.type_value
        result.builtin = getattr(object_type_option, "builtin_object", False)
        result.enable_tags = object_type_option.enable_tags
        null_key = getattr(object_type_option, "custom_null_key", None)
        if null_key:
            result.null_key_values = null_key.split(";")

        if len(object_type_option.history.attributes) > 0:
            result.history = OrmHistoryOptions()
            result.add_touch_history = True
            if object_type_option.history.HasField("filter"):
                result.history.filter = object_type_option.history.filter
            for attribute in object_type_option.history.attributes:
                result.history.attributes.append(
                    OrmHistoryAttribute(
                        path=attribute.path,
                        indexed=attribute.indexed,
                        allowed_in_filter=attribute.allowed_in_filter,
                    )
                )
            unique_paths = set(map(lambda attr: attr.path, result.history.attributes))
            assert len(unique_paths) == len(
                result.history.attributes
            ), f"Duplicate history attributes found for {result.camel_case_name}"
        for watch_log in object_type_option.watch_logs:
            assert watch_log.snake_case_name.endswith(
                "watch_log"
            ), f'Watch log {watch_log.snake_case_name} for {result.camel_case_name} does not end with "_watch_log"'
            result.watch_logs.append(
                OrmWatchLog(
                    watch_log.snake_case_name,
                    watch_log.filter,
                    watch_log.selectors,
                    watch_log.required_tags,
                    watch_log.excluded_tags,
                    watch_log.tablet_count,
                    watch_log.min_data_ttl,
                    watch_log.max_data_ttl,
                    watch_log.min_data_versions,
                    watch_log.max_data_versions,
                )
            )
        result.name_supported = object_type_option.name_supported
        if not result.name_supported:
            result.meta.fields_by_name["name"].set_update_policy(object_pb2.UP_READ_ONLY)
            result.meta.fields_by_name["name"].set_not_initializable()
        if object_type_option.HasField("skip_store_without_changes"):
            result.skip_store_without_changes = object_type_option.skip_store_without_changes
        else:
            result.skip_store_without_changes = package_desc.database_options.skip_store_without_changes
        result.custom_type_handler = object_type_option.custom_type_handler
        result.custom_cpp_object = object_type_option.custom_cpp_object
        result.skip_cpp = compat.yp() and not object_type_option.yp_generate_cpp
        result.yp_compat_includes = object_type_option.yp_compat_includes
        if object_type_option.HasField("force_zero_key_evaluation"):
            result.force_zero_key_evaluation = object_type_option.force_zero_key_evaluation
        else:
            result.force_zero_key_evaluation = package_desc.database_options.force_zero_key_evaluation

        result.parent_key_storage_policy = package_desc.database_options.parent_key_storage_policy
        if object_type_option.HasField("parent_key_storage_policy"):
            result.parent_key_storage_policy = object_type_option.parent_key_storage_policy

        if object_type_option.HasField("parent_field_number_offset"):
            result.parent_field_number_offset = object_type_option.parent_field_number_offset
        else:
            result.parent_field_number_offset = package_desc.database_options.parent_field_number_offset

        if object_type_option.HasField("nested_object_field_messages"):
            result.nested_object_field_messages = object_type_option.nested_object_field_messages
        else:
            result.nested_object_field_messages = package_desc.database_options.nested_object_field_messages

        result.custom_options = object_type_option.custom_options

        result.forbid_parent_removal = package_desc.database_options.forbid_parent_removal
        if object_type_option.HasField("forbid_parent_removal"):
            result.forbid_parent_removal = object_type_option.forbid_parent_removal

        result.remove_legacy_meta_account_id = package_desc.database_options.remove_legacy_meta_account_id
        if object_type_option.HasField("remove_legacy_meta_account_id"):
            result.remove_legacy_meta_account_id = object_type_option.remove_legacy_meta_account_id

        result.root.object = result
        for message in result.messages():
            message.object = result

        result._analyze_embedded_semaphores()

        result._analyze_finalizers()

        result._analyze_revision_trackers(
            package_desc.database_options.revision_trackers, object_type_option.revision_trackers
        )

        result._analyze_geo_points()

        if object_type_option.primary_key:
            meta_fields = result.meta.fields_by_name.copy()
            for key_name in object_type_option.primary_key:
                key_field = meta_fields.pop(key_name, None)  # Pop to prevent duplicates.
                assert key_field, "Key field {} of {} must be in /meta (and used exactly once)".format(
                    key_name, result.snake_case_name
                )
                assert key_field.proto_type in (
                    "int64",
                    "uint64",
                    "sint64",
                    "fixed64",
                    "sfixed64",
                    "string",
                ), "Key field {} of {} has forbidden key type {}".format(
                    key_name, result.snake_case_name, result.key_field.proto_type
                )
                key_field.is_primary = True
                key_field.set_column()
                if not key_field.generation_policy:
                    key_field.generation_policy = "Manual"
                key_field.set_update_policy(object_pb2.UP_READ_ONLY)
                result.primary_key.append(key_field)

        assert result.primary_key, "Primary key must be specified for {}".format(result.camel_case_name)

        if object_type_option.access_control_parent:
            access_control_parent_field = result.meta.fields_by_name.get(object_type_option.access_control_parent)
            assert (
                access_control_parent_field
            ), f"Unknown meta field {object_type_option.access_control_parent} for access control parent"
            result.access_control_parent = access_control_parent_field
            access_control_parent_field.is_access_control_parent = True

        if object_type_option.use_custom_default_acl:
            result.use_custom_default_acl = True
            result.default_acl = object_type_option.default_acl

        for attribute_sensor in object_type_option.attribute_sensors:
            result.attribute_sensors[attribute_sensor.path] = OrmAttributeSensor(policy=attribute_sensor.policy)

        return result

    def collect_reference_deprecated_attributes(self):
        visitor = ReferenceDeprecatedCollector(self)
        visitor.visit()
        self.references_deprecated = visitor.result
        return [self.resolve_attribute_by_path(path) for path in visitor.result.keys()]

    def collect_children_views(self):
        visitor = ChildrenViewsCollector(self)
        visitor.visit()
        return [self.resolve_attribute_by_path(path) for path in visitor.result]

    def make_references(self):
        visitor = ReferenceCollector(self)
        visitor.visit()
        self.references = visitor.result

    def compute_tags(self, tags_enum: OrmEnum):
        for watch_log in self.watch_logs:
            watch_log._required_tags = [
                enum_value_name_to_value(tags_enum, tag) for tag in watch_log.required_tags_strs
            ]
            watch_log._excluded_tags = [
                enum_value_name_to_value(tags_enum, tag) for tag in watch_log.excluded_tags_strs
            ]
            assert (
                not (watch_log._required_tags or watch_log._excluded_tags) or self.enable_tags
            ), "Cannot use require or exclude tags in watchlog {} with tags disabled for object {}".format(
                watch_log.snake_case_name, self.snake_case_name
            )
            tags_intersection = list(set(watch_log._required_tags) & set(watch_log._excluded_tags))
            assert (
                len(tags_intersection) == 0
            ), "Using the same tags in require and exclude tags is not allowed. Tags found: {}".format(
                ", ".join(tag.enum_value_name for tag in tags_intersection)
            )

    def finalize(self, context: "OrmContext"):
        self.root.finalize(context)

        resolve_tags(self)

        self.make_references()

        foreign_objects = set(self.foreign_objects)

        for child in self.children:
            foreign_objects.add(child)
        if self.parent:
            foreign_objects.add(self.parent)

        for object in [f.transitive_reference.foreign_object for f in self.meta.fields if f.transitive_reference]:
            foreign_objects.add(object)

        foreign_objects.discard(self)

        self.foreign_objects = sorted(foreign_objects, key=lambda x: x.snake_case_name)

    def evaluate_strict_enum_value_checks(self, default_strict_enum_value_check: bool):
        StrictEnumChecksEvaluator(self, default_strict_enum_value_check).visit()

    def messages(self):
        return [f.value_message for f in self.root.fields if f.value_message]

    def all_messages(self):
        result: list[OrmMessage] = []
        visited_messages: set[str] = set()

        def dfs(message: OrmMessage):
            if message.full_name in visited_messages:
                return
            else:
                visited_messages.add(message.full_name)
            for field in message.fields:
                if field.value_message:
                    result.append(field.value_message)
                    dfs(field.value_message)

        dfs(self.root)
        return result

    def resolve_attribute_by_path(self, path: str) -> OrmAttribute:
        path_and_etc_name = path.split(ETC_NAME_SEPARATOR)
        parts: list[str] = path_and_etc_name[0].split(FIELD_PATH_SEP)
        etc_name = None if len(path_and_etc_name) == 1 else path_and_etc_name[1]
        fields: list[OrmField] = []
        assert len(parts) >= 2, "Attribute {} must contain at least 2 parts".format(path)
        assert not parts[0], "Attribute {} must start with `/`".format(path)
        assert parts[1] in TOP_LEVEL_FIELDS, "First part of attribute {} must be one of {}".format(
            path, TOP_LEVEL_FIELDS
        )

        column_field_index = 0
        field = self.root.fields_by_name[parts[1]]
        fields.append(field)

        for i, name in enumerate(parts[2:]):
            assert field.value_message, "Attribute {} must be a message by path {}".format(path, field.snake_case_name)
            assert name in field.value_message.fields_by_name, "Attribute {} has no child {}".format(
                "/".join(parts[: i + 2]), name
            )
            field = field.value_message.fields_by_name[name]
            fields.append(field)
            if field.is_column or (field.value_message and field.value_message.is_composite):
                column_field_index = len(fields) - 1

        assert (
            etc_name is None or column_field_index == len(fields) - 1
        ), "Path {} explicitly specifies etc name for non-column attribute".format(path)
        column_field = fields[column_field_index]
        assert etc_name is None or (
            column_field.value_message is not None and column_field.value_message.is_composite
        ), "Path {} explicitly specifies etc name for non-composite attribute".format(path)
        etc_name_parts: list[str] | None = None
        if etc_name is not None:
            etc_name_parts = etc_name.split(".")
        assert etc_name_parts is None or (
            any(etc.suffix_parts() == etc_name_parts for etc in column_field.value_message.etcs)
        ), "Path {} explicitly specifies a non-existent etc name".format(path)

        return OrmAttribute(
            fields=fields,
            column_field_index=column_field_index,
            etc_name_parts=etc_name_parts,
        )

    def resolve_fields(self, path: str, allow_prefix: bool = False) -> list[OrmField]:
        parts: list[str] = path.split(FIELD_PATH_SEP)
        assert len(parts) >= 2, "Attribute path {} must contain at least one entry".format(path)
        assert not parts[0], "Attribute path {} must start with `/`".format(path)

        message: OrmMessage | None = self.root
        fields: list[OrmField] = []

        for field_name in parts[1:]:
            assert message, f"No message for field {field_name}"

            field = message.fields_by_name.get(field_name)
            if not field:
                assert allow_prefix, "Message {} has no field {}".format(message.full_name, field_name)
                break
            fields.append(field)
            if field.value_message:
                message = field.value_message
            else:
                message = None

        return fields

    def _analyze_embedded_semaphores(self):
        self.has_embedded_semaphores = any(
            f.is_embedded_semaphore for message in self.messages() for f in message.fields
        )

        if self.has_embedded_semaphores:
            semaphore_control = self.package_desc.get_message(
                f"{self.package_desc.target_proto_namespace}.TGenericObjectEmbeddedSemaphoreControl"
            )
            control_field = self.root.fields_by_name["control"]
            control_field.value_message = self.package_desc.capture_message(
                semaphore_control, into=control_field.value_message
            )

    def _analyze_finalizers(self):
        if self.package_desc.database_options.enable_finalizers:
            finalizers_mixin = self.package_desc.get_message(
                f"{self.package_desc.target_proto_namespace}.TGenericObjectFinalizer"
            )
            meta = self.root.fields_by_name["meta"]
            meta.value_message = self.package_desc.capture_message(finalizers_mixin, into=meta.value_message)

    def _hash_expression(self, parent_key_columns, key_columns):
        columns = []

        if self.hash_policy == object_pb2.EHashPolicy.HP_NO_HASH_COLUMN:
            return None

        if self.hash_policy == object_pb2.EHashPolicy.HP_FARM_HASH_ENTIRE_KEY:
            columns = parent_key_columns + key_columns

        if self.hash_policy == object_pb2.EHashPolicy.HP_FARM_HASH_PARENT_KEY:
            columns = parent_key_columns

        if self.hash_policy == object_pb2.EHashPolicy.HP_FARM_HASH_PARENT_OR_OBJECT_KEY:
            if parent_key_columns:
                columns = parent_key_columns
            else:
                columns = key_columns

        assert columns, "For {}, could not choose columns for hash_policy {}".format(
            self.snake_case_name,
            object_pb2.EHashPolicy.Name(self.hash_policy) if self.hash_policy is not None else "None",
        )

        return "farm_hash({})".format(", ".join([f"[{c.name}]" for c in columns]))

    def _build_orm_tracker(self, tracker: object_pb2.TRevisionTracker):
        assert tracker.tracker_path, "Tracker path should be specified"
        assert tracker.tracked_paths, "At least one tracked path should be specified"
        for tracked_path in tracker.tracked_paths:
            assert tracked_path[0] == FIELD_PATH_SEP, "Tracked path should start with /"

        tracker_field = self.resolve_attribute_by_path(tracker.tracker_path).field
        assert tracker_field.is_column, f"Tracked attribute {tracker_field.path} is not column"
        assert tracker_field.proto_type == "fixed64", "Revision attributes must have fixed64 proto type"
        add_tags = tracker_field.get_option(object_pb2.add_tags, [])
        remove_tags = tracker_field.get_option(object_pb2.remove_tags, [])
        assert (
            not add_tags and not remove_tags
        ), f"Tags are not supported for revision trackers (Object: {self.snake_case_name}, Tracker: {tracker_field.path})"

        excluded_attributes = []
        for excluded_path in tracker.excluded_paths:
            attribute = self.resolve_attribute_by_path(excluded_path)
            assert not attribute.field.computed, "Computed attributes can not be in excluded field"
            excluded_attributes.append(attribute)

        return OrmRevisionTracker(
            path=tracker.tracker_path,
            tracked_paths=list(tracker.tracked_paths),
            name=tracker_field.snake_case_name,
            lock_group_restriction_enabled=bool(tracker.restrict_to_lock_group),
            tracker_field=tracker_field,
            excluded_attributes=excluded_attributes,
        )

    @staticmethod
    def _merge_revision_trackers(
        package_revision_trackers: list[object_pb2.TRevisionTracker] = None,
        object_revision_trackers: list[object_pb2.TRevisionTracker] = None,
    ):
        disable_trackers_for_paths = set()
        for tracker in list(package_revision_trackers) + list(object_revision_trackers):
            if tracker.disabled:
                for tracked_path in tracker.tracked_paths:
                    disable_trackers_for_paths.add(tracked_path)

        trackers_by_path = {}
        for tracker in package_revision_trackers:
            if disable_trackers_for_paths.intersection(tracker.tracked_paths):
                continue
            trackers_by_path[tracker.tracker_path] = tracker

        # object trackers overwrite package trackers
        for tracker in object_revision_trackers:
            if disable_trackers_for_paths.intersection(tracker.tracked_paths):
                continue
            trackers_by_path[tracker.tracker_path] = tracker

        return [trackers_by_path[path] for path in sorted(trackers_by_path)]

    def _analyze_revision_trackers(
        self,
        package_revision_trackers: list[object_pb2.TRevisionTracker] = None,
        object_revision_trackers: list[object_pb2.TRevisionTracker] = None,
    ):
        revision_trackers = self._merge_revision_trackers(package_revision_trackers, object_revision_trackers)
        if not revision_trackers:
            return

        for tracker in revision_trackers:
            orm_tracker = self._build_orm_tracker(tracker)
            self.revision_trackers.append(orm_tracker)

    def _analyze_geo_points(self):
        visitor = GeoPointsCollector(self)
        visitor.visit()
        self.hashed_geo_points = visitor.result

    def link_attribute_migrations(self):
        for object_attribute_migration in self.object_type_option.attribute_migrations:
            if (
                object_attribute_migration.on_conflict
                == object_pb2.EOnMigrationConflictAction.ON_MIGRATION_CONFLICT_USE_TARGET
            ):
                on_conflict = "target"
            elif (
                object_attribute_migration.on_conflict
                == object_pb2.EOnMigrationConflictAction.ON_MIGRATION_CONFLICT_USE_SOURCE
            ):
                on_conflict = "source"
            elif (
                object_attribute_migration.on_conflict
                == object_pb2.EOnMigrationConflictAction.ON_MIGRATION_CONFLICT_ERROR
            ):
                on_conflict = "error"
            else:
                assert False, f"Unhandled on conflict action {object_attribute_migration.on_conflict}"

            migration = OrmAttributeMigration(
                source=self.resolve_attribute_by_path(object_attribute_migration.source),
                target=self.resolve_attribute_by_path(object_attribute_migration.target),
                custom_migrator=object_attribute_migration.custom_migrator or False,
                on_conflict_action=on_conflict,
                reverse_write=object_attribute_migration.reverse_write,
                custom_reverse_writer=object_attribute_migration.custom_reverse_writer,
            )
            assert (
                migration.reverse_write or not migration.custom_reverse_writer
            ), "Cannot use custom reverse writer for migration without reverse write"
            self.attribute_migrations.append(migration)

    @property
    def table_name(self):
        """The primary YT table storing this object."""
        return table_name(self)

    @property
    def table_cpp_name(self):
        """The primary YT table storing this object."""
        return f"{collective_name(self)}Table"

    @property
    def camel_case_foreign_name(self):
        return self._camel_case_foreign_name or self.camel_case_name

    @property
    def snake_case_foreign_name(self):
        return self._snake_case_foreign_name or self.snake_case_name

    def key_columns(
        self,
        meta: bool = False,
        parent: bool = False,
        self_ref: bool = False,
        source: bool = False,
        target: bool = False,
        override_prefix: Optional[str] = None,
        group: str = PRIMARY_DATA_GROUP,
    ):
        prefix = "meta." if meta else ""
        cpp_prefix = "Meta" if meta else ""
        foreign = parent or source or target

        if source and self_ref:
            prefix = prefix + "source_"
            cpp_prefix = cpp_prefix + "Source"
        elif target and self_ref:
            prefix = prefix + "target_"
            cpp_prefix = cpp_prefix + "Target"

        if foreign:
            prefix = f"{prefix}{foreign_key_prefix(self)}_"
            cpp_prefix = f"{cpp_prefix}{self.camel_case_foreign_name}"

        if override_prefix:
            prefix = override_prefix + "_"
            cpp_prefix = snake_to_camel_case(override_prefix)

        key_columns = []

        for field in self.primary_key:
            if foreign:
                field = copy(field)
            key_columns.append(
                OrmColumn.make(
                    field,
                    name=f"{prefix}{field.snake_case_name}",
                    cpp_name=f"{cpp_prefix}{field.camel_case_name}",
                    group=group,
                    is_reference_source=source,
                    is_reference_target=target,
                    is_parent_key_part=parent,
                )
            )

        return key_columns

    def _table_columns(self):
        parent_key_columns = []
        key_columns = []

        if self.parent:
            parent_key_columns = self.parent.key_columns(meta=True, parent=True)

        key_columns = self.key_columns(meta=True)

        hash_expression = self._hash_expression(parent_key_columns, key_columns)
        if hash_expression:
            yield OrmColumn.make_hash(hash_expression)

        yield from parent_key_columns
        yield from key_columns

        if "account_id" not in self.meta.fields_by_name and not self.remove_legacy_meta_account_id:
            yield OrmColumn(
                name="meta.account_id",
                cpp_name="MetaAccountId",
                type="string",
                lock="api",
                is_dummy=True,
                group=self.meta.data_group,
            )

        assert (
            len(self.meta.etcs) <= 1
        ), "Meta does not support multiple etcs, consider moving fields with lock or data group to a separate column"
        meta_etc_column = OrmColumn(
            name="meta.etc",
            cpp_name="MetaEtc",
            type="any",
            cpp_yt_type="NYT::NTableClient::EValueType::Any",
            group=self.meta.data_group,
        )
        self.meta.etc_columns = [meta_etc_column]
        yield meta_etc_column

        yield OrmColumn(
            name="existence_lock", cpp_name="ExistenceLock", type="boolean", lock="existence_lock", is_base=True
        )

        if self.package_desc.database_options.enable_history_snapshot_column:
            yield OrmColumn(
                name="meta.history_snapshot_timestamp",
                cpp_name="MetaHistorySnapshotTimestamp",
                type="uint64",
                lock="history_lock",
                is_base=True,
            )

        for field in self.meta.all_fields:
            if field.is_primary or not field.is_column:
                continue
            yield OrmColumn.make(field, prefix="meta", cpp_prefix="Meta")

        yield from self._columns_from_fields(self.spec, prefix="spec", cpp_prefix="Spec")
        yield from self._columns_from_fields(self.status, prefix="status", cpp_prefix="Status")

        yield OrmColumn(name="labels", cpp_name="Labels", type="any", is_base=True)

    @property
    def parents_table_name(self):
        return f"{table_name(self)}_to_{table_name(self.parent)}"

    @property
    def parents_table_cpp_name(self):
        return f"{collective_name(self)}To{collective_name(self.parent)}Table"

    @property
    def parents_table_columns(self):
        if self.parent:
            key_columns = self.key_columns(meta=True, group=None)

            parent_key_columns = self.parent.key_columns(meta=True, parent=True, group=None)
            for column in parent_key_columns:
                column.sort_order = None

            hash_expression = self._hash_expression(key_columns, [])
            if hash_expression:
                yield OrmColumn.make_hash(hash_expression, group=None)

            yield from key_columns
            yield from parent_key_columns

    @property
    def separate_parents_table(self):
        return self.parent and self.parent_key_storage_policy == (
            object_pb2.EParentKeyStoragePolicy.PKSP_PREFIX_WITH_SEPARATE_PARENTS_TABLE
        )

    def _columns_from_fields(self, message, prefix, cpp_prefix, etc_column=None):
        etc_columns = []
        if self.instantiated:
            message.instantiated_etcs = message.etcs
        for etc in message.etcs:
            etc_column = OrmColumn(
                name=f"{prefix}{etc.schema_suffix}",
                cpp_name=f"{cpp_prefix}{etc.field_suffix}",
                type="any",
                cpp_yt_type="NYT::NTableClient::EValueType::Any",
                lock=etc.lock_group,
                group=etc.data_group,
            )
            etc_columns.append(etc_column)
            etc.column = etc_column
            for field in etc.fields:
                field.column = etc_column

        for field in message.all_fields:
            if field.ignore_columns:
                continue
            if field.is_column:
                yield OrmColumn.make(field, prefix=prefix, cpp_prefix=cpp_prefix)
            elif field.value_message and field.value_message.is_composite:
                if self.instantiated:
                    field.value_message = field.value_message.instantiate(field)
                yield from self._columns_from_fields(
                    field.value_message,
                    prefix=f"{prefix}.{field.snake_case_name}",
                    cpp_prefix=f"{cpp_prefix}{field.camel_case_name}",
                    etc_column=etc_column,
                )

        for column in etc_columns:
            yield column

    @cached_property
    def all_key_fields(self):
        assert self.instantiated
        return [column.field for column in self.table_columns if column.is_key and not column.is_hash]

    @cached_property
    def key_fields(self):
        return [f for f in self.all_key_fields if not f.is_parent_key_field]

    @cached_property
    def parent_key_fields(self):
        return [f for f in self.all_key_fields if f.is_parent_key_field]

    @cached_property
    def meta_parent_primary_key_fields(self):
        return list(map(self._get_meta_parent_field, self.parent.primary_key))

    @cached_property
    def meta_parent_key_fields(self):
        return list(map(self._get_meta_parent_field, self.parent_key_fields))

    def _get_meta_parent_field(self, parent_field):
        meta_field_name = f"{foreign_key_prefix(self.parent)}_{parent_field.snake_case_name}"
        return self.meta.fields_by_name[meta_field_name]

    def _add_service_fields(self):
        meta = self.meta
        if self.parent:
            for f in self.parent_key_fields:
                field = OrmField(
                    snake_case_name=f"{foreign_key_prefix(self.parent)}_{f.snake_case_name}",
                    lower_camel_case_name="",
                    proto_value_type=f.proto_value_type,
                    number=f.number + self.parent_field_number_offset,
                    column=f.column,
                    parent=meta,
                    yt_schema_value_type=f.yt_schema_value_type,
                )
                field.set_column()
                field.set_system()
                field.set_update_policy(object_pb2.UP_READ_ONLY)
                f.column.field = field
                meta._add_field(field)

        type_field = OrmField(
            snake_case_name="type",
            lower_camel_case_name="",
            proto_value_type="EObjectType",
            number=self.package_desc.database_options.meta_type_field_number,
        )
        type_field.set_computed()
        type_field.set_system()
        type_field.set_not_initializable()
        type_field.set_update_policy(object_pb2.UP_READ_ONLY)
        meta._add_field(type_field)

        meta._rebuild_field_lists()

        meta.instantiated_etcs = meta.etcs
        if len(meta.etcs) >= 1:
            meta.etcs[0].column = meta.etc_columns[0]

    def _verify(self):
        assert (
            not self.package_desc.database_options.no_legacy_parents_table
            or not self.parent
            or self.separate_parents_table
        ), f"Legacy 'parents' table is disabled, but object {self.snake_case_name} still needs it"

        assert all(
            (
                self.resolve_attribute_by_path(path).field.aggregate is None
                for tracker in self.revision_trackers
                for path in tracker.tracked_paths + [tracker.path]
            )
        )

    def instantiate(self):
        assert not self.instantiated
        self.instantiated = True
        self.table_columns = list(self._table_columns())
        self._add_service_fields()
        self._analyze_null_key()
        self._verify()

    def _analyze_null_key(self):
        if not self.null_key_values:
            self.null_key_values = [DEFAULTS_BY_KEY_FIELD_TYPE[field.cpp_type] for field in self.key_fields]
        assert len(self.null_key_values) == len(self.key_fields), (
            f"Object {self.snake_case_name} has {len(self.key_fields)} key fields but "
            f"{len(self.null_key_values)} null key values"
        )

    @property
    def null_key_constructor_params(self):
        return object_key_constructor_params(self.null_key_values, self.key_fields)
