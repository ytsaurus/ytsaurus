from .common import OrmContext, snake_case_to_upper_camel_case
from .message import OrmEnum, OrmMessage
from yt_proto.yt.orm.client.proto import object_pb2
from yt_proto.yt.core.yson.proto import protobuf_interop_pb2

from google.protobuf import descriptor_pb2 as protobuf_descriptor_pb2
from dataclasses import dataclass, field as dataclass_field
import posixpath
from typing import Optional, Any

import importlib
import pkgutil

CUSTOM_NAME_OPTIONS = {
    "": "root_base_message_type_name",
    "Meta": "meta_base_message_type_name",
    "Control": "control_base_message_type_name",
    "Spec": "spec_message_type_name",
    "Status": "status_message_type_name",
}


@dataclass
class OrmModule:
    path: Optional[str] = None
    original_proto_namespace: Optional[str] = None
    target_proto_namespace: Optional[str] = None
    is_source: Optional[bool] = None
    is_proto3: Optional[bool] = None
    not_exported: Optional[bool] = None
    java_outer_classname: Optional[str] = None
    java_multiple_files: Optional[bool] = None
    object_type_options: Any = None
    index_options: Any = None
    exported_extensions: dict[str, Any] = dataclass_field(default_factory=dict)

    public_imports: list[str] = dataclass_field(default_factory=list)
    messages: list[OrmMessage] = dataclass_field(default_factory=list)
    enums: list[OrmEnum] = dataclass_field(default_factory=list)

    _output_filename: Optional[str] = None
    _is_public: bool = False
    _options: Any = None

    @classmethod
    def make(cls, proto_file_descriptor: Any, is_source: bool, package: "OrmPackage"):
        if is_source:
            target_proto_namespace = package.target_proto_namespace
        else:
            target_proto_namespace = proto_file_descriptor.package

        result = cls(
            path=proto_file_descriptor.name,
            original_proto_namespace=proto_file_descriptor.package,
            target_proto_namespace=target_proto_namespace,
            is_source=is_source,
            is_proto3=proto_file_descriptor.syntax == "proto3",
        )

        result._analyze_imports(proto_file_descriptor.public_dependencies)
        result._analyze_options(proto_file_descriptor.GetOptions())
        result._analyze_enums(proto_file_descriptor.enum_types_by_name, package)
        result._analyze_messages(proto_file_descriptor.message_types_by_name, package)
        return result

    @property
    def filename(self) -> str:
        return self._output_filename

    @property
    def is_public(self) -> bool:
        return not self.is_source and self._is_public

    def _analyze_imports(self, public_dependencies):
        for proto_file_descriptor in public_dependencies:
            self.public_imports.append(proto_file_descriptor.name)

    def _analyze_options(self, options):
        self._options = options
        self.java_outer_classname = options.java_outer_classname
        self.java_multiple_files = options.java_multiple_files
        for key in (protobuf_interop_pb2.derive_underscore_case_names,):
            if options.HasExtension(key):
                self.exported_extensions[key.full_name] = options.Extensions[key]
        self.object_type_options = options.Extensions[object_pb2.object_type]
        self.extend_object_type_options = options.Extensions[object_pb2.extend_object_type]
        self.index_options = options.Extensions[object_pb2.index]
        self.not_exported = options.Extensions[object_pb2.do_not_export]
        if options.HasExtension(object_pb2.module_output_filename):
            self._output_filename = options.Extensions[object_pb2.module_output_filename]
        else:
            self._output_filename = posixpath.basename(self.path)

    def _analyze_enums(self, enum_types_by_name, package):
        for proto_enum_descriptor in enum_types_by_name.values():
            self.enums.append(OrmEnum.make(proto_enum_descriptor, package=package, module=self))

    def _analyze_messages(self, message_types_by_name, package):
        for proto_message_descriptor in message_types_by_name.values():
            self.messages.append(OrmMessage.make(proto_message_descriptor, package=package, module=self))


@dataclass
class OrmHistoryTableOptions:
    primary_table_name: str
    index_table_name: str
    commit_time: str
    time_mode: str
    use_uuid_in_key: bool
    no_hash: bool
    table_key_group: str
    optimize_for_ascending_time: bool
    do_not_optimize_event_type_order: bool
    use_deprecated_hashing: bool

    @property
    def cpp_table_name(self) -> str:
        return snake_case_to_upper_camel_case(self.primary_table_name) + "Table"

    @property
    def cpp_index_name(self) -> str:
        return snake_case_to_upper_camel_case(self.index_table_name) + "Table"

    @property
    def use_positive_event_types(self) -> bool:
        return self.do_not_optimize_event_type_order == self.optimize_for_ascending_time

    @property
    def time_column_name(self) -> str:
        return "time" if self.optimize_for_ascending_time else "inverted_time"

    @property
    def primary_table_hash_expression(self) -> str:
        return "farm_hash(object_type, object_id)"

    @property
    def index_table_hash_expression(self) -> str:
        if self.use_deprecated_hashing:
            return "farm_hash(object_type, history_event_type, index_event_type, object_id)"
        else:
            return self.primary_table_hash_expression


# Global options affecting the whole database, e.g.: defaults for each object type; system table
# schemas, etc. Collected from all modules of a package.
@dataclass
class OrmDatabaseOptions:
    no_parents_hash: bool
    meta_attributes_updatable_by_superuser_only: bool
    enable_tombstones: bool
    enable_annotations: bool
    object_with_reference_removal_policy: object_pb2.EObjectWithReferenceRemovalPolicy
    version_compatibility: str
    skip_store_without_changes: bool
    force_zero_key_evaluation: bool
    hash_policy: object_pb2.EHashPolicy
    parent_key_storage_policy: object_pb2.EParentKeyStoragePolicy
    no_legacy_parents_table: bool
    nested_object_field_messages: bool
    meta_type_field_number: int
    parent_field_number_offset: int
    default_enum_storage_type: str
    finalizers_field_number_offset: int
    enable_finalizers: bool
    enable_custom_base_type_handler: bool
    enable_cumulative_data_weight_for_watch_log: bool
    enable_asynchronous_removals: bool
    set_update_object_mode: str
    default_geohash_length: int
    revision_trackers: list[object_pb2.TRevisionTracker] = dataclass_field(default_factory=list)
    history_tables: list[OrmHistoryTableOptions] = dataclass_field(default_factory=list)
    enable_history_snapshot_column: bool = False
    forbid_parent_removal: bool = False
    remove_legacy_meta_account_id: bool = False
    enable_base_schema_acl_inheritance: bool = False
    enable_strict_enum_value_check: bool = True


@dataclass
class OrmPackage:
    target_proto_namespace: str
    database_options: Optional[OrmDatabaseOptions] = None
    modules: list[OrmModule] = dataclass_field(default_factory=list)
    source_package_paths: set[str] = dataclass_field(default_factory=set)
    inplace_package_paths: set[str] = dataclass_field(default_factory=set)
    enums_by_name: dict[str, OrmEnum] = dataclass_field(default_factory=dict)
    messages_by_name: dict[str, OrmMessage] = dataclass_field(default_factory=dict)
    initialized: bool = False

    @dataclass
    class OrmProtoFile:
        proto_file_descriptor: Any
        options: "OrmPackage.OrmProtoFileOptions"
        is_source: bool

        @dataclass
        class OrmProtoFileOptions:
            source_proto_file_name: str
            _options: protobuf_descriptor_pb2.FileOptions

            @classmethod
            def make(cls, proto_file_descriptor: Any):
                return cls(proto_file_descriptor.name, proto_file_descriptor.GetOptions())

            def get_extension(self, name):
                return self._options.Extensions[name]

            def has_extensions(self, name) -> bool:
                try:
                    return self._options.HasExtension(name)
                except KeyError as err:
                    extensions = self._options.Extensions[name]
                    try:
                        iter(extensions)
                        return len(extensions) > 0
                    except TypeError:
                        raise err

        @classmethod
        def make(cls, proto_file_descriptor: Any, is_source: bool) -> "OrmPackage.OrmProtoFile":
            return cls(
                proto_file_descriptor,
                OrmPackage.OrmProtoFile.OrmProtoFileOptions.make(proto_file_descriptor),
                is_source,
            )

    @classmethod
    def make(
        cls,
        source_packages: list[Any],
        inplace_packages: list[Any],
        target_proto_namespace: str,
    ):
        result = cls(
            target_proto_namespace=target_proto_namespace,
        )

        proto_files = result._analyze_source_packages(source_packages, inplace_packages)
        result._analyze_database_options([file.options for file in proto_files])
        for file in proto_files:
            result._add_modules(file)
        result._analyze_enums()
        result._analyze_messages()
        return result

    def _analyze_source_packages(self, source_packages, inplace_packages) -> "list[OrmPackage.OrmProtoFile]":
        result: list[OrmPackage.OrmProtoFile] = []

        def parse_packages(packages: Any, is_source: bool):
            for package in packages:
                for _, module_name, _ in pkgutil.iter_modules(package.__path__):
                    py_module_name = f"{package.__package__}.{module_name}"
                    py_module = importlib.import_module(py_module_name)
                    proto_file_descriptor = getattr(py_module, "DESCRIPTOR", None)
                    if proto_file_descriptor:
                        result.append(OrmPackage.OrmProtoFile.make(proto_file_descriptor, is_source))

        parse_packages(source_packages, is_source=True)
        parse_packages(inplace_packages, is_source=False)
        return result

    def get_index_options(self) -> list[object_pb2.TIndexOption]:
        return [options for module in self.modules for options in module.index_options]

    def get_index_options_by_name(self) -> dict[str, object_pb2.TIndexOption]:
        index_options_by_name: dict[str, object_pb2.TIndexOption] = dict()
        for module in self.modules:
            for options in module.index_options:
                index_options_by_name[options.snake_case_name] = options

        return index_options_by_name

    def _analyze_database_options(self, modules_options):
        def infer_option(name, default):
            extension = getattr(object_pb2, name)
            result_value = None
            result_proto_file_name = None
            for module_options in modules_options:
                if module_options.has_extensions(extension):
                    current_value = module_options.get_extension(extension)
                    if result_value is None:
                        result_value = current_value
                        result_proto_file_name = module_options.source_proto_file_name
                    elif result_value != current_value:
                        assert False, (
                            f"Found different values of DB option {name}: {result_value} in {result_proto_file_name} "
                            f"and {current_value} in {module_options.source_proto_file_name}"
                        )
            return result_value if result_value is not None else default

        options_input = [
            ("no_parents_hash", False),
            ("meta_attributes_updatable_by_superuser_only", False),
            ("enable_tombstones", False),
            ("enable_annotations", False),
            (
                "object_with_reference_removal_policy",
                object_pb2.EObjectWithReferenceRemovalPolicy.OWRRP_REMOVAL_ALLOWED,
            ),
            (
                "version_compatibility",
                object_pb2.EVersionCompatibility.VC_LOWER_OR_EQUAL_THAN_DB_VERSION,
            ),
            ("skip_store_without_changes", False),
            ("force_zero_key_evaluation", False),
            ("hash_policy", object_pb2.EHashPolicy.HP_NO_HASH_COLUMN),
            (
                "parent_key_storage_policy",
                object_pb2.EParentKeyStoragePolicy.PKSP_PREFIX_WITH_SEPARATE_PARENTS_TABLE,
            ),
            ("no_legacy_parents_table", False),
            ("nested_object_field_messages", False),
            ("meta_type_field_number", 8),
            ("parent_field_number_offset", 100),
            ("default_enum_storage_type", "string"),
            ("finalizers_field_number_offset", 110),
            ("enable_finalizers", False),
            ("enable_custom_base_type_handler", False),
            ("enable_cumulative_data_weight_for_watch_log", False),
            ("enable_asynchronous_removals", True),
            ("revision_trackers", []),
            (
                "set_update_object_mode",
                object_pb2.ESetUpdateObjectMode.SUOM_OVERWRITE,
            ),
            ("enable_history_snapshot_column", False),
            ("forbid_parent_removal", False),
            ("remove_legacy_meta_account_id", False),
            ("enable_base_schema_acl_inheritance", False),
            ("enable_strict_enum_value_check", True),
            ("default_geohash_length", 8),
        ]

        options = dict()
        for name, default in options_input:
            assert name not in options, f"Duplicate DB option {name}"
            options[name] = infer_option(name, default)

        options["version_compatibility"] = object_pb2.EVersionCompatibility.Name(options["version_compatibility"])
        options["set_update_object_mode"] = object_pb2.ESetUpdateObjectMode.Name(options["set_update_object_mode"])

        history_options_input = [
            ("primary_table_name", "history_events"),
            ("index_table_name", "history_index"),
            ("commit_time", "HCT_TRANSACTION_START"),
            ("time_mode", "HTM_LOGICAL"),
            ("use_uuid_in_key", False),
            ("no_hash", False),
            ("table_key_group", "default"),
            ("optimize_for_ascending_time", False),
            ("do_not_optimize_event_type_order", False),
            ("use_deprecated_hashing", False),
        ]

        options["history_tables"] = []
        for history_table in infer_option("history_tables", []):
            history_options = dict()
            for name, default in history_options_input:
                if history_table.HasField(name):
                    history_options[name] = getattr(history_table, name)
                else:
                    history_options[name] = default
            if not isinstance(history_options["commit_time"], str):
                history_options["commit_time"] = object_pb2.EHistoryCommitTime.Name(history_options["commit_time"])

            if not isinstance(history_options["time_mode"], str):
                history_options["time_mode"] = object_pb2.EHistoryTimeMode.Name(history_options["time_mode"])
            options["history_tables"].append(OrmHistoryTableOptions(**history_options))

        if len(options["history_tables"]) == 0:
            options["history_tables"] = [OrmHistoryTableOptions(**dict(history_options_input))]

        self.database_options = OrmDatabaseOptions(**options)

    def get_enum_by_short_name(self, enum_name):
        return next(filter(lambda enum: enum.name == enum_name, self.enums_by_name.values()))

    def get_message_by_short_name(self, message_name):
        return next(filter(lambda message: message.name == message_name, self.messages))

    @property
    def messages(self):
        return self.messages_by_name.values()

    def get_message(self, message_name: str) -> Optional[OrmMessage]:
        return self.messages_by_name.get(message_name)

    def compose_object_message(self, object_type_option, field, context):
        standard_message_name = (
            f"{self.target_proto_namespace}.T{object_type_option.camel_case_name}{field.camel_case_name}"
        )

        custom_name_option_name = CUSTOM_NAME_OPTIONS.get(field.camel_case_name)
        message_name = getattr(object_type_option, custom_name_option_name, "") or standard_message_name

        result = self.get_message(message_name)

        if not result:
            # TODO: get rid of inplace messages in base ORM data model.
            orm_message_name = f"NYT.NOrm.NDataModel.T{object_type_option.camel_case_name}{field.camel_case_name}"
            result = self.get_message(orm_message_name)
            if result:
                message_name = orm_message_name

        if not result:
            proto_namespace, name = message_name.rsplit(".", 1)
            result = OrmMessage(name=name, proto_namespace=proto_namespace, is_mixin=True)
            self.capture_message(result)

        if message_name != standard_message_name:
            result.original_full_names.add(standard_message_name)
        return self._finish_composing_object_message(object_type_option, result, field, context)

    def compose_object_nested_message(
        self, object_type_option: Any, root: OrmMessage, field: "OrmField", context: dict[str, Any]
    ) -> OrmMessage:
        name = f"T{field.camel_case_name}"
        result = root.nested_messages_by_name.get(name)
        if not result:
            result = OrmMessage(
                name=name,
                proto_namespace=self.target_proto_namespace,
                containing_message=root,
                is_mixin=True,
            )
            self.capture_message(result)
        result = self._finish_composing_object_message(object_type_option, result, field, context)
        root.nested_messages_by_name[name] = result
        result.containing_message = root
        # Need to fix fields proto_value_type, see usage of `TOrmMessage._update_field_types`.
        field.proto_value_type = result.name
        return result

    @staticmethod
    def _should_skip_merging(object_type_option: Any, result: OrmMessage):
        if not result.ignore_builtin or not object_type_option:
            return False
        return hasattr(object_type_option, "builtin_object") and object_type_option.builtin_object

    def _finish_composing_object_message(
        self,
        object_type_option: Any,
        result: OrmMessage,
        field: "OrmField",
        context: dict[str, Any],
    ) -> OrmMessage:
        generic_message_name = f"{self.target_proto_namespace}.TGenericObject{field.camel_case_name}"
        generic_result = self.get_message(generic_message_name)
        result.cpp_object_field_name = field.cpp_camel_case_name
        if self._should_skip_merging(object_type_option, generic_result):
            return result
        result = self.capture_message(generic_result, into=result)
        if not result.in_place and result.output_to_default:
            result.consumed = True

        field.value_message = result
        return result

    def initialize(self, context: OrmContext):
        assert not self.initialized
        self.initialized = True

        for message in self.messages:
            for name in message.original_full_names:
                assert name not in context.original_type_renames, f"{name} seen twice"
                context.original_type_renames[name] = message.full_name

        for enum in self.enums_by_name.values():
            for name in enum.original_full_names:
                assert name not in context.original_type_renames, f"{name} seen twice"
                context.original_type_renames[name] = enum.full_name

        self.get_message_by_short_name("TAccessControlEntry").initialize(context)

    def get_source_modules(self) -> dict[str, OrmModule]:
        return {module.filename: module for module in self.modules if module.is_source}

    def get_sourced_enums(self):
        result = {}
        for enum in self.enums_by_name.values():
            if enum.is_nested:
                continue
            output = result.setdefault(enum.output_filename, [])
            output.append(enum)
        return result

    def get_sourced_messages(self):
        result = {}
        for message in self.messages:
            if message.is_nested or message.consumed or message.is_generic:
                continue
            if message.filename and posixpath.dirname(message.filename) not in self.source_package_paths:
                continue
            output = result.setdefault(message.output_filename, [])
            output.append(message)
        return result

    def get_public_dependencies(self) -> set[str]:
        return set(module.path for module in self.modules if module.is_public)

    def _add_modules(self, proto_file: "OrmPackage.OrmProtoFile"):
        module = OrmModule.make(proto_file.proto_file_descriptor, proto_file.is_source, self)
        self.modules.append(module)
        package_path = module.path.rsplit("/", 1)[0]
        if proto_file.is_source:
            self.source_package_paths.add(package_path)
            assert package_path not in self.inplace_package_paths, f"{package_path} is inplace package"
        else:
            self.inplace_package_paths.add(package_path)
            assert package_path not in self.source_package_paths, f"{package_path} is source package"

    def _analyze_enums(self):
        for module in self.modules:
            if (
                not module.is_source
                and module.original_proto_namespace != self.target_proto_namespace
                and module.original_proto_namespace != "NYT.NOrm.NDataModel"
            ):
                continue
            for enum in module.enums:
                self.capture_enum(enum)

    def capture_enum(self, enum: "OrmEnum", into: "OrmEnum" = None) -> "OrmEnum":
        if into:
            name = into.full_name
        else:
            name = enum.full_name

        existing_enum = self.enums_by_name.get(name)
        assert not into or into is existing_enum
        if existing_enum and enum is not existing_enum:
            enum = OrmEnum.merge(existing_enum, enum)
        self.enums_by_name[name] = enum
        if enum.containing_message:
            enum.containing_message.nested_enums_by_name[enum.name] = enum

        return enum

    def _analyze_finalizers(self):
        if not self.database_options.enable_finalizers:
            return

        finalizers_mixin = self.get_message(f"{self.target_proto_namespace}.TGenericObjectFinalizer")
        for i, field_number in enumerate(sorted(finalizers_mixin.fields_by_number)):
            field = finalizers_mixin.fields_by_number[field_number]
            new_field_number = self.database_options.finalizers_field_number_offset + i
            finalizers_mixin.renumber_field(field.snake_case_name, field_number, new_field_number)

    def _analyze_messages(self):
        for module in self.modules:
            if (
                not module.is_source
                and module.original_proto_namespace != self.target_proto_namespace
                and module.original_proto_namespace != "NYT.NOrm.NDataModel"
            ):
                continue
            for message in module.messages:
                self.capture_message(message)

        self._analyze_finalizers()

    def capture_message(self, message: "OrmMessage", into: Optional["OrmMessage"] = None) -> "OrmMessage":
        if into:
            name = into.full_name
        else:
            name = message.full_name

        existing_message = self.messages_by_name.get(name)
        assert not into or into is existing_message
        if existing_message and message is not existing_message:
            message = OrmMessage.merge(existing_message, message)
        self.messages_by_name[name] = message

        for nested_enum in message.nested_enums_by_name.copy().values():
            existing_enum = self.enums_by_name.get(nested_enum.full_name)
            if not existing_enum:
                self.capture_enum(nested_enum)
            else:
                assert existing_enum is nested_enum
        for nested_message in message.nested_messages_by_name.copy().values():
            existing_message = self.messages_by_name.get(nested_message.full_name)
            if not existing_message:
                self.capture_message(nested_message)
            else:
                assert existing_message is nested_message
        if message.containing_message:
            message.containing_message.nested_messages_by_name[message.name] = message

        return message
