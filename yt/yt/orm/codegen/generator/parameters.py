from yt.yt.orm.codegen.model.client_plugin import OrmClientPlugin
from yt.yt.orm.codegen.model.snapshot import (
    OrmSnapshotConfig,
    OrmSnapshotFieldConfig,
    OrmSnapshotObjectConfig,
    OrmSnapshotManyToOneReferenceConfig,
    OrmSnapshotOneToManyReferenceConfig,
)

from dataclasses import asdict, dataclass, field
from typing import Optional


class WeaklyImmutable(object):
    _frozen = False

    def __post_init__(self):
        self._frozen = True

    def __setattr__(self, name, value):
        if self._frozen and not hasattr(self, name):
            raise AttributeError(f"Unknown parameter: {name}")

        super(WeaklyImmutable, self).__setattr__(name, value)


@dataclass
class SnapshotFieldParameters(OrmSnapshotFieldConfig, WeaklyImmutable):
    pass


@dataclass
class SnapshotManyToOneReferenceParameters(OrmSnapshotManyToOneReferenceConfig, WeaklyImmutable):
    pass


@dataclass
class SnapshotOneToManyReferenceParameters(OrmSnapshotOneToManyReferenceConfig, WeaklyImmutable):
    pass


@dataclass
class SnapshotObjectParameters(OrmSnapshotObjectConfig, WeaklyImmutable):
    pass


@dataclass
class SnapshotParameters(OrmSnapshotConfig, WeaklyImmutable):
    pass


@dataclass
class AuxiliaryParameters(WeaklyImmutable):
    access_control_file: Optional[str] = None
    object_types_enum_file: Optional[str] = None
    client_discovery_java_package: Optional[str] = None
    client_go_api_package: Optional[str] = None
    client_go_package: Optional[str] = None
    client_java_package: Optional[str] = None
    client_misc_cpp_namespace: Optional[str] = None
    client_misc_lib: Optional[str] = None
    client_native_cpp_namespace: Optional[str] = None
    client_native_cpp_path: Optional[str] = None
    client_objects_cpp_namespace: Optional[str] = None
    client_objects_cpp_path: Optional[str] = None
    client_proto_cpp_namespace: Optional[str] = None
    client_proto_package: Optional[str] = None
    cpp_data_model_proto_namespace: Optional[str] = None
    cpp_objects_namespace: Optional[str] = None
    cpp_objects_path: Optional[str] = None
    cpp_server_custom_base_type_handler_file_name: Optional[str] = None
    cpp_server_custom_base_type_handler_path: Optional[str] = None
    cpp_server_namespace: Optional[str] = None
    cpp_server_path: Optional[str] = None
    cpp_server_plugins_namespace: Optional[str] = None
    cpp_server_plugins_path: Optional[str] = None
    cpp_server_proto_namespace: Optional[str] = None
    data_model_proto_package: Optional[str] = None
    data_model_go_package: Optional[str] = None
    data_model_java_package: Optional[str] = None
    data_model_proto_path: Optional[str] = None
    default_db_name: Optional[str] = None
    default_grpc_port: int = 8090
    error_cpp_namespace: Optional[str] = None
    error_extra_proto_file: Optional[str] = None
    error_go_package: Optional[str] = None
    error_java_package: Optional[str] = None
    error_proto_file: str = "yt_proto/yt/orm/client/proto/error.proto"
    error_proto_package: Optional[str] = None
    generated_files_dir_name: Optional[str] = None
    initial_db_version: Optional[str] = None
    proto_package: Optional[str] = None
    server_go_package: Optional[str] = None
    server_java_package: Optional[str] = None
    server_proto_package: Optional[str] = None
    server_proto_path: Optional[str] = None

    dont_skip_cpp: bool = True
    proto3: bool = False
    custom_dynamic_config_manager: bool = False
    data_model_java_multiple_files: bool = False

    db_version: int = 1

    # It is expected that custom configs are defined in {{ cpp_server_plugins_path }}/config.h
    # in {{ cpp_server_plugins_namespace }} namespace.
    custom_config: bool = False
    custom_object_manager_config: bool = False

    snapshots_parameters: dict[str, SnapshotParameters] = field(default_factory=dict)

    def prepare(self):
        def set_default(attribute, value):
            if getattr(self, attribute) is None:
                setattr(self, attribute, value)

        if self.proto_package is not None:
            set_default("client_proto_package", self.proto_package)
            set_default("server_proto_package", self.proto_package)
            set_default("data_model_proto_package", self.proto_package)

        if self.data_model_proto_package is not None:
            set_default(
                "cpp_data_model_proto_namespace",
                self.data_model_proto_package.replace(".", "::"),
            )

        if self.client_proto_package is not None:
            set_default("client_proto_cpp_namespace", self.client_proto_package.replace(".", "::"))

        if self.client_java_package is not None:
            set_default("client_discovery_java_package", self.client_java_package)
            set_default("data_model_java_package", self.client_java_package)

        if self.client_go_package is not None:
            set_default("data_model_go_package", self.client_go_package)

        if self.server_proto_package is not None:
            set_default("cpp_server_proto_namespace", self.server_proto_package.replace(".", "::"))

        if self.cpp_server_namespace:
            set_default("cpp_objects_namespace", self.cpp_server_namespace)

        if self.cpp_server_path:
            set_default("cpp_objects_path", self.cpp_server_path)

        if self.cpp_server_custom_base_type_handler_file_name is not None:
            set_default(
                "cpp_server_custom_base_type_handler_path",
                f"{self.cpp_server_plugins_path}/{self.cpp_server_custom_base_type_handler_file_name}",
            )

    def to_dict(self):
        return asdict(self)


@dataclass
class CodegenParameters(WeaklyImmutable):
    aux_parameters: AuxiliaryParameters = field(default_factory=AuxiliaryParameters)
    source_packages: list = field(default_factory=list)
    inplace_packages: list = field(default_factory=list)
    client_plugin: Optional[OrmClientPlugin] = None

    def prepare(self):
        self.aux_parameters.prepare()
