from . import compat, filters
from .common import OrmContext
from .descriptions import load_proto_descriptions
from .enum import OrmEnum
from .field import OrmField
from .index import OrmIndex
from .model import OrmModel
from .object import FieldPathSetter, OrmObject, TOP_LEVEL_FIELDS
from .package import OrmPackage
from .reference import (
    OrmReferenceDeprecated,
    OrmReferenceType,
    OrmReferencesTable,
    OrmReferencesTableCardinality,
    OrmTransitiveReference,
    OrmTransitiveReferenceLink,
)
from .snapshot import enrich_snapshot_config

from google.protobuf import descriptor
from yt_proto.yt.orm.client.proto import object_pb2
from yt.yt.orm.library.snapshot.codegen.config import SnapshotConfig
from yt.yt.orm.library.snapshot.codegen.model import Snapshot, init_snapshots

from dataclasses import dataclass
from collections import defaultdict
import graphlib
import importlib
import posixpath
from typing import Any, Iterable, Optional


@dataclass
class ObjectSource:
    module: "OrmModule"
    object_type_option: object_pb2.TObjectTypeOption


def _merge_object_type_option(object_source, object_source_extension):
    option = object_source.object_type_option
    extension = object_source_extension.object_type_option
    name = option.camel_case_name

    assert (
        extension.camel_case_name == option.camel_case_name
    ), f"Set the same `camel_case_name` when extending the object_type {name}"
    assert (
        extension.snake_case_name == option.snake_case_name
    ), f"Set the same `snake_case_name` when extending the object_type {name}"
    assert (
        len(extension.primary_key) == 0 and not extension.parent
    ), f"Can not override options like `primary_key` and `parent` when extending the object_type {name}"

    option.MergeFrom(extension)

    if object_source_extension.module.is_source and not object_source.module.is_source:
        object_source.module = object_source_extension.module


def _get_object_sources(package_desc: OrmPackage) -> list[object_pb2.TObjectTypeOption]:
    object_sources = [
        ObjectSource(module, option) for module in package_desc.modules for option in module.object_type_options
    ]

    type_value_unique = set()
    for object_source in object_sources:
        option = object_source.object_type_option
        assert option.type_value not in type_value_unique, "Duplicate object_type with type_value {}, name {}".format(
            option.type_value, option.camel_case_name
        )
        type_value_unique.add(option.type_value)

    extend_object_sources = {}
    for module in package_desc.modules:
        for option in module.extend_object_type_options:
            assert (
                option.type_value not in extend_object_sources
            ), "Duplicate extend_object_type with type_value {}, name {}".format(
                option.type_value, option.camel_case_name
            )
            extend_object_sources[option.type_value] = ObjectSource(module, option)

    for object_source in object_sources:
        extension = extend_object_sources.get(object_source.object_type_option.type_value, None)
        if extension:
            _merge_object_type_option(object_source, extension)
            del extend_object_sources[object_source.object_type_option.type_value]

    assert not extend_object_sources, "Some extend_object_type options didn't have matching object_type: {}".format(
        [
            {type_value: source.object_type_option.camel_case_name}
            for type_value, source in extend_object_sources.items()
        ]
    )

    return object_sources


def _get_objects(package_desc: OrmPackage, context: dict[str, Any]) -> list[OrmObject]:
    objects: list[OrmObject] = []

    for object_source in _get_object_sources(package_desc):
        object_type_option = object_source.object_type_option
        root = package_desc.compose_object_message(object_type_option, OrmField("", ""), context)
        for name in TOP_LEVEL_FIELDS:
            field = root.fields_by_name[name]
            assert field, f"Object {object_type_option.camel_case_name} is missing {name} field"
            if object_type_option.nested_object_field_messages:
                package_desc.compose_object_nested_message(object_type_option, root, field, context)
            else:
                package_desc.compose_object_message(object_type_option, field, context)

        object = OrmObject.make(
            object_type_option,
            root=root,
            package_desc=package_desc,
            owning_module=object_source.module,
        )

        objects.append(object)

    return objects


def _get_objects_by_source(objects: list[OrmObject]):
    result: dict[str, list[OrmObject]] = {}
    for object in objects:
        if not object.owning_module.is_source:
            continue
        output = result.setdefault(object.owning_module.filename, [])
        output.append(object)
    return result


def _resolve_children(
    objects: list[OrmObject],
    object_by_camel_case_name: dict[str, OrmObject],
):
    parents: dict[str, str] = {}
    children: dict[str, list[str]] = {}

    for object in objects:
        parents[object.camel_case_name] = object.object_type_option.parent
        children.setdefault(object.object_type_option.parent, []).append(object.camel_case_name)

    graphlib.TopologicalSorter(children).prepare()  # Check for cycles.

    for object in objects:
        object_parent = parents.get(object.camel_case_name)
        if object_parent:
            object.parent = object_by_camel_case_name[object_parent]
        object_children = sorted(children.get(object.camel_case_name, []))
        if object_children:
            object.children = [object_by_camel_case_name[child] for child in object_children]


def _get_indexes(package_desc: OrmPackage, object_by_snake_case_name: dict[str, OrmObject]) -> list[OrmIndex]:
    indexes: list[OrmIndex] = []

    index_options = package_desc.get_index_options()
    for index_option in index_options:
        assert (
            index_option.object_type_snake_case_name in object_by_snake_case_name
        ), "Object type {} does not exist".format(index_option.object_type_snake_case_name)
        object = object_by_snake_case_name[index_option.object_type_snake_case_name]
        index_desc = OrmIndex.make(index_option, object)
        indexes.append(index_desc)
        object.indexes.append(index_desc)

    for object in object_by_snake_case_name.values():
        object.indexes.sort(key=lambda index: index.camel_case_name)

    indexes.sort(key=lambda index: index.camel_case_name)

    return indexes


def _resolve_indexes_for_increment(objects: list[OrmObject]):
    for object in objects:
        for message in object.messages():
            for field in message.fields:
                if not field.index_for_increment:
                    continue

                if field.index_for_increment == "primary_key":
                    assert object.primary_key == [field], (
                        "The index_for_increment option value of primary_key may be used "
                        "only when the field is the singular primary key field of the object, "
                        "but got field '{}', primary key '{}' for object {}".format(
                            field.snake_case_name, object.primary_key[0].snake_case_name, object.snake_case_name
                        )
                    )
                    field.index_for_increment = None
                    continue

                for index_desc in object.indexes:
                    if index_desc.snake_case_name != field.index_for_increment:
                        continue
                    assert (
                        len(index_desc.index_attributes) == 1 and index_desc.index_attributes[0].field == field
                    ), "Index for increment {} must track only the incremented field {}".format(
                        field.index_for_increment, field.snake_case_name
                    )
                    assert (
                        index_desc.is_unique and not index_desc.is_repeated
                    ), "Index for increment {} must be singular and unique".format(field.index_for_increment)


def _prepare_references(
    objects: list[OrmObject],
    object_by_camel_case_name: dict[str, OrmObject],
) -> list[OrmReferencesTable]:
    references_tables: list[OrmReferencesTable] = []

    for object in objects:
        reference_deprecated_attributes = object.collect_reference_deprecated_attributes()
        for attribute in reference_deprecated_attributes:
            field = attribute.field
            foreign_object = object_by_camel_case_name[field.foreign_object_type]
            assert len(foreign_object.primary_key) == 1, "Composite foreign keys not yet supported"
            references_table = OrmReferencesTable(
                source=object,
                target=foreign_object,
                indexed=bool(field.index_over_reference_table),
                source_cardinality=(
                    OrmReferencesTableCardinality.MANY if field.is_repeated else OrmReferencesTableCardinality.ONE
                ),
                suffix_snake_case=field.references_table_suffix,
                target_name_snake_case=field.reference_name,
            )
            field.reference_deprecated = OrmReferenceDeprecated(
                type=(OrmReferenceType.MANY_TO_MANY_INLINE if field.is_repeated else OrmReferenceType.MANY_TO_ONE),
                table=references_table,
                source_object=object,
                forbid_non_empty_removal=field.forbid_removal_with_non_empty_references,
                nullable=field.nullable,
            )
            if not field.no_foreign_view:
                field.parent._add_field(OrmField.make_foreign_view_field_deprecated(field.parent, field))

            foreign_reference = OrmReferenceDeprecated(
                type=(OrmReferenceType.MANY_TO_MANY_TABULAR if field.is_repeated else OrmReferenceType.ONE_TO_MANY),
                table=references_table,
                source_object=object,
                source_attribute=attribute,
                source_field=field,
                forbid_non_empty_removal=(field.forbid_target_object_removal_with_non_empty_references),
            )
            foreign_object.status.add_reference_field_deprecated(foreign_reference)
            references_tables.append(references_table)

            if (
                field.index_over_reference_table
                and field.index_over_reference_table.index_attributes[0].full_path == attribute.full_path
            ):
                index = field.index_over_reference_table
                assert 1 == len(index.index_attributes), "Index {} must contain only one field, but was {}".format(
                    index.snake_case_name, len(index.index_attributes)
                )
                index_field = index.index_attributes[0].field
                assert index_field == field, "Index {} must contain {} field, but was {}".format(
                    index.snake_case_name,
                    field.snake_case_name,
                    index_field.snake_case_name,
                )
                assert object == index.object, "Index {} must contain {} object, but was {}".format(
                    index.snake_case_name,
                    object.snake_case_name,
                    index.object.snake_case_name,
                )
                index.underlying_table = filters.references_table_camel_case(references_table)
                index.underlying_table_snake_case = filters.references_table_snake_case(references_table)
                # Copy options from index, because references table can be configured only
                # via indexes.
                references_table.hash_expression = index.hash_expression
    return references_tables


def _prepare_children_views(objects: list[OrmObject], object_by_camel_case_name: dict[str, OrmObject]):
    for obj in objects:
        children_views = obj.collect_children_views()
        for attribute in children_views:
            field = attribute.field
            assert field.proto_value_type == "google.protobuf.Any", (
                f"Children view field {attribute.full_path} of "
                "object {obj.snake_case_name} must use google.protobuf.Any as placeholder type"
            )
            target = object_by_camel_case_name.get(field.children_view)
            assert (
                target
            ), f"Unknown object type {field.children_view} in children view {attribute.full_path} of object {obj.snake_case_name}"
            assert (
                target.parent == obj
            ), f"Cannot create children view {attribute.full_path} in object {obj.snake_case_name} because {target.snake_case_name} is not its child"
            field.proto_value_type = "{}.T{}".format(target.root.proto_namespace, field.children_view)
            field.children_view_object = target
            field.source_module_paths.remove("google/protobuf/any.proto")
            if (
                target.root.output_filename
                and target.root.output_filename != field.parent.output_filename
                and not target.root.output_to_default
            ):
                field.source_module_paths.add(target.root.output_filename)


def _resolve_transitive_references(objects: list[OrmObject]):
    for object in objects:
        for field in object.meta.fields:
            if not field.transitive_key:
                continue
            links = []
            current_object: Optional[OrmObject] = object
            while current_object.camel_case_name != field.transitive_key:
                parent_key = [f for f in current_object.all_key_fields if f.is_parent_key_field]
                assert len(parent_key) == 1, "Composite parent keys are not supported for transitive reference"
                links.append(
                    OrmTransitiveReferenceLink(
                        object_from=current_object,
                        object_to=current_object.parent,
                        field=parent_key[0],
                    )
                )
                assert (
                    current_object.parent is not None
                ), "Object of type {} was not found in {} ancestors hierarchy".format(field.transitive_key, object)
                current_object = current_object.parent
            assert (
                field.proto_value_type == links[-1].field.proto_value_type
            ), "{} field {} type {} does not match tranitively referenced field's type {}".format(
                object.snake_case_name,
                field.snake_case_name,
                field.proto_value_type,
                links[-1].proto_value_type,
            )
            field.transitive_reference = OrmTransitiveReference(
                owner=object,
                foreign_object=current_object,
                links=links,
            )


def _get_public_enums(objects: list[OrmObject]) -> list[OrmEnum]:
    public_enums: dict[str, OrmEnum] = dict()
    for object in objects:
        for message in object.all_messages():
            for field in message.fields:
                if field.is_column and field.value_enum:
                    enum = field.value_enum
                    if enum.name in public_enums and enum is not public_enums[enum.name]:
                        enum = OrmEnum.merge(public_enums[enum.name], enum)
                    public_enums[enum.name] = enum
    return public_enums


def _get_imports(
    objects: list[OrmObject],
    public_imports: set[str],
    data_model_proto_path: str,
    source_package_paths: set[str],
    unexported_module_paths: set[str],
) -> set[str]:
    imports: set[str] = set()

    for object in objects:
        for message in object.messages():
            imports.update(collect_imports(message, data_model_proto_path, set(), unexported_module_paths))

    imports.add("yt_proto/yt/core/yson/proto/protobuf_interop.proto")
    imports.add("yt_proto/yt/core/ytree/proto/attributes.proto")
    imports.add("yt_proto/yt/orm/client/proto/object.proto")
    imports -= public_imports

    return imports


def _get_server_proto_imports(
    etc_messages: Iterable["OrmMessage"],
    data_model_proto_path: str,
    source_package_paths: set[str],
    unexported_module_paths: set[str],
) -> set[str]:
    server_proto_imports: set[str] = set()
    server_proto_imports.add("yt_proto/yt/core/yson/proto/protobuf_interop.proto")
    server_proto_imports.add("yt_proto/yt/orm/client/proto/object.proto")

    for etc in (x for m in etc_messages for x in m.etcs):
        for f in etc.fields:
            if f.source_module_paths:
                server_proto_imports.update(f.source_module_paths)
            if f.map_key and f.map_key.source_module_paths:
                server_proto_imports.update(f.map_key.source_module_paths)
            server_proto_imports.update(f.option_source_module_paths)

    server_proto_imports = {
        convert_source_path(path, data_model_proto_path, source_package_paths)
        for path in server_proto_imports
        if path not in unexported_module_paths
    }
    return server_proto_imports


def _compute_tags(objects: list[OrmObject], tags_enum: OrmEnum):
    for object in objects:
        object.compute_tags(tags_enum)
        object.root.compute_tags(tags_enum)
        for message in object.messages():
            message.compute_tags(tags_enum)


def _link_attribute_migrations(objects: list[OrmObject]):
    for obj in objects:
        obj.link_attribute_migrations()


def convert_source_path(path: str, data_model_proto_path: str, source_package_paths: set[str]) -> str:
    package_path, filename = posixpath.split(path)
    if not package_path or package_path in source_package_paths:
        return posixpath.join(data_model_proto_path, filename)
    else:
        return path


def collect_imports(
    message: "OrmMessage",
    data_model_proto_path: str,
    source_package_paths: set[str],
    unexported_module_paths: set[str],
    aux_imports: bool = False,
    ingore_self: bool = False,
) -> set[str]:
    if message.in_place and not aux_imports:
        return set()
    imports = set()
    for path in message.source_module_paths:
        if path in unexported_module_paths:
            continue
        imports.add(convert_source_path(path, data_model_proto_path, source_package_paths))

    # Remove own protobuf from imports (do not remove orm common protobufs).
    # It could leak in when we have fields of our immediate nested message type.
    if ingore_self and message.filename is not None:
        own_destination_file = convert_source_path(message.filename, data_model_proto_path, source_package_paths)
        if message.filename != own_destination_file:
            imports.discard(own_destination_file)
    return imports


def get_server_proto_imports(
    etc_messages: Iterable["OrmMessage"],
    data_model_proto_path: str,
    model: OrmModel,
    is_multiproto: bool = False,
) -> set[str]:
    return _get_server_proto_imports(
        etc_messages,
        data_model_proto_path=data_model_proto_path,
        source_package_paths=model.source_package_paths if is_multiproto else set(),
        unexported_module_paths=model.unexported_module_paths,
    )


def get_proto_module_descriptor(package, proto_file) -> descriptor.FileDescriptor:
    module_name = proto_file.replace(".proto", "_pb2")
    if "/" in proto_file:
        module_name = module_name.replace("/", ".")
    else:
        module_name = ".".join((package.__package__, module_name))

    module = importlib.import_module(module_name)
    return module.DESCRIPTOR


def _adjust_object_fields_for_compatibility(package_desc, objects):
    for obj in objects:
        if compat.yp():
            obj.meta.renumber_field("creation_time", 9, 3)
        if not package_desc.database_options.enable_annotations:
            obj.root.try_remove_field("annotations", True)
        if not obj.control:
            continue
        if compat.yp() and len(list(filter(lambda field: field.snake_case_name == "id", obj.primary_key))) == 0:
            obj.meta.try_remove_field("id", False)
        if not obj.indexes:
            obj.control.try_remove_field("touch_index", False)
        if not obj.revision_trackers:
            obj.control.try_remove_field("touch_revision", False)
        if not obj.attribute_migrations:
            obj.control.try_remove_field("migrate_attributes", False)


def _transitive_etc_messages(message):
    original_message = message.original_message or message
    if not original_message.etc_consumed and message.etcs:
        original_message.etc_consumed = True
        if message.is_composite:
            yield message
        for field in message.composite_fields:
            yield from _transitive_etc_messages(field.value_message)


def _etc_messages_by_output(objects) -> dict[str, list["OrmMessage"]]:
    def add_etc_messages(message):
        for m in _transitive_etc_messages(message):
            etc_messages_per_file[message.output_filename or "default"].append(m)

    etc_messages_per_file = defaultdict(list)
    for obj in objects:
        etc_messages_per_file[obj.meta.output_filename or "default"].append(obj.meta)
        obj.meta.etc_consumed = True
        add_etc_messages(obj.meta)
        add_etc_messages(obj.spec)
        add_etc_messages(obj.status)
    return etc_messages_per_file


def _init_snapshots(context: OrmContext) -> dict[str, Snapshot]:
    configs: dict[str, SnapshotConfig] = {}

    for parameters in context.aux_parameters["snapshots_parameters"]:
        original_config = SnapshotConfig.from_dict(parameters)
        enriched_config = enrich_snapshot_config(original_config, context)
        if enriched_config.name.snake_case in configs:
            raise ValueError(f"Duplicating snapshots with snake case name \"{enriched_config.name.snake_case}\"")
        configs[enriched_config.name.snake_case] = enriched_config

    return init_snapshots(configs)


def _initialize(context: OrmContext):
    load_proto_descriptions()

    context.package = OrmPackage.make(
        context.source_packages, context.inplace_packages, context.aux_parameters["data_model_proto_package"]
    )

    context.objects = _get_objects(context.package, context.aux_parameters)
    context.objects.sort(key=lambda obj: obj.camel_case_name)

    for obj in context.objects:
        result = context.object_by_camel_case_name.setdefault(obj.camel_case_name, obj)
        assert result == obj, "Duplicate camel case name {}".format(obj.camel_case_name)

        result = context.object_by_snake_case_name.setdefault(obj.snake_case_name, obj)
        assert result == obj, "Duplicate snake case name {}".format(obj.snake_case_name)

    context.package.initialize(context)


def _link(context: OrmContext):
    for obj in context.objects:
        context.current_object = obj
        obj.root.initialize(context)
        context.current_object = None

    context.tags_enum = context.package.get_enum_by_short_name("ETag")
    _compute_tags(context.objects, context.tags_enum)

    _resolve_children(context.objects, context.object_by_camel_case_name)

    context.indexes = _get_indexes(context.package, context.object_by_snake_case_name)

    _resolve_indexes_for_increment(context.objects)
    context.references_tables = _prepare_references(context.objects, context.object_by_camel_case_name)
    _prepare_children_views(context.objects, context.object_by_camel_case_name)

    _link_attribute_migrations(context.objects)

    # TODO: Move yp specific logic to client plugin.
    _adjust_object_fields_for_compatibility(context.package, context.objects)

    for obj in context.objects:
        context.current_object = obj
        obj.instantiate()

    for obj in context.objects:
        context.current_object = obj
        obj.root.link(context)

    context.current_object = None

    _resolve_transitive_references(context.objects)

    for obj in context.objects:
        FieldPathSetter(obj).visit()

    context.snapshots = _init_snapshots(context)


def _finalize(context: OrmContext) -> OrmModel:
    for obj in context.objects:
        context.current_object = obj
        obj.finalize(context)
        context.current_object = None

    data_model_proto_path = context.aux_parameters["data_model_proto_path"]
    public_imports = context.package.get_public_dependencies()

    public_enums = sorted(_get_public_enums(context.objects).values(), key=lambda v: v.name)

    sourced_messages = context.package.get_sourced_messages()

    unexported_module_paths = {
        module.path for module in context.package.modules if module.is_source and module.not_exported
    }

    imports = _get_imports(
        context.objects,
        public_imports,
        data_model_proto_path,
        context.package.source_package_paths,
        unexported_module_paths,
    )

    etc_messages_by_output = _etc_messages_by_output(context.objects)

    return OrmModel(
        source_modules=context.package.get_source_modules(),
        source_package_paths=context.package.source_package_paths,
        imports=sorted(imports),
        public_imports=sorted(public_imports),
        unexported_module_paths=unexported_module_paths,
        objects=context.objects,
        public_enums=public_enums,
        references_tables=context.references_tables,
        indexes=context.indexes,
        database_options=context.package.database_options,
        objects_by_source=_get_objects_by_source(context.objects),
        sourced_enums=context.package.get_sourced_enums(),
        sourced_messages=sourced_messages,
        etc_messages_by_output=etc_messages_by_output,
        acl_actions_enum=context.package.get_enum_by_short_name("EAccessControlAction"),
        acl_permissions_enum=context.package.get_enum_by_short_name("EAccessControlPermission"),
        acl_entry_message=context.package.get_message_by_short_name("TAccessControlEntry"),
        tags_enum=context.tags_enum,
        snapshots=context.snapshots,
    )


def _evaluate_strict_enum_value_checks(orm_model: OrmModel):
    for obj in orm_model.objects:
        obj.evaluate_strict_enum_value_checks(orm_model.database_options.enable_strict_enum_value_check)


def load_model(parameters: "CodegenParameters") -> OrmModel:
    """Returns a graph describing the ORM schema composed from the package and context."""

    parameters.prepare()
    aux_parameters = parameters.aux_parameters.to_dict()
    client_plugin = parameters.client_plugin

    context = OrmContext(
        aux_parameters=aux_parameters,
        source_packages=parameters.source_packages,
        inplace_packages=parameters.inplace_packages,
    )

    _initialize(context)

    if client_plugin:
        client_plugin.post_initialize(context)

    _link(context)

    if client_plugin:
        client_plugin.post_link(context)

    orm_model = _finalize(context)

    if client_plugin:
        client_plugin.post_finalize(orm_model)

    _evaluate_strict_enum_value_checks(orm_model)

    return orm_model
