from yt.yt.orm.codegen.model import compat, filters
from yt.yt.orm.codegen.model.loader import (
    collect_imports,
    get_proto_module_descriptor,
    convert_source_path,
)
from yt.yt.orm.codegen.model.message import OrmEnum

from library.python import resource

from dataclasses import fields
import contextlib
from google.protobuf import descriptor
from typing import Dict

import jinja2
import pathlib


def _load_template(name):
    content = resource.find(name)
    assert content, "unknown template %s" % name
    return content.decode("utf-8")


def _get_template(name):
    content = _load_template(name)

    env = jinja2.Environment(
        keep_trailing_newline=True,
        lstrip_blocks=True,
        undefined=jinja2.StrictUndefined,
        loader=jinja2.FunctionLoader(_load_template),
    )
    filters.setup(env)

    return env.template_class.from_code(env, env.compile(content, filename=name), env.globals, None)


def _get_proto_module_error_values(module_descriptor: descriptor.FileDescriptor) -> OrmEnum:
    enums = module_descriptor.enum_types_by_name
    assert len(enums) == 1, f"Expected one enum defined in {module_descriptor}"
    proto_enum_descriptor = enums.values()[0]
    return OrmEnum.make(proto_enum_descriptor)


def _get_error_values(aux_parameters: Dict[str, str]) -> OrmEnum:
    error_proto_file = aux_parameters["error_proto_file"]

    errors = _get_proto_module_error_values(get_proto_module_descriptor(package=None, proto_file=error_proto_file))

    error_extra_proto_file = aux_parameters.get("error_extra_proto_file")
    if error_extra_proto_file:
        extra_errors = _get_proto_module_error_values(
            get_proto_module_descriptor(package=None, proto_file=error_extra_proto_file)
        )
        errors = OrmEnum.merge(errors, extra_errors)

    return errors


def _limit_to_shard(env, key, shard_count, shard):
    if shard_count == 1:
        return

    items = env[key]
    shard_size, remainder = divmod(len(items), shard_count)
    env[key] = items[
        shard_size * shard + min(shard, remainder) :
        shard_size * (shard + 1) + min(shard + 1, remainder)
    ]


def check_no_intersection(a, b, message):
    intersection = a & b
    assert not intersection, f"{message} intersect: {intersection}"


def render_template(template_name, aux_parameters, model, output, shard_count=1, shard=0, **options):
    template = _get_template(template_name)

    model_fields = {field.name: getattr(model, field.name) for field in fields(model)}
    check_no_intersection(aux_parameters.keys(), model_fields.keys(), "aux_parameters and model fields")
    check_no_intersection(aux_parameters.keys(), options.keys(), "aux_parameters and options")
    check_no_intersection(options.keys(), model_fields.keys(), "options and model fields")

    render_env = model_fields
    render_env |= aux_parameters
    render_env |= {"shard_count": shard_count, "shard": shard}
    render_env |= {"yp_compatible": compat.yp(), "proto3": compat.yp()}
    render_env |= options

    _limit_to_shard(render_env, "objects", shard_count, shard)
    output.write(template.render(render_env))


def render_template_simple(template_name, aux_parameters, output):
    """Renders a template that does not depend on schema."""

    template = _get_template(template_name)
    render_env = {"yp_compatible": compat.yp(), "proto3": compat.yp()}
    render_env |= aux_parameters
    output.write(template.render(render_env, errors=_get_error_values(aux_parameters)))


def _get_aux_imports(model, aux_parameters, aux_output_name):
    data_model_proto_path = aux_parameters["data_model_proto_path"]
    source_package_paths = model.source_package_paths
    imports = set()

    for source_enum in model.sourced_enums.get(aux_output_name, []):
        for path in source_enum.source_module_paths():
            imports.add(convert_source_path(path, data_model_proto_path, source_package_paths))

    for message in model.sourced_messages.get(aux_output_name, []):
        imports.update(
            collect_imports(
                message,
                data_model_proto_path,
                model.source_package_paths,
                model.unexported_module_paths,
                aux_imports=True,
            )
        )
        if message.object and message.object.nested_object_field_messages:
            imports.add("{}/{}".format(data_model_proto_path, aux_parameters["access_control_file"]))
            if aux_parameters["object_types_enum_file"]:
                imports.add("{}/{}".format(data_model_proto_path, aux_parameters["object_types_enum_file"]))

    imports.discard("{}/{}".format(data_model_proto_path, aux_output_name))
    imports.add("yt_proto/yt/orm/client/proto/object.proto")
    return imports


def render_template_with_aux(template_name, aux_parameters, model, output_stream, aux_outputs):
    template = _get_template(template_name)
    render_env = {field.name: getattr(model, field.name) for field in fields(model)}
    render_env |= aux_parameters
    render_env["yp_compatible"] = compat.yp()
    render_env["proto3"] = compat.yp()

    data_model_proto_path = aux_parameters["data_model_proto_path"]
    public_imports = set(render_env["public_imports"])
    for aux_output_name in aux_outputs:
        public_imports.add("{}/{}".format(data_model_proto_path, aux_output_name))

    imports = _get_aux_imports(model, aux_parameters, "default")
    for object in model.objects:
        messages = [m for m in object.messages() if m.output_to_default]
        if object.root.output_to_default:
            messages.append(object.root)
        for message in messages:
            imports.update(
                collect_imports(
                    message,
                    data_model_proto_path,
                    model.source_package_paths,
                    model.unexported_module_paths,
                    aux_imports=True,
                )
            )

    imports.add("yt_proto/yt/orm/client/proto/object.proto")
    imports -= public_imports
    render_env["imports"] = sorted(imports)
    render_env["public_imports"] = sorted(public_imports)
    output_stream.write(template.render(render_env))

    aux_template = _get_template("/aux_" + template_name[1:])

    for aux_output_name, aux_output_stream in aux_outputs.items():
        module = model.source_modules[aux_output_name]

        public_imports = set()
        for public_import in module.public_imports:
            public_imports.add(
                convert_source_path(
                    public_import,
                    data_model_proto_path,
                    model.source_package_paths,
                )
            )

        imports = _get_aux_imports(model, aux_parameters, aux_output_name)
        imports.discard("{}/{}".format(data_model_proto_path, aux_output_name))
        imports -= public_imports

        render_env["aux_output_name"] = aux_output_name
        render_env["aux_imports"] = sorted(imports)
        render_env["aux_public_imports"] = sorted(public_imports)
        render_env["proto3"] = module.is_proto3
        render_env["module"] = module

        aux_output_stream.write(aux_template.render(render_env))


def render_file(
    output_dir,
    aux_parameters,
    file_name,
    file_ext,
    model=None,
    shard_count=1,
    shard=0,
    template_name_prefix="",
    output_file_name=None,
    **options,
):
    template_name = f"{template_name_prefix}/{file_name}.{file_ext}.j2"

    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if output_file_name:
        file_name = output_file_name

    if shard_count == 1:
        full_file_name = f"{file_name}.{file_ext}"
    else:
        full_file_name = f"{file_name}_{shard}.{file_ext}"

    with (output_dir / full_file_name).open("w") as fd:
        if model is not None:
            render_template(template_name, aux_parameters, model, fd, shard_count=shard_count, shard=shard, **options)
        else:
            render_template_simple(template_name, aux_parameters, fd)


def render_cpp_module(
    output_dir,
    name,
    aux_parameters,
    model=None,
    shard_count=1,
    template_name_prefix="",
    output_file_name=None,
    **options,
):
    render_file(
        output_dir,
        aux_parameters,
        name,
        file_ext="h",
        model=model,
        template_name_prefix=template_name_prefix,
        output_file_name=output_file_name,
        **options,
    )
    for shard in range(shard_count):
        render_file(
            output_dir,
            aux_parameters,
            name,
            file_ext="cpp",
            model=model,
            shard_count=shard_count,
            shard=shard,
            template_name_prefix=template_name_prefix,
            output_file_name=output_file_name,
            **options,
        )


def render_data_model_proto(aux_parameters, model, output_stream):
    if aux_parameters["access_control_file"] is not None:
        raise ValueError("The option `access_control_file` can be used only with multiproto")

    render_template("/data_model.proto.j2", aux_parameters, model, output_stream)


def render_proto_api(aux_parameters, model, output_dir):
    for file_name in ("object_service", "discovery_service"):
        render_file(output_dir, aux_parameters, file_name, file_ext="proto", model=model)


def render_data_model_multiproto(aux_parameters, model, output_dir, output_file_name, aux_file_names):
    if aux_parameters["access_control_file"] is None:
        raise ValueError("The option `access_control_file` must be provided")

    with contextlib.ExitStack() as stack:
        access_control_file = stack.enter_context((output_dir / aux_parameters["access_control_file"]).open("w"))
        output = stack.enter_context((output_dir / output_file_name).open("w"))
        aux_outputs = {name: stack.enter_context((output_dir / name).open("w")) for name in aux_file_names}

        if aux_parameters["object_types_enum_file"]:
            object_types_file = stack.enter_context((output_dir / aux_parameters["object_types_enum_file"]).open("w"))
            render_template("/aux_object_types.proto.j2", aux_parameters, model, object_types_file)
        render_template("/access_control.proto.j2", aux_parameters, model, access_control_file)
        render_template_with_aux("/data_model.proto.j2", aux_parameters, model, output, aux_outputs)


def render_server_proto(aux_parameters, model, output_dir):
    for file_name in ("etc", "continuation_token"):
        render_file(output_dir, aux_parameters, file_name, file_ext="proto", model=model)


def render_objects_lib(aux_parameters, model, output_dir, shard_count):
    module_names = [
        "config",
        "db_schema",
        "object_detail",
        "object_manager",
        "public",
        "type_handlers",
    ]
    if not aux_parameters["custom_dynamic_config_manager"]:
        module_names.append("dynamic_config_manager")

    for name in module_names:
        render_cpp_module(output_dir, name, aux_parameters, model)
    for name in ("objects", "type_handler_impls"):
        render_cpp_module(output_dir, name, aux_parameters, model, shard_count)


def render_objects_lib_multi_file(aux_parameters, model, output_dir):
    module_names = [
        "config",
        "db_schema",
        "object_detail",
        "object_manager",
        "public",
    ]
    if not aux_parameters["custom_dynamic_config_manager"]:
        module_names.append("dynamic_config_manager")

    for name in module_names:
        render_cpp_module(output_dir, name, aux_parameters, model, use_per_object_type_handler=True)

    render_file(
        output_dir, aux_parameters, "type_handlers", file_ext="h", model=model, output_file_name="type_handlers"
    )
    render_file(output_dir, aux_parameters, "objects_aux", file_ext="h", model=model, output_file_name="objects")

    for object in model.objects:
        render_cpp_module(
            output_dir,
            "object",
            aux_parameters,
            model,
            output_file_name=object.snake_case_name,
            object=object,
        )
        render_cpp_module(
            output_dir,
            "type_handler_impl",
            aux_parameters,
            model,
            output_file_name=f"{object.snake_case_name}_type_handler_impl",
            object=object,
        )


def render_yp_objects_lib(aux_parameters, model, output_dir, shard_count):
    # This whole don't skip cpp business is temporary to keep canonizing YP C++ code
    # during migration.
    aux_parameters["dont_skip_cpp"] = False

    modules_names = [
        "config",
        "db_schema",
        "dynamic_config_manager",
        "object_manager",
        "public",
        "type_handlers",
    ]

    for name in modules_names:
        render_cpp_module(output_dir, name, aux_parameters, model)
    for name in ("objects", "type_handler_impls"):
        render_cpp_module(output_dir, name, aux_parameters, model, shard_count)


def render_client_misc(aux_parameters, model, output_dir):
    modules = ["enums", "index_helpers", "traits"]
    for module_name in modules:
        render_cpp_module(
            output_dir,
            module_name,
            aux_parameters,
            model,
            template_name_prefix="/client/misc",
        )

    render_file(
        output_dir,
        aux_parameters,
        "schema_transitive",
        file_ext="h",
        model=model,
        template_name_prefix="/client/misc",
    )


def render_client_native(aux_parameters, model, output_dir):
    module_names = (
        "object_service_proxy",
        "discovery_service_proxy",
        "public",
        "client",
        "config",
        "connection",
    )

    for name in module_names:
        render_cpp_module(
            output_dir,
            name,
            aux_parameters,
            model,
            template_name_prefix="/client/native",
        )

    render_file(
        output_dir,
        aux_parameters,
        "private",
        file_ext="h",
        model=model,
        template_name_prefix="/client/native",
    )


def render_client_objects(aux_parameters, model, output_dir):
    for name in ("acl", "init", "public", "tags", "type"):
        render_cpp_module(output_dir, name, aux_parameters, model, template_name_prefix="/client/objects")


def render_db_versions(aux_parameters, model, output_dir):
    output_file_name = f'yt_schema_v{aux_parameters["db_version"]}'

    render_file(
        output_dir,
        aux_parameters,
        "yt_schema",
        file_ext="py",
        model=model,
        output_file_name=output_file_name,
    )
    render_file(
        output_dir,
        aux_parameters,
        "db_versions_init",
        file_ext="py",
        model=model,
        output_file_name="__init__",
    )
    render_file(
        output_dir,
        aux_parameters,
        "db_versions_ya",
        file_ext="make",
        model=model,
        output_file_name="ya",
    )


def render_yt_schema(aux_parameters, model, output_stream):
    render_template("/yt_schema.py.j2", aux_parameters, model, output_stream)


def render_program_lib(aux_parameters, model, output_dir):
    render_cpp_module(output_dir, "program", aux_parameters, model)


def render_error_proto(aux_parameters, output_stream):
    render_template_simple("/error.proto.j2", aux_parameters, output_stream)


def render_error_cpp_enum(aux_parameters, output_stream):
    render_template_simple("/error.h.j2", aux_parameters, output_stream)


def render_main_cpp(aux_parameters, output_stream):
    render_template_simple("/main.cpp.j2", aux_parameters, output_stream)


def render_snapshot(aux_parameters, model, output_dir):
    render_file(
        output_dir,
        aux_parameters,
        "public",
        file_ext="h",
        model=model,
        template_name_prefix="/snapshot",
    )
    render_file(
        output_dir,
        aux_parameters,
        "objects-inl",
        file_ext="h",
        model=model,
        template_name_prefix="/snapshot",
    )

    render_cpp_module(output_dir, "objects", aux_parameters, model, template_name_prefix="/snapshot")
    render_cpp_module(output_dir, "snapshot", aux_parameters, model, template_name_prefix="/snapshot")
