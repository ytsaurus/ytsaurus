from yt.yt.orm.codegen.model import compat, filters
from yt.yt.orm.codegen.model.loader import (
    collect_imports,
    convert_source_path,
    get_proto_module_descriptor,
    get_server_proto_imports,
)
from yt.yt.orm.codegen.model.message import OrmEnum
from yt.yt.orm.codegen.model.model import OrmModel
from yt.yt.orm.library.snapshot.codegen.render import render_snapshot

from library.python import resource

from dataclasses import dataclass, field, fields
import contextlib
from functools import lru_cache
from google.protobuf import descriptor
from typing import Any, Dict, Set

import copy
import difflib
import filecmp
import jinja2
import pathlib
import sys
import tempfile


@dataclass
class RenderContext(contextlib.ContextDecorator):
    aux_parameters: Dict[str, Any]
    model: OrmModel | None = None
    output_dir: pathlib.Path | None = None
    original_dir: pathlib.Path | None = None

    _check_diff: bool = False
    _diff_one_side: bool = False
    _diff_ignore_ya_make_and_md: bool = False
    _subdir: pathlib.Path = field(default_factory=pathlib.Path)
    failed_checks: Set[str] = field(default_factory=set)

    _rendered_files: Set[str] = field(default_factory=set)
    _all_rendered_files: Set[str] = field(default_factory=set)

    def __enter__(self):
        self._rendered_files.clear()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._check_diff:
            self._do_check_diff()
        return False

    def in_subdir(self, subdir, delete_old_contents=True, keep_ya_make_and_md=False):
        if subdir in self.aux_parameters:
            subdir = self.aux_parameters[subdir]

        sub_context = copy.copy(self)
        sub_context._diff_one_side = not delete_old_contents
        sub_context._diff_ignore_ya_make_and_md = keep_ya_make_and_md
        sub_context._rendered_files = set()
        sub_context._subdir = pathlib.Path(self._subdir) / subdir if self._subdir else subdir

        if sub_context.original_dir is not None:
            sub_context.original_dir = pathlib.Path(sub_context.original_dir) / subdir

        if sub_context.output_dir is not None:
            sub_context.output_dir = pathlib.Path(sub_context.output_dir) / subdir
            if sub_context.output_dir.exists():
                if delete_old_contents:
                    for fp in sub_context.output_dir.iterdir():
                        if keep_ya_make_and_md and (fp.name.endswith("README.md") or fp.name.endswith("ya.make")):
                            continue
                        if not fp.name.endswith(".j2"):
                            fp.unlink()
            else:
                sub_context.output_dir.mkdir(parents=True)

        return sub_context

    @contextlib.contextmanager
    def check(self, do_check):
        if not do_check:
            yield self
            return

        with tempfile.TemporaryDirectory() as tmpdir:
            self.original_dir = self.output_dir
            self.output_dir = pathlib.Path(tmpdir)
            self._check_diff = True
            yield self

            self._check_diff = False

            if self.failed_checks:
                print("Files out of sync. Need to execute run_static_codegen.sh", file=sys.stderr)
                sys.exit(1)

    def _do_check_diff(self):
        source_dir = self.original_dir
        target_dir = self.output_dir
        assert source_dir is not None
        assert target_dir is not None

        ignore = [".", "..", "ya.make.j2", "ya.make.inc.j2"]
        if self._diff_ignore_ya_make_and_md:
            ignore += ["ya.make", "README.md"]

        self._no_diff = True

        def diff_found():
            if self._no_diff:
                print("In directory:", self._subdir, file=sys.stderr)
            self._no_diff = False

        comparison_result = filecmp.dircmp(source_dir, target_dir, ignore=ignore)
        if comparison_result.diff_files:
            diff_found()
            for name in comparison_result.diff_files:
                with open(source_dir / name, "r") as fd:
                    a = fd.readlines()
                with open(target_dir / name, "r") as fd:
                    b = fd.readlines()
                sys.stderr.writelines(difflib.unified_diff(a, b, fromfile=name, tofile=name))
                sys.stderr.write("\n")
        if comparison_result.left_only and not self._diff_one_side:
            for name in comparison_result.left_only:
                if not (source_dir / name).is_symlink():
                    diff_found()
                    print("  Unexpected file:", name, file=sys.stderr)
        if comparison_result.right_only:
            diff_found()
            for name in comparison_result.right_only:
                print("  Missing file:", name, file=sys.stderr)

        if not self._no_diff:
            self.failed_checks.add(str(source_dir))


def _load_template(name):
    content = resource.find(name)
    assert content, "unknown template %s" % name
    return content.decode("utf-8")


@lru_cache(maxsize=20)
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


@contextlib.contextmanager
def _ya_make_inc(context, source_extension, for_subdir=False):
    yield

    subdir = pathlib.Path(context.output_dir).name
    render_template(
        "/generated_dir_ya.make.inc.j2",
        context,
        "ya.make.inc",
        ya_make_inc_set_variable=f"{subdir.upper()}_{"SUBDIR_" if for_subdir else ""}SRCS",
        ya_make_inc_subdir=f"{subdir}/" if for_subdir else "",
        ya_make_sources=sorted(f for f in context._rendered_files if f.endswith(source_extension)),
    )


def _get_proto_module_error_values(module_descriptor: descriptor.FileDescriptor) -> OrmEnum:
    enums = module_descriptor.enum_types_by_name
    assert len(enums) == 1, f"Expected one enum defined in {module_descriptor}"
    proto_enum_descriptor = enums.values()[0]
    return OrmEnum.make(proto_enum_descriptor)


def _get_error_values(aux_parameters: Dict[str, Any]):
    error_proto_file = aux_parameters["error_proto_file"]

    errors = _get_proto_module_error_values(get_proto_module_descriptor(package=None, proto_file=error_proto_file))

    error_extra_proto_file = aux_parameters.get("error_extra_proto_file")
    if error_extra_proto_file:
        extra_errors = _get_proto_module_error_values(
            get_proto_module_descriptor(package=None, proto_file=error_extra_proto_file)
        )
        errors = OrmEnum.merge(errors, extra_errors)

    aux_parameters["errors"] = errors


def check_no_intersection(a, b, message):
    intersection = a & b
    intersection.discard("proto3")
    assert not intersection, f"{message} intersect: {intersection}"


def render_template(template_name, context, output_file, **options):
    context._rendered_files.add(str(output_file))
    context._all_rendered_files.add(str(context._subdir / output_file))

    if context.output_dir is not None:
        context.output_dir = pathlib.Path(context.output_dir)
        context.output_dir.mkdir(parents=True, exist_ok=True)
        output_file = context.output_dir / output_file
    else:
        output_file = pathlib.Path(output_file)

    with output_file.open("w") as output:
        template = _get_template(template_name)

        render_env = {"yp_compatible": compat.yp()}

        if context.model is not None:
            model_fields = {field.name: getattr(context.model, field.name) for field in fields(context.model)}
            check_no_intersection(context.aux_parameters.keys(), model_fields.keys(), "aux_parameters and model fields")
            check_no_intersection(options.keys(), model_fields.keys(), "options and model fields")
            render_env |= model_fields

        check_no_intersection(context.aux_parameters.keys(), options.keys(), "aux_parameters and options")
        render_env |= context.aux_parameters

        if compat.yp():
            render_env["proto3"] = True
        render_env |= options

        output.write(template.render(render_env))


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
                ingore_self=True,
            )
        )
        if message.object and message.object.nested_object_field_messages:
            imports.add("{}/{}".format(data_model_proto_path, aux_parameters["access_control_file"]))
            if aux_parameters["object_types_enum_file"]:
                imports.add("{}/{}".format(data_model_proto_path, aux_parameters["object_types_enum_file"]))

    imports.discard("{}/{}".format(data_model_proto_path, aux_output_name))
    return imports


def _fix_imports(imports, public_imports):
    # TODO (bulatman): Manually fix imports due a bug in import collecting.
    imports.add("yt_proto/yt/orm/client/proto/object.proto")

    imports.discard("yt_proto/yt/orm/data_model/access_control.proto")
    imports.discard("yt_proto/yt/orm/data_model/generic.proto")

    imports -= public_imports


def render_template_with_aux(template_name, context, output_file, aux_file_names):
    aux_parameters = context.aux_parameters
    data_model_proto_path = aux_parameters["data_model_proto_path"]
    public_imports = set(context.model.public_imports)
    imports = _get_aux_imports(context.model, aux_parameters, "default")

    if aux_parameters["data_model_objects_proto_file"]:
        public_imports = set()
        imports.add("{}/{}".format(data_model_proto_path, aux_parameters["access_control_file"]))
        if aux_parameters["object_types_enum_file"]:
            imports.add("{}/{}".format(data_model_proto_path, aux_parameters["object_types_enum_file"]))
    else:
        for aux_file in aux_file_names:
            public_imports.add("{}/{}".format(data_model_proto_path, aux_file))

    for object in context.model.objects:
        messages = [m for m in object.messages() if m.output_to_default]
        if object.root.output_to_default:
            messages.append(object.root)
        for message in messages:
            imports.update(
                collect_imports(
                    message,
                    data_model_proto_path,
                    context.model.source_package_paths,
                    context.model.unexported_module_paths,
                    aux_imports=True,
                )
            )

    _fix_imports(imports, public_imports)

    patched_context = copy.copy(context)
    patched_context.model = copy.copy(context.model)
    patched_context.model.imports = sorted(imports)
    patched_context.model.public_imports = sorted(public_imports)

    render_template(template_name, patched_context, output_file)

    aux_template_name = "/aux_" + template_name[1:]

    for aux_file in aux_file_names:
        module = context.model.source_modules[aux_file]

        public_imports = set()
        for public_import in module.public_imports:
            public_imports.add(
                convert_source_path(
                    public_import,
                    data_model_proto_path,
                    context.model.source_package_paths,
                )
            )

        imports = _get_aux_imports(context.model, aux_parameters, aux_file)
        _fix_imports(imports, public_imports)

        render_template(
            aux_template_name,
            patched_context,
            aux_file,
            aux_output_name=aux_file,
            aux_imports=sorted(imports),
            aux_public_imports=sorted(public_imports),
            proto3=module.is_proto3,
            module=module,
        )


def render_file(
    context,
    file_name,
    file_ext="",
    template_name_prefix="",
    output_file_name=None,
    **options,
):
    template_name = f"{template_name_prefix}/{file_name}{file_ext}.j2"
    output_file_name = f"{output_file_name or file_name}{file_ext}"

    render_template(template_name, context, output_file_name, **options)


def render_cpp_module(
    context,
    name,
    template_name_prefix="",
    output_file_name=None,
    **options,
):
    render_file(
        context,
        name,
        file_ext=".h",
        template_name_prefix=template_name_prefix,
        output_file_name=output_file_name,
        **options,
    )

    render_file(
        context,
        name,
        file_ext=".cpp",
        template_name_prefix=template_name_prefix,
        output_file_name=output_file_name,
        **options,
    )


def render_data_model_proto(context, output_file):
    if context.aux_parameters["access_control_file"] is not None:
        raise ValueError("The option `access_control_file` can be used only with multiproto")

    render_template("/data_model.proto.j2", context, output_file)


def render_proto_api(context):
    for file_name in ("object_service.proto", "discovery_service.proto"):
        render_file(context, file_name)


def _get_rendered_client_object_protos(context):
    aux_file_names_set = set(
        object.root.output_filename for object in context.model.objects if not object.root.output_to_default
    )
    aux_file_names_set |= set(context.model.sourced_enums) | set(context.model.sourced_messages)

    # TODO: Investigate why sourced_enums can contain None.
    aux_file_names_set.discard(None)

    # TODO: Put YP all objects into their own files in and remove this compat flag.
    aux_file_names_set |= set(context.aux_parameters["force_proto_render"])

    # Do not overwrite files manually rendered earlier.
    aux_file_names_set -= context._rendered_files
    return sorted(aux_file_names_set)


def render_data_model_multiproto(context):
    aux_parameters = context.aux_parameters
    output_file_name = aux_parameters["client_data_model_filename"]
    access_control_file = aux_parameters["access_control_file"]
    if access_control_file is None:
        raise ValueError("The option `access_control_file` must be provided")

    if aux_parameters["object_types_enum_file"]:
        object_types_file = aux_parameters["object_types_enum_file"]
        render_template("/aux_object_types.proto.j2", context, object_types_file)
    if aux_parameters["tags_enum_file"]:
        tags_enum_file = aux_parameters["tags_enum_file"]
        render_template("/aux_tags_enum.proto.j2", context, tags_enum_file)
    render_template("/access_control.proto.j2", context, access_control_file)

    aux_file_names = _get_rendered_client_object_protos(context)

    render_template_with_aux("/data_model.proto.j2", context, output_file_name, aux_file_names)

    object_modules = set()
    if aux_parameters["data_model_objects_proto_file"]:
        data_model_proto_path = aux_parameters["data_model_proto_path"]
        for aux_output_name in aux_file_names:
            object_modules.add("{}/{}".format(data_model_proto_path, aux_output_name))
        object_modules.add("{}/{}".format(data_model_proto_path, output_file_name))
        object_modules.add("{}/{}".format(data_model_proto_path, access_control_file))
        if aux_parameters["object_types_enum_file"]:
            object_modules.add("{}/{}".format(data_model_proto_path, aux_parameters["object_types_enum_file"]))
        if aux_parameters["tags_enum_file"]:
            object_modules.add("{}/{}".format(data_model_proto_path, aux_parameters["tags_enum_file"]))

        render_template(
            "/data_model_objects.proto.j2",
            context,
            aux_parameters["data_model_objects_proto_file"],
            object_modules=sorted(object_modules),
        )


def render_server_proto(context):
    data_model_proto_path = context.aux_parameters["data_model_proto_path"]
    etc_messages = [m for messages in context.model.etc_messages_by_output.values() for m in messages]
    server_proto_imports = get_server_proto_imports(etc_messages, data_model_proto_path, context.model, compat.yp())
    server_proto_imports.add(context.aux_parameters["client_schema_proto"])
    server_proto_imports = sorted(server_proto_imports)
    etc_messages.sort(key=lambda x: x.etc_type_name)

    for file_name in ("etc.proto", "continuation_token.proto"):
        render_file(
            context,
            file_name,
            server_proto_imports=server_proto_imports,
            etc_messages=etc_messages,
        )


def render_server_multi_proto(context):
    render_file(context, "continuation_token.proto")

    data_model_proto_path = context.aux_parameters["data_model_proto_path"]

    for filename, etc_messages in context.model.etc_messages_by_output.items():
        output_file = "etc.proto" if filename == "default" else filename
        server_proto_imports = get_server_proto_imports(etc_messages, data_model_proto_path, context.model, True)
        render_template(
            "/etc.proto.j2",
            context,
            output_file,
            etc_messages=sorted(etc_messages, key=lambda x: x.etc_type_name),
            server_proto_imports=sorted(server_proto_imports),
        )


def render_objects_lib_multi_file(context):
    module_names = [
        "config",
        "db_schema",
        "object_manager",
        "public",
    ]
    if not context.aux_parameters["custom_dynamic_config_manager"]:
        module_names.append("dynamic_config_manager")
    if not context.aux_parameters["custom_object_detail"]:
        module_names.append("object_detail")

    with _ya_make_inc(context, ".cpp", True):
        for name in module_names:
            render_cpp_module(context, name)

        render_file(context, "type_handlers.h")
        render_file(context, "objects_aux.h", output_file_name="objects.h")

        for object in context.model.objects:
            if object.skip_cpp and not context.aux_parameters["dont_skip_cpp"]:
                continue

            render_cpp_module(
                context,
                "object",
                output_file_name=object.snake_case_name,
                object=object,
            )
            render_cpp_module(
                context,
                "type_handler",
                output_file_name=f"{object.snake_case_name}_type_handler",
                object=object,
            )


def render_client_misc(context):
    modules = ["enums", "index_helpers", "traits"]
    for module_name in modules:
        render_cpp_module(context, module_name, template_name_prefix="/client/misc")

    render_file(context, "schema_transitive.h", template_name_prefix="/client/misc")


def render_client_native(context):
    module_names = (
        "object_service_proxy",
        "discovery_service_proxy",
        "public",
        "client",
        "config",
    )

    for name in module_names:
        render_cpp_module(context, name, template_name_prefix="/client/native")


def render_client_objects(context):
    for name in ("acl", "init", "public", "tags", "type"):
        render_cpp_module(context, name, template_name_prefix="/client/objects")


def render_db_versions(context):
    output_file_name = f"yt_schema_v{context.aux_parameters["db_version"]}.py"

    render_file(context, "yt_schema.py", output_file_name=output_file_name)
    render_file(context, "db_versions_init.py", output_file_name="__init__.py")
    render_file(context, "db_versions_ya.make", output_file_name="ya.make")


def render_yt_schema(context, output_path):
    render_template("/yt_schema.py.j2", context, output_path)


def render_program_lib(context):
    render_cpp_module(context, "program")


def render_error_proto(context, output_path):
    _get_error_values(context.aux_parameters)
    render_template("/error.proto.j2", context, output_path)


def render_error_cpp_enum(context, output_path):
    _get_error_values(context.aux_parameters)
    render_template("/error.h.j2", context, output_path)


def render_main_cpp(context, output_path):
    render_template("/main.cpp.j2", context, output_path)


def render_codegen_verify(context):
    if context.aux_parameters["user_codegen_verify_dir"] is not None:
        with context.in_subdir("user_codegen_verify_dir", keep_ya_make_and_md=True) as subdir_context:
            with _ya_make_inc(subdir_context, ""):
                for file in subdir_context._all_rendered_files:
                    subdir_context._rendered_files.add(
                        str(pathlib.Path(file).relative_to(subdir_context._subdir, walk_up=True))
                    )
                    subdir_context._rendered_files.add("ya.make.inc")


def render_static(context):
    aux_parameters = context.aux_parameters

    if aux_parameters["proto_api_path"] != aux_parameters["data_model_proto_full_path"]:
        with context.in_subdir("proto_api_path", keep_ya_make_and_md=True) as subdir_context:
            render_proto_api(subdir_context)

    with context.in_subdir("data_model_proto_full_path", keep_ya_make_and_md=True) as subdir_context:
        with _ya_make_inc(subdir_context, ".proto"):
            print("Generating proto API")
            if aux_parameters["proto_api_path"] == aux_parameters["data_model_proto_full_path"]:
                render_proto_api(subdir_context)

            if aux_parameters["access_control_file"] is not None:
                print("Generating data model (multiproto)")
                render_data_model_multiproto(subdir_context)
            else:
                print("Generating data model")
                render_data_model_proto(subdir_context, aux_parameters["client_data_model_filename"])

    with context.in_subdir("server_proto_path", keep_ya_make_and_md=True) as subdir_context:
        print("Generating server proto")
        if aux_parameters["server_multi_proto"]:
            with _ya_make_inc(subdir_context, ".proto"):
                render_server_multi_proto(subdir_context)
        else:
            render_server_proto(subdir_context)

    with context.in_subdir("server_objects_cpp_path") as subdir_context:
        print("Generating server objects")
        render_objects_lib_multi_file(subdir_context)

        if aux_parameters["program_lib_cpp_path"] == aux_parameters["server_objects_cpp_path"]:
            render_program_lib(subdir_context)

    if aux_parameters["program_lib_cpp_path"]:
        if aux_parameters["program_lib_cpp_path"] != aux_parameters["server_objects_cpp_path"]:
            with context.in_subdir("program_lib_cpp_path") as subdir_context:
                print("Generating master program")
                render_program_lib(subdir_context)

    if aux_parameters["server_bin_path"] is not None:
        with context.in_subdir("server_bin_path", keep_ya_make_and_md=True) as subdir_context:
            render_main_cpp(subdir_context, "main.cpp")

    if aux_parameters["db_versions_dir"] is not None:
        with context.in_subdir("db_versions_dir", delete_old_contents=False) as subdir_context:
            print("Generating schema in db_versions")
            render_db_versions(subdir_context)

    if aux_parameters["python_admin_lib"] is not None:
        with context.in_subdir("python_admin_lib", delete_old_contents=False) as subdir_context:
            print("Generating schema python admin")
            render_yt_schema(subdir_context, "schema.py")

    with context.in_subdir("client_misc_lib", keep_ya_make_and_md=True) as subdir_context:
        print("Generating client misc")
        render_client_misc(subdir_context)

        if aux_parameters["client_misc_lib"] == aux_parameters["client_objects_cpp_path"]:
            render_client_objects(subdir_context)

    if aux_parameters["client_misc_lib"] != aux_parameters["client_objects_cpp_path"]:
        with context.in_subdir("client_objects_cpp_path", keep_ya_make_and_md=True) as subdir_context:
            print("Generating client objects")
            render_client_objects(subdir_context)

    with context.in_subdir("client_native_generated_cpp_path", keep_ya_make_and_md=True) as subdir_context:
        print("Generating client native")
        render_client_native(subdir_context)

    for snapshot in subdir_context.model.snapshots.values():
        if not snapshot.cpp_path:
            continue

        print(f"Generating {snapshot.name} snapshot")
        with context.in_subdir(snapshot.cpp_path) as subdir_context:
            autogen_marker = aux_parameters["autogen_marker"]
            rendered_paths = render_snapshot(snapshot, subdir_context.output_dir, autogen_marker)
            for path in rendered_paths:
                context._rendered_files.add(path.name)
                context._all_rendered_files.add(str(subdir_context._subdir / path.name))

    render_codegen_verify(context)
