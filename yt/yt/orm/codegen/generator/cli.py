from . import engine

from .dynamic_parameters import needs_dynamic_parameters
from .engine import RenderContext
from .parameters import CodegenParameters

from yt.yt.orm.codegen.model.loader import load_model

import click
import functools


@click.group(help="Generates ORM stubs according to database schema")
@click.make_pass_decorator(CodegenParameters)
def dispatch(codegen_parameters):
    codegen_parameters.prepare()


def generator_command(requires_model=True):
    def decorator(function):
        def wrapper(codegen_parameters, *args, **kwargs):
            aux_parameters = codegen_parameters.aux_parameters.to_dict()
            if requires_model:
                model = load_model(codegen_parameters)
                return function(model=model, aux_parameters=aux_parameters, *args, **kwargs)
            else:
                return function(aux_parameters=aux_parameters, *args, **kwargs)

        renamed_wrapper = functools.update_wrapper(wrapper, function)
        pass_codegen_parameters = click.make_pass_decorator(CodegenParameters)
        return dispatch.command()(pass_codegen_parameters(renamed_wrapper))

    return decorator


def takes_output_dir(function):
    return click.option(
        "--output-dir",
        required=True,
        help="Directory to put generated filed into",
    )(function)


def takes_output_path(function):
    return click.option(
        "--output-path",
        "output_path",
        required=True,
        help="Path to output file",
    )(function)


def takes_shard_count(function):
    return click.option(
        "--shard-count",
        type=int,
        default=1,
        help="Number of shards to split .cpp files into",
    )(function)


@generator_command()
@takes_output_path
def render_data_model_proto(aux_parameters, model, output_path):
    engine.render_data_model_proto(RenderContext(aux_parameters, model, None), output_path)


@generator_command()
@takes_output_dir
def render_data_model_multiproto(aux_parameters, model, output_dir):
    engine.render_data_model_multiproto(RenderContext(aux_parameters, model, output_dir))


@generator_command()
@takes_output_dir
def render_proto_api(aux_parameters, model, output_dir):
    engine.render_proto_api(RenderContext(aux_parameters, model, output_dir))


@generator_command()
@takes_output_dir
def render_server_proto(aux_parameters, model, output_dir):
    engine.render_server_proto(RenderContext(aux_parameters, model, output_dir))


@generator_command()
@takes_output_dir
def render_objects_lib_multi_file(aux_parameters, model, output_dir):
    engine.render_objects_lib_multi_file(RenderContext(aux_parameters, model, output_dir))


@generator_command()
@takes_output_dir
def render_client_misc(aux_parameters, model, output_dir):
    engine.render_client_misc(RenderContext(aux_parameters, model, output_dir))


@generator_command()
@takes_output_dir
def render_client_native(aux_parameters, model, output_dir):
    engine.render_client_native(RenderContext(aux_parameters, model, output_dir))


@generator_command()
@takes_output_dir
def render_client_objects(aux_parameters, model, output_dir):
    engine.render_client_objects(RenderContext(aux_parameters, model, output_dir))


@generator_command()
@needs_dynamic_parameters(["user_codegen_dir"])
@takes_output_dir
def render_db_versions(aux_parameters, model, output_dir):
    engine.render_db_versions(RenderContext(aux_parameters, model, output_dir))


@generator_command()
@takes_output_path
def render_yt_schema(aux_parameters, model, output_path):
    engine.render_yt_schema(RenderContext(aux_parameters, model), output_path)


@generator_command()
@takes_output_dir
def render_program_lib(aux_parameters, model, output_dir):
    engine.render_program_lib(RenderContext(aux_parameters, model, output_dir))


@generator_command(requires_model=False)
@takes_output_path
def render_error_proto(aux_parameters, output_path):
    engine.render_error_proto(RenderContext(aux_parameters), output_path)


@generator_command(requires_model=False)
@takes_output_path
def render_error_cpp_enum(aux_parameters, output_path):
    engine.render_error_cpp_enum(RenderContext(aux_parameters), output_path)


@generator_command(requires_model=False)
@takes_output_path
def render_main_cpp(aux_parameters, output_path):
    engine.render_main_cpp(RenderContext(aux_parameters), output_path)


@generator_command()
@needs_dynamic_parameters(["snapshot_name"])
@takes_output_dir
def render_snapshot(aux_parameters, model, output_dir):
    engine.render_snapshot(RenderContext(aux_parameters, model, output_dir))


@generator_command()
@click.option("--arcadia-root", "arcadia_root", help="arcadia root dir", required=True)
@click.option("--check-diff", is_flag=True, help="check diff without updating files")
@click.option("--cpp-comment", is_flag=True, help="print progress messages as a c++ comment")
def render_static(aux_parameters, model, arcadia_root, check_diff, cpp_comment):
    if cpp_comment:
        print("/**")
    with RenderContext(aux_parameters, model, arcadia_root).check(check_diff) as context:
        engine.render_static(context)
    if cpp_comment:
        print("*/")
