from . import engine

from .dynamic_parameters import needs_dynamic_parameters
from .parameters import CodegenParameters

from yt.yt.orm.codegen.model.loader import load_model

import click
import functools
import pathlib
import sys


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
        default=".",
        help="Directory to put generated filed into",
    )(function)


def takes_output_stream(function):
    def wrapper(output_path, *args, **kwargs):
        if output_path is None:
            return function(output_stream=sys.stdout, *args, **kwargs)

        with open(output_path, "w") as file:
            return function(output_stream=file, *args, **kwargs)

    return click.option(
        "--output-path",
        default=None,
        help="Path to output file; stdout will be used by default",
    )(functools.update_wrapper(wrapper, function))


def takes_output_file_name(function):
    return click.option(
        "--output",
        "output_file_name",
        required=True,
        help="Main output file name",
    )(function)


def takes_aux_file_names(function):
    return click.option(
        "--aux-output",
        "aux_file_names",
        multiple=True,
        help="Auxiliary output file names",
    )(function)


def takes_shard_count(function):
    return click.option(
        "--shard-count",
        type=int,
        default=1,
        help="Number of shards to split .cpp files into",
    )(function)


@generator_command()
@takes_output_stream
def render_data_model_proto(aux_parameters, model, output_stream):
    engine.render_data_model_proto(aux_parameters, model, output_stream)


@generator_command()
@takes_output_dir
@takes_output_file_name
@takes_aux_file_names
def render_data_model_multiproto(aux_parameters, model, output_dir, output_file_name, aux_file_names):
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    engine.render_data_model_multiproto(aux_parameters, model, output_dir, output_file_name, aux_file_names)


@generator_command()
@needs_dynamic_parameters(["client_schema_path", "proto_api_path"])
@takes_output_dir
def render_proto_api(aux_parameters, model, output_dir):
    engine.render_proto_api(aux_parameters, model, output_dir)


@generator_command()
@needs_dynamic_parameters(["client_schema_path"])
@takes_output_dir
def render_server_proto(aux_parameters, model, output_dir):
    engine.render_server_proto(aux_parameters, model, output_dir)


@generator_command()
@needs_dynamic_parameters(["client_schema_path"])
@takes_output_dir
@takes_shard_count
def render_objects_lib(aux_parameters, model, output_dir, shard_count):
    engine.render_objects_lib(aux_parameters, model, output_dir, shard_count)


@generator_command()
@needs_dynamic_parameters(["client_schema_path"])
@takes_output_dir
@takes_shard_count
def render_yp_objects_lib(aux_parameters, model, output_dir, shard_count):
    engine.render_yp_objects_lib(aux_parameters, model, output_dir, shard_count)


@generator_command()
@needs_dynamic_parameters(["client_schema_path"])
@takes_output_dir
def render_client_misc(aux_parameters, model, output_dir):
    engine.render_client_misc(aux_parameters, model, output_dir)


@generator_command()
@needs_dynamic_parameters(["proto_api_path"])
@takes_output_dir
def render_client_native(aux_parameters, model, output_dir):
    engine.render_client_native(aux_parameters, model, output_dir)


@generator_command()
@takes_output_dir
def render_client_objects(aux_parameters, model, output_dir):
    engine.render_client_objects(aux_parameters, model, output_dir)


@generator_command()
@needs_dynamic_parameters(["user_codegen_dir"])
@takes_output_dir
def render_db_versions(aux_parameters, model, output_dir):
    engine.render_db_versions(aux_parameters, model, output_dir)


@generator_command()
@takes_output_stream
def render_yt_schema(aux_parameters, model, output_stream):
    engine.render_yt_schema(aux_parameters, model, output_stream)


@generator_command()
@takes_output_dir
def render_program_lib(aux_parameters, model, output_dir):
    engine.render_program_lib(aux_parameters, model, output_dir)


@generator_command(requires_model=False)
@takes_output_stream
def render_error_proto(aux_parameters, output_stream):
    engine.render_error_proto(aux_parameters, output_stream)


@generator_command(requires_model=False)
@takes_output_stream
def render_error_cpp_enum(aux_parameters, output_stream):
    engine.render_error_cpp_enum(aux_parameters, output_stream)


@generator_command(requires_model=False)
@takes_output_stream
def render_main_cpp(aux_parameters, output_stream):
    engine.render_main_cpp(aux_parameters, output_stream)


@generator_command()
@needs_dynamic_parameters(["snapshot_name"])
@takes_output_dir
def render_snapshot(aux_parameters, model, output_dir):
    engine.render_snapshot(aux_parameters, model, output_dir)
