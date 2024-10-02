import click
import functools

GO_PACKAGE_PREFIX = "a.yandex-team.ru/"


def _needs_client_schema_path(function):
    def wrapper(client_schema_path, aux_parameters, *args, **kwargs):
        aux_parameters["client_schema_path"] = client_schema_path
        aux_parameters["client_schema_h"] = f"{client_schema_path}.pb.h"
        aux_parameters["client_schema_proto"] = f"{client_schema_path}.proto"

        return function(aux_parameters=aux_parameters, *args, **kwargs)

    return click.option(
        "--client-schema-path",
        required=True,
        help="Path to client schema",
    )(functools.update_wrapper(wrapper, function))


def _needs_proto_api_path(function):
    def wrapper(proto_api_path, aux_parameters, *args, **kwargs):
        aux_parameters["proto_api_path"] = proto_api_path
        if aux_parameters["client_go_api_package"] is None:
            aux_parameters["client_go_api_package"] = f"{GO_PACKAGE_PREFIX}{proto_api_path}"

        return function(aux_parameters=aux_parameters, *args, **kwargs)

    return click.option(
        "--proto-api-path",
        required=True,
        help="Path to proto API (Object Service and Discovery Service)",
    )(functools.update_wrapper(wrapper, function))


def _needs_user_codegen_dir(function):
    def wrapper(user_codegen_dir, aux_parameters, *args, **kwargs):
        aux_parameters["user_codegen_dir"] = user_codegen_dir
        return function(aux_parameters=aux_parameters, *args, **kwargs)

    return click.option(
        "--user-codegen-dir",
        required=True,
        help="Directory of user codegen entrypoint",
    )(functools.update_wrapper(wrapper, function))


def _needs_snapshot_name(function):
    def wrapper(snapshot_name, aux_parameters, *args, **kwargs):
        aux_parameters["snapshot_name"] = snapshot_name
        return function(aux_parameters=aux_parameters, *args, **kwargs)

    return click.option(
        "--snapshot-name",
        required=True,
        help="Name of the snapshot to render",
    )(functools.update_wrapper(wrapper, function))


_PARAMETER_TO_DECORATOR = dict(
    client_schema_path=_needs_client_schema_path,
    proto_api_path=_needs_proto_api_path,
    user_codegen_dir=_needs_user_codegen_dir,
    snapshot_name=_needs_snapshot_name,
)


def needs_dynamic_parameters(parameters):
    def get_parameter_decorator(parameter):
        if parameter not in _PARAMETER_TO_DECORATOR:
            raise KeyError(f"No decorator found for parameter {parameter}")

        return _PARAMETER_TO_DECORATOR[parameter]

    def decorator(function):
        for inner_decorator in map(get_parameter_decorator, parameters):
            function = inner_decorator(function)

        return function

    return decorator
