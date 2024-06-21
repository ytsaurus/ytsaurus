from .common import set_param
from .driver import make_request, make_formatted_request, get_structured_format
from .ypath import YPath


def start_pipeline(pipeline_path, client=None):
    """Start YT Flow pipeline.

    :param pipeline_path: path to pipeline.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}

    return make_request("start_pipeline", params, client=client)


def stop_pipeline(pipeline_path, client=None):
    """Stop YT Flow pipeline.

    :param pipeline_path: path to pipeline.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}

    return make_request("stop_pipeline", params, client=client)


def pause_pipeline(pipeline_path, client=None):
    """Pause YT Flow pipeline.

    :param pipeline_path: path to pipeline.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}

    return make_request("pause_pipeline", params, client=client)


def get_pipeline_spec(pipeline_path, spec_path=None, format=None, client=None):
    """Get YT Flow pipeline spec.

    :param pipeline_path: path to pipeline.
    :param spec_path: path to part of the spec.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}
    set_param(params, "spec_path", spec_path)

    result = make_formatted_request(
        "get_pipeline_spec",
        params=params,
        format=format,
        client=client)
    return result


def set_pipeline_spec(pipeline_path, value, spec_path=None, expected_version=None, force=None, format=None, client=None):
    """Set YT Flow pipeline spec.

    :param pipeline_path: path to pipeline.
    :param spec: new pipeline spec.
    :param spec_path: path to part of the spec.
    :param expected_version: current spec expected version.
    :param force: if true, update spec even if pipeline is paused.
    """

    is_format_specified = format is not None
    format = get_structured_format(format, client=client)
    if not is_format_specified:
        value = format.dumps_node(value)

    params = {"pipeline_path": YPath(pipeline_path, client=client)}
    set_param(params, "spec_path", spec_path)
    set_param(params, "expected_version", expected_version)
    set_param(params, "force", force)

    return make_request(
        "set_pipeline_spec",
        params,
        data=value,
        client=client)


def remove_pipeline_spec(pipeline_path, spec_path=None, expected_version=None, force=None, client=None):
    """Remove YT Flow pipeline spec.

    :param pipeline_path: path to pipeline.
    :param spec_path: path to part of the spec.
    :param expected_version: current spec expected version.
    :param force: if true, remove spec even if pipeline is paused.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}
    set_param(params, "spec_path", spec_path)
    set_param(params, "expected_version", expected_version)
    set_param(params, "force", force)

    return make_request(
        "remove_pipeline_spec",
        params,
        client=client)


def get_pipeline_dynamic_spec(pipeline_path, spec_path=None, format=None, client=None):
    """Get YT Flow pipeline dynamic spec.

    :param pipeline_path: path to pipeline.
    :param spec_path: path to part of the spec.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}
    set_param(params, "spec_path", spec_path)

    result = make_formatted_request(
        "get_pipeline_dynamic_spec",
        params=params,
        format=format,
        client=client)
    return result


def set_pipeline_dynamic_spec(pipeline_path, value, spec_path=None, expected_version=None, format=None, client=None):
    """Set YT Flow pipeline dynamic spec.

    :param pipeline_path: path to pipeline.
    :param spec: new pipeline spec.
    :param spec_path: path to part of the spec.
    :param expected_version: current dynamic spec expected version.
    """

    is_format_specified = format is not None
    format = get_structured_format(format, client=client)
    if not is_format_specified:
        value = format.dumps_node(value)

    params = {"pipeline_path": YPath(pipeline_path, client=client)}
    set_param(params, "spec_path", spec_path)
    set_param(params, "expected_version", expected_version)

    return make_request(
        "set_pipeline_dynamic_spec",
        params,
        data=value,
        client=client)


def remove_pipeline_dynamic_spec(pipeline_path, spec_path=None, expected_version=None, client=None):
    """Remove YT Flow pipeline dynamic spec.

    :param pipeline_path: path to pipeline.
    :param spec_path: path to part of the spec.
    :param expected_version: current dynamic spec expected version.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}
    set_param(params, "spec_path", spec_path)
    set_param(params, "expected_version", expected_version)

    return make_request(
        "remove_pipeline_dynamic_spec",
        params,
        client=client)


def get_pipeline_state(pipeline_path, client=None):
    """Get YT Flow pipeline state

    :param pipeline_path: path to pipeline
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}

    return make_request(
        "get_pipeline_state",
        params,
        client=client)


def get_flow_view(pipeline_path, view_path=None, format=None, client=None):
    """Get YT Flow flow view

    :param pipeline_path: path to pipeline
    :param view_path: path to part of the view
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}
    set_param(params, "view_path", view_path)

    result = make_formatted_request(
        "get_flow_view",
        params=params,
        format=format,
        client=client)
    return result
