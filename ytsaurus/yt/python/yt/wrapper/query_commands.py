from .driver import make_request, make_formatted_request
from .config import get_config
from .batch_response import apply_function_to_result
from .table_helpers import _prepare_command_format
from .common import datetime_to_string, set_param, get_value

from datetime import datetime


def start_query(engine, query, settings=None, stage=None, client=None):
    """Start query.

    :param engine: one of "ql", "yql".
    :type engine: str
    :param query: text of a query
    :type query: str
    :param settings: a ditionary of settings
    :type settings: dict or None
    :param stage: query tracker stage, defaults to "production"
    :type stage: str
    """

    params = {
        "engine": engine,
        "query": query,
        "settings": get_value(settings, {}),
        "stage": get_value(stage, "production"),
    }

    response = make_formatted_request("start_query", params, format=None, client=client)

    query_id = apply_function_to_result(
        lambda response: response["query_id"],
        response)

    return query_id


def abort_query(query_id, message=None, stage=None, client=None):
    """Abort query.

    :param query_id: id of a query to abort
    :type query_id: str
    :param message: optional message to be shown in query abort error
    :type message: str or None
    :param stage: query tracker stage, defaults to "production"
    :type stage: str
    """

    params = {
        "query_id": query_id,
        "stage": get_value(stage, "production"),
    }
    set_param(params, "message", message)

    return make_request("abort_query", params, client=client)


def read_query_result(query_id, result_index=None, stage=None, format=None, raw=None, client=None):
    """Read query result.

    :param query_id: id of a query to read result
    :type query_id: str
    :param result_index: index of a result to read, defaults to 0
    :type result_index: int
    :param stage: query tracker stage, defaults to "production"
    :type stage: str
    """

    if raw is None:
        raw = get_config(client)["default_value_of_raw_option"]
    format = _prepare_command_format(format, raw, client)

    params = {
        "query_id": query_id,
        "result_index": get_value(result_index, 0),
        "output_format": format.to_yson_type(),
        "stage": get_value(stage, "production"),
    }

    return make_request(
        "read_query_result",
        params=params,
        return_content=False,
        use_heavy_proxy=True,
        client=client)


def get_query(query_id, attributes=None, stage=None, format=None, client=None):
    """Get query.

    :param query_id: id of a query to get
    :type query_id: str
    :param attributes: optional attribute filter
    :type attributes: list or None
    :param stage: query tracker stage, defaults to "production"
    :type stage: str
    """

    params = {
        "query_id": query_id,
        "stage": get_value(stage, "production"),
    }
    set_param(params, "attributes", attributes)

    return make_formatted_request("get_query", params, format=format, client=client)


def list_queries(user=None, engine=None, state=None, filter=None, from_time=None, to_time=None, cursor_time=None,
                 cursor_direction=None, limit=None, attributes=None, stage=None, format=None, client=None):
    """List operations that satisfy given options.
    """
    def format_time(time):
        if isinstance(time, datetime):
            return datetime_to_string(time)
        return time

    params = {
        "stage": get_value(stage, "production"),
    }
    set_param(params, "user", user)
    set_param(params, "engine", engine)
    set_param(params, "state", state)
    set_param(params, "filter", filter)
    set_param(params, "from_time", from_time, format_time)
    set_param(params, "to_time", to_time, format_time)
    set_param(params, "cursor_time", cursor_time, format_time)
    set_param(params, "cursor_direction", cursor_direction)
    set_param(params, "limit", limit)
    set_param(params, "attributes", attributes)

    return make_formatted_request(
        "list_queries",
        params=params,
        format=format,
        client=client)
