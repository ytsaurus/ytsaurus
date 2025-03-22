from .driver import make_request, make_formatted_request
from .config import get_config
from .batch_response import apply_function_to_result
from .table_helpers import _prepare_command_format
from .common import datetime_to_string, date_string_to_datetime, set_param, get_value, YtError, utcnow
from .http_helpers import get_proxy_address_url
from .errors import YtQueryFailedError
from .operation_commands import TimeWatcher, get_proxy_and_cluster_path, order_progress
from .exceptions_catcher import ExceptionCatcher

import yt.logger as logger

from collections import defaultdict
from datetime import datetime
from time import time
from typing import Iterator, Optional, Tuple
import logging


def start_query(engine, query, settings=None, files=None, stage=None, annotations=None, access_control_object=None, access_control_objects=None, secrets=None, client=None):
    """Start query.

    :param engine: one of "ql", "yql", "chyt", "spyt".
    :type engine: str
    :param query: text of a query
    :type query: str
    :param settings: a dictionary of settings
    :type settings: dict or None
    :param files: a YSON list of files, each of which is represented by a map with keys "name", "content", "type". Field "type" is one of "raw_inline_data", "url"
    :type files: list or None
    :param stage: query tracker stage, defaults to "production"
    :type stage: str
    :param annotations: a dictionary of annotations
    :type stage: dict or None
    :param access_control_object: access control object name
    :type access_control_object: str or None
    :param access_control_objects: list access control object names
    :type access_control_objects: list or None
    """

    params = {
        "engine": engine,
        "query": query,
        "settings": get_value(settings, {}),
        "files": get_value(files, []),
        "stage": get_value(stage, "production"),
        "annotations": get_value(annotations, {}),
        "secrets": get_value(secrets, []),
    }

    set_param(params, "access_control_object", access_control_object)
    set_param(params, "access_control_objects", access_control_objects)

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

    response = make_request(
        "read_query_result",
        params=params,
        return_content=False,
        use_heavy_proxy=True,
        client=client)

    if raw:
        return response
    else:
        return format.load_rows(response)


def get_query_result(query_id, result_index=None, stage=None, format=None, client=None):
    """Get query result.

    :param query_id: id of a query to get result
    :type query_id: str
    :param result_index: index of a result to get, defaults to 0
    :type result_index: int
    :param stage: query tracker stage, defaults to "production"
    :type stage: str
    """

    params = {
        "query_id": query_id,
        "result_index": get_value(result_index, 0),
        "stage": get_value(stage, "production"),
    }

    return make_formatted_request("get_query_result", params, format=format, client=client)


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


def alter_query(query_id, stage=None, annotations=None, access_control_objects=None, client=None):
    """Alter query.

    :param query_id: id of a query to get
    :type query_id: str
    :param stage: query tracker stage, defaults to "production"
    :type stage: str
    :param annotations: a dictionary of annotations
    :type stage: dict or None
    :param access_control_objects: list access control object names
    :type access_control_objects: list or None
    """

    params = {
        "query_id": query_id,
        "stage": get_value(stage, "production"),
    }
    set_param(params, "annotations", annotations)
    set_param(params, "access_control_objects", access_control_objects)

    return make_request("alter_query", params, client=client)


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

# Helpers


def get_query_url(query_id: str, client=None) -> str:
    proxy_url = get_proxy_address_url(required=False, client=client)
    if not proxy_url:
        return None

    proxy, cluster_path = get_proxy_and_cluster_path(client=client)

    return get_config(client)["proxy"]["query_link_pattern"].format(
        proxy=proxy,
        cluster_path=cluster_path,
        id=query_id)


class QueryState:
    """State of the query (simple wrapper for the string name)."""
    def __init__(self, name: str):
        self.name = name

    def is_finished(self) -> bool:
        return self.name in ("aborted", "completed", "failed", "draft")

    def is_successfully_finished(self) -> bool:
        return self.name in ("completed", "draft")

    def is_running(self) -> bool:
        return self.name == "running"

    def is_starting(self) -> bool:
        return self.name == "starting"

    def __eq__(self, other):
        return self.name == str(other)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)


def get_query_state_monitor(
        query_id: str, time_watcher: TimeWatcher, stage: Optional[str] = None, client=None) -> Iterator[QueryState]:
    """
    Yields state and sleeps. Waits for the state of query to become final.

    :return: iterator over query states.
    """
    last_state = None
    while True:
        state = QueryState(get_query(query_id, stage=stage, client=client, attributes=["state"])["state"])
        yield state
        if state.is_finished():
            break

        if state != last_state:
            time_watcher.reset()
        last_state = state

        time_watcher.wait()


class QueryResult:
    """Holds information about query result."""
    def __init__(self, query_id: str, result_index: int, query_tracker_stage: Optional[str] = None, client=None):
        self.query_id = query_id
        self.result_index = result_index
        self.query_tracker_stage = query_tracker_stage
        self.client = client
        self._cached_query_result = None

    def get_meta(self) -> dict:
        """Returns query result attributes."""
        if self._cached_query_result is None:
            self._cached_query_result = get_query_result(self.query_id, self.result_index, stage=self.query_tracker_stage, client=self.client)

        return self._cached_query_result

    def get_error(self) -> Optional[YtError]:
        """Returns error of the query result."""
        meta = self.get_meta()
        if "error" in meta:
            return YtError.from_dict(meta["error"])
        return None

    def is_truncated(self) -> bool:
        """Returns whether the result is truncated."""
        return self.get_meta().get("is_truncated", False)

    def full_result(self) -> Optional[dict]:
        """Returns table with full result. For yql it's yson containing 'cluster' and 'table_path'"""
        return self.get_meta().get("full_result")

    def read_rows(self, validate_not_truncated: bool = True, format: Optional[str] = None, raw: Optional[bool] = None):
        error = self.get_error()
        if error:
            raise error
        if validate_not_truncated and self.is_truncated():
            raise YtError("Query result is truncated, use get_rows(validate_not_truncated=False) to get the truncated "
                          "result")
        return read_query_result(self.query_id, self.result_index, stage=self.query_tracker_stage,
                                 format=format, raw=raw, client=self.client)

    def __iter__(self):
        return self.read_rows()


class GenericQueryInfoPrinter:
    """Tracks query state and prints info on updates."""
    def __init__(self, query: 'Query', client=None):
        self.query = query
        self.state = None
        self.progress_line = None
        # TODO(max42): the corresponding code in operation_commands.py handles timezones incorrectly
        # around the daylight saving time changes. That code should be fixed as well.

        start_time_str = self.query.get_meta(attributes=["start_time"])["start_time"]
        self.start_time = date_string_to_datetime(start_time_str)
        self.client = client
        self.level = logging.getLevelName(get_config(self.client)["query_tracker"]["progress_logging_level"])

    def __call__(self, new_state):
        if new_state.is_running():
            new_progress_line = self.get_running_progress_line()
            if self.progress_line != new_progress_line:
                self.log("query %s: %s", self.query.id, new_progress_line)
            self.progress_line = new_progress_line
        elif new_state != self.state:
            self.log("query %s %s", self.query.id, new_state)
        self.state = new_state

    # This method is to be overridden in subclasses for concrete engines.
    def get_running_progress_line(self) -> str:
        """Return a line that is to be printed for a running operation. It is printed only
        if it differs from its previous state"""
        return "running"

    def log(self, message: str, *args, **kwargs):
        elapsed_seconds = (utcnow() - self.start_time).total_seconds()
        message = "({0:2} min) ".format(int(elapsed_seconds) // 60) + message
        logger.log(self.level, message, *args, **kwargs)


class YqlQueryInfoPrinter(GenericQueryInfoPrinter):
    def get_running_progress_line(self) -> str:
        progress = self.query.get_progress()
        yql_progress = progress.get("yql_progress", {})
        # YQL node progress contains something similar to YT progress counter and also some additional
        # fields like "finishedAt" or "stages". We want to aggregate numbers that are similar to YT progress
        # and output them similar to how YT operation progress is printed.
        key_whitelist = ["aborted", "completed", "failed", "lost", "pending", "running", "total"]
        total_progress = defaultdict(int)
        for node_id, node_progress in yql_progress.items():
            for key, value in node_progress.items():
                if key in key_whitelist:
                    total_progress[key] += value
        return " ".join("{0}={1:<5}".format(k, v) for k, v in order_progress(total_progress))


class Query:
    """Holds information about query."""
    def __init__(
            self, id: str, engine: str, query_tracker_stage: Optional[str] = None,
            abort_exceptions: Tuple[type] = (KeyboardInterrupt, TimeoutError), client=None):
        self.id = id
        self.engine = engine
        self.abort_exceptions = abort_exceptions
        self.client = client
        self.url = get_query_url(id, client=client)
        self.query_tracker_stage = query_tracker_stage

        # TODO(max42): CHYT support was introduced quite recently, so we do not support it yet. QL does not
        # provide means of tracking query progress, so we do not support it either. SPYT will also follow later.
        printers = {
            "yql": YqlQueryInfoPrinter,
        }
        self.printer = printers.get(self.engine, GenericQueryInfoPrinter)(self, client=client)

    def abort(self, message: str = None):
        """Aborts query."""
        abort_query(self.id, message=message, stage=self.query_tracker_stage, client=self.client)

    def get_state_monitor(self, time_watcher: TimeWatcher) -> Iterator[QueryState]:
        """Returns iterator over query progress states."""
        return get_query_state_monitor(
            self.id, time_watcher, stage=self.query_tracker_stage, client=self.client)

    def get_meta(self, attributes=None) -> dict:
        """Returns query attributes, possibly with given attribute filter."""
        return get_query(self.id, attributes=attributes, stage=self.query_tracker_stage, client=self.client)

    def get_progress(self) -> dict:
        """Returns dictionary that represents the query execution progress."""
        return self.get_meta(attributes=["progress"]).get("progress", {})

    def get_state(self) -> QueryState:
        """Returns object that represents state of operation."""
        return QueryState(self.get_meta(attributes=["state"])["state"])

    def get_result(self, result_index: int = 0) -> QueryResult:
        """Returns query result with the given index."""
        return QueryResult(self.id, result_index, query_tracker_stage=self.query_tracker_stage, client=self.client)

    def get_results(self) -> list:
        """Returns list of query results."""
        result_count = self.get_meta(attributes=["result_count"])["result_count"]
        return [self.get_result(i) for i in range(result_count)]

    def __iter__(self):
        if len(self.get_results()) != 1:
            raise YtError("Query does not have exactly one result; use get_results() instead")
        return iter(self.get_result(0))

    def get_error(self, return_error_if_all_results_are_errors: bool = True) -> Optional[YtQueryFailedError]:
        """If query has failed, or, if return_error_if_all_results_are_errors = True and
        all subqueries within a query resulted in errors, returns YtQueryFailed. Subquery errors
        are returned as inner errors. Otherwise, return None."""
        state = self.get_state()

        if not state.is_finished():
            return None

        if not state.is_successfully_finished():
            error = self.get_meta(attributes=["error"])["error"]
            return YtQueryFailedError(self.id, state, [error], self.url)

        if return_error_if_all_results_are_errors:
            result_errors = [result.get_error() for result in self.get_results()]
            if len(result_errors) >= 1 and all(error is not None for error in result_errors):
                # YtQueryError.
                return YtQueryFailedError(
                    "All subqueries within a query resulted in errors",
                    QueryState("failed"),
                    result_errors,
                    self.url)

        return None

    def wait(self, print_progress: bool = True, timeout: Optional[int] = None,
             raise_if_all_results_are_errors: Optional[bool] = True):
        """Synchronously tracks query, prints current progress and finalizes at the completion.

        If query fails, raises :class:`YtOperationFailedError <yt.wrapper.errors.YtOperationFailedError>`.
        If `KeyboardInterrupt` occurs, aborts query, finalizes and re-raises `KeyboardInterrupt`.

        :param print_progress: print progress
        :param timeout: timeout of query in msec. If `None`, wait indefinitely.
        :param raise_if_all_results_are_errors: if `True`, raise `YtQueryFailedError` if all results are errors
        even for a completed query.
        """

        query_poll_period = get_config(self.client)["query_tracker"]["poll_period"] / 1000.0
        time_watcher = TimeWatcher(min_interval=query_poll_period / 10.0,
                                   max_interval=query_poll_period,
                                   slowdown_coef=0.2)
        print_info = self.printer if print_progress else lambda state: None

        def abort():
            self.abort(message="query aborted by client interruption")
            for state in self.get_state_monitor(TimeWatcher(1.0, 1.0, 0.0)):
                print_info(state)

        abort_on_sigint = get_config(self.client)["query_tracker"]["abort_on_sigint"]

        with ExceptionCatcher(self.abort_exceptions, abort, enable=abort_on_sigint):
            start_time = time()
            for state in self.get_state_monitor(time_watcher):
                print_info(state)
                if timeout is not None and time() - start_time > timeout / 1000.0:
                    raise TimeoutError("Timed out while waiting for query to finish")

        error = self.get_error(return_error_if_all_results_are_errors=raise_if_all_results_are_errors)
        if error is not None:
            raise error


def run_query(
        engine: str, query: str, settings: Optional[dict] = None, files: Optional[list] = None, stage: Optional[str] = None,
        annotations: Optional[dict] = None, access_control_objects: Optional[list] = None, sync: bool = True,
        client=None) -> Query:
    """Run query and track its progress (unless sync = false).

    :param engine: one of "ql", "yql", "chyt", "spyt".
    :type engine: str
    :param query: text of a query
    :type query: str
    :param settings: a dictionary of settings
    :type settings: dict or None
    :param files: a YSON list of files, each of which is represented by a map with keys "name", "content", "type". Field "type" is one of "raw_inline_data", "url"
    :type files: list or None
    :param stage: query tracker stage, defaults to "production"
    :type stage: str or None
    :param annotations: a dictionary of annotations
    :type annotations: dict or None
    :param access_control_objects: list access control object names
    :type access_control_objects: list or None
    :param sync: if True, wait for query to finish, otherwise return immediately
    :type sync: bool
    """

    query_id = start_query(engine, query, settings=settings, files=files, stage=stage,
                           annotations=annotations, access_control_objects=access_control_objects, client=client)
    query = Query(query_id, engine, query_tracker_stage=stage, client=client)
    logger.info("Query started: %s", query.url or query.id)
    if sync:
        query.wait()
    return query
