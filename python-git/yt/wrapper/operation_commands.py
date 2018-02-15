from .common import ThreadPoolHelper, set_param, datetime_to_string
from .config import get_config
from .errors import YtOperationFailedError, YtResponseError
from .driver import make_request, make_formatted_request
from .http_helpers import get_proxy_url, get_retriable_errors
from .exceptions_catcher import ExceptionCatcher
from .cypress_commands import exists, get, list
from .ypath import ypath_join
from .file_commands import read_file
from .job_commands import list_jobs, get_job_stderr
from . import yson

import yt.logger as logger
from yt.common import format_error, date_string_to_datetime, to_native_str, flatten

from yt.packages.decorator import decorator
from yt.packages.six import iteritems, itervalues
from yt.packages.six.moves import builtins, filter as ifilter, map as imap

import logging
from datetime import datetime
from time import sleep
from multiprocessing import TimeoutError

try:
    from cStringIO import StringIO
except ImportError:  # Python 3
    from io import StringIO

OPERATIONS_PATH = "//sys/operations"


# Public functions

def abort_operation(operation, reason=None, client=None):
    """Aborts operation.

    Do nothing if operation is in final state.

    :param str operation: operation id.
    """
    if get_operation_state(operation, client=client).is_finished():
        return
    params = {"operation_id": operation}
    set_param(params, "abort_reason", reason)
    make_request("abort_op", params, client=client)

def suspend_operation(operation, client=None):
    """Suspends operation.

    :param str operation: operation id.
    """
    make_request("suspend_op", {"operation_id": operation}, client=client)

def resume_operation(operation, client=None):
    """Continues operation after suspending.

    :param str operation: operation id.
    """
    make_request("resume_op", {"operation_id": operation}, client=client)

def complete_operation(operation, client=None):
    """Completes operation.

    Aborts all running and pending jobs.
    Preserves results of finished jobs.
    Does nothing if operation is in final state.

    :param str operation: operation id.
    """
    if get_operation_state(operation, client=client).is_finished():
        return
    make_request("complete_op", {"operation_id": operation}, client=client)

def get_operation(operation_id, attributes=None, client=None):
    """Get operation attributes through API.
    """
    params={"operation_id": operation_id}
    set_param(params, "attributes", attributes)
    return make_formatted_request(
        "get_operation",
        params,
        format=None,
        client=client)

def list_operations(user=None, state=None, type=None, filter=None, pool=None, with_failed_jobs=None,
                    from_time=None, to_time=None, cursor_time=None, cursor_direction=None,
                    include_archive=None, include_counters=None, limit=None, enable_ui_mode=False,
                    client=None):
    """List operations that satisfy given options.
    """
    def format_time(time):
        if isinstance(time, datetime):
            return datetime_to_string(time)
        return time

    params = {}
    set_param(params, "user", user)
    set_param(params, "state", state, lambda state: state.name if isinstance(state, OperationState) else state)
    set_param(params, "type", type)
    set_param(params, "filter", filter)
    set_param(params, "pool", pool)
    set_param(params, "with_failed_jobs", with_failed_jobs)
    set_param(params, "from_time", from_time, format_time)
    set_param(params, "to_time", to_time, format_time)
    set_param(params, "cursor_time", cursor_time, format_time)
    set_param(params, "cursor_direction", cursor_direction)
    set_param(params, "include_archive", include_archive)
    set_param(params, "include_counters", include_counters)
    set_param(params, "limit", limit)
    set_param(params, "enable_ui_mode", enable_ui_mode)

    return make_formatted_request(
        "list_operations",
        params=params,
        format=None,
        client=client)

def update_operation_parameters(operation_id, parameters, client=None):
    """Updates operation runtime parameters."""
    return make_request(
        "update_op_parameters",
        {"operation_id": operation_id, "parameters": parameters},
        client=client)

# Helpers

class OperationState(object):
    """State of operation (simple wrapper for string name)."""
    def __init__(self, name):
        self.name = name

    def is_finished(self):
        return self.name in ["aborted", "completed", "failed"]

    def is_unsuccessfully_finished(self):
        return self.name in ["aborted", "failed"]

    def is_running(self):
        return self.name == "running"

    def __eq__(self, other):
        return self.name == str(other)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name


class TimeWatcher(object):
    """Class for proper sleeping in waiting operation."""
    def __init__(self, min_interval, max_interval, slowdown_coef):
        """
        Initialize time watcher.

        :param min_interval: minimal sleeping interval.
        :param max_interval: maximal sleeping interval.
        :param slowdown_coef: growth coefficient of sleeping interval.
        """
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.slowdown_coef = slowdown_coef
        self.total_time = 0.0

    def _bound(self, interval):
        return min(max(interval, self.min_interval), self.max_interval)

    def wait(self):
        """Sleep proper time."""
        pause = self._bound(self.total_time * self.slowdown_coef)
        self.total_time += pause
        sleep(pause)


class OperationProgressFormatter(logging.Formatter):
    def __init__(self, format="%(asctime)-15s\t%(message)s", date_format=None, start_time=None):
        logging.Formatter.__init__(self, format, date_format)
        if start_time is None:
            self._start_time = datetime.now()
        else:
            self._start_time = start_time

    def formatTime(self, record, date_format=None):
        created = datetime.fromtimestamp(record.created)
        if date_format is not None:
            return created.strftime(date_format)
        else:
            def total_minutes(time):
                return time.seconds // 60 + 60 * 24 * time.days
            elapsed = total_minutes(datetime.now() - self._start_time)
            time = datetime.now()
            if time.microsecond > 0:
                time = time.isoformat(" ")[:-3]
            else:
                time = time.isoformat(" ")
            return "{0} ({1:2} min)".format(time, elapsed)

def get_operation_attributes(operation, fields=None, client=None):
    """Returns dict with operation attributes.

    :param str operation: operation id.
    :return: operation description.
    :rtype: dict
    """
    if get_config(client)["enable_operations_api"]:
        return get_operation(operation, attributes=fields, client=client)
    else:
        operation_path = ypath_join(OPERATIONS_PATH, operation)
        return get(operation_path + "/@", attributes=fields, client=client)

def get_operation_state(operation, client=None):
    """Returns current state of operation.

    :param str operation: operation id.

    Raises :class:`YtError <yt.common.YtError>` if operation does not exists.
    """
    config = get_config(client)
    retry_count = config["proxy"]["retries"]["count"]
    config["proxy"]["retries"]["count"] = config["proxy"]["operation_state_discovery_retry_count"]
    try:
        return OperationState(get_operation_attributes(operation, fields=["state"], client=client)["state"])
    finally:
        config["proxy"]["retries"]["count"] = retry_count

def get_operation_progress(operation, client=None):
    def calculate_total(counter):
        if isinstance(counter, dict):
            return sum(imap(calculate_total, itervalues(counter)))
        return counter

    try:
        attributes = get_operation_attributes(operation, fields=["progress"], client=client)
        progress = attributes.get("progress", {}).get("jobs", {})
        for key in progress:
            # Show total for hierarchical count.
            if key in progress and isinstance(progress[key], dict):
                if "total" in progress[key]:
                    progress[key] = progress[key]["total"]
                else:
                    progress[key] = calculate_total(progress[key])

    except YtResponseError as err:
        if err.is_resolve_error():
            progress = {}
        else:
            raise
    return progress

def order_progress(progress):
    filter_out = ["completed_details"]
    keys = ["running", "completed", "pending", "failed", "aborted", "lost", "total"]
    result = []
    for key in keys:
        if key in filter_out:
            continue
        if key in progress:
            result.append((key, progress[key]))
    for key, value in iteritems(progress):
        if key not in keys:
            result.append((key, value))
    return result

class PrintOperationInfo(object):
    """Caches operation state and prints info by update."""
    def __init__(self, operation, client=None):
        self.operation = operation
        self.state = None
        self.progress = None

        creation_time_str = get_operation_attributes(operation, fields=["start_time"], client=client)["start_time"]
        creation_time = date_string_to_datetime(creation_time_str).replace(tzinfo=None)
        local_creation_time = creation_time + (datetime.now() - datetime.utcnow())

        self.formatter = OperationProgressFormatter(start_time=local_creation_time)

        self.client = client
        self.level = logging.getLevelName(get_config(self.client)["operation_tracker"]["progress_logging_level"])

    def __call__(self, state):
        if state.is_running():
            progress = get_operation_progress(self.operation, client=self.client)
            if progress and progress != self.progress:
                self.log(
                    "operation %s: %s",
                    self.operation,
                    "\t".join("{0}={1}".format(k, v) for k, v in order_progress(progress)))
            self.progress = progress
        elif state != self.state:
            self.log("operation %s %s", self.operation, state)
        self.state = state

    def log(self, *args, **kwargs):
        if logger.LOGGER.isEnabledFor(self.level):
            old_formatter = logger.formatter
            logger.set_formatter(self.formatter)
            logger.log(self.level, *args, **kwargs)
            logger.set_formatter(old_formatter)

def get_operation_state_monitor(operation, time_watcher, action=lambda: None, client=None):
    """
    Yields state and sleeps. Waits for final state of operation.

    :return: iterator over operation states.
    """
    while True:
        action()

        state = get_operation_state(operation, client=client)
        yield state
        if state.is_finished():
            break
        time_watcher.wait()


def get_stderrs(operation, only_failed_jobs, client=None):
    # TODO(ostyakov): Remove local import
    from .client import YtClient

    def get_stderr_from_job(job, yt_client):
        job_with_stderr = {}
        job_with_stderr["host"] = job.attributes["address"]

        if only_failed_jobs:
            job_with_stderr["error"] = job.attributes["error"]

        ignore_errors = get_config(yt_client)["operation_tracker"]["ignore_stderr_if_download_failed"]

        stderr = None

        if get_config(client)["enable_operations_api"]:
            try:
                stderr = to_native_str(get_job_stderr(operation, job, client=client).read())
            except tuple(builtins.list(get_retriable_errors()) + [YtResponseError]) as err:
                if err.is_no_such_job():
                    pass
                elif not ignore_errors:
                    raise
        else:
            stderr_path = ypath_join(OPERATIONS_PATH, operation, "jobs", job, "stderr")
            has_stderr = exists(stderr_path, client=yt_client)
            if has_stderr:
                try:
                    stderr = to_native_str(read_file(stderr_path, client=yt_client).read())
                except tuple(builtins.list(get_retriable_errors()) + [YtResponseError]):
                    if not ignore_errors:
                        raise
        if stderr is not None:
            job_with_stderr["stderr"] = stderr
        return job_with_stderr

    result = []

    def download_job_stderr(job):
        yt_client = YtClient(config=get_config(client))

        job_with_stderr = get_stderr_from_job(job, yt_client)
        if job_with_stderr:
            result.append(job_with_stderr)

    if get_config(client)["enable_operations_api"]:
        job_state = None
        if only_failed_jobs:
            job_state = "failed"

        response = list_jobs(operation, include_cypress=True, include_archive=True, include_runtime=False, job_state=job_state, client=client)

        jobs = []
        for info in response["jobs"]:
            attributes = {"address": info["address"]}
            if "error" in info:
                attributes["error"] = info["error"]
            jobs.append(yson.to_yson_type(info["id"], attributes=attributes))
    else:
        jobs_path = ypath_join(OPERATIONS_PATH, operation, "jobs")
        if not exists(jobs_path, client=client):
            return []
        jobs = list(jobs_path, attributes=["error", "address"], client=client)
        if only_failed_jobs:
            jobs = builtins.list(ifilter(lambda obj: "error" in obj.attributes, jobs))

    if not get_config(client)["operation_tracker"]["stderr_download_threading_enable"]:
        return [get_stderr_from_job(job, client) for job in jobs]

    thread_count = min(get_config(client)["operation_tracker"]["stderr_download_thread_count"], len(jobs))
    if thread_count > 0:
        pool = ThreadPoolHelper(thread_count)
        timeout = get_config(client)["operation_tracker"]["stderr_download_timeout"] / 1000.0
        try:
            pool.map_async(download_job_stderr, jobs).get(timeout)
        except TimeoutError:
            logger.info("Timed out while downloading jobs stderr messages")
        finally:
            pool.terminate()
            pool.join()
    return result

def format_operation_stderr(job_with_stderr, output):
    output.write("Host: ")
    output.write(job_with_stderr["host"])
    output.write("\n")

    if "error" in job_with_stderr:
        output.write("Error:\n")
        output.write(format_error(job_with_stderr["error"]))
        output.write("\n")

    if "stderr" in job_with_stderr:
        output.write(job_with_stderr["stderr"])
        output.write("\n")

def format_operation_stderrs(jobs_with_stderr):
    """Formats operation jobs with stderr to string."""
    output = StringIO()
    for job_with_stderr in jobs_with_stderr:
        format_operation_stderr(job_with_stderr, output)
        output.write("\n")
    return output.getvalue()

# TODO(ignat): is it convinient and generic way to get stderrs? Move to tests? Or remove it completely?
def add_failed_operation_stderrs_to_error_message(func):
    def _add_failed_operation_stderrs_to_error_message(func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except YtOperationFailedError as error:
            if "stderrs" in error.attributes:
                error.message = error.message + format_operation_stderrs(error.attributes["stderrs"])
            raise
    return decorator(_add_failed_operation_stderrs_to_error_message, func)

def get_operation_error(operation, client=None):
    # NB(ignat): conversion to json type necessary for json.dumps in TM.
    # TODO(ignat): we should decide what format should be used in errors (now it is yson both here and in http.py).
    result = yson.yson_to_json(get_operation_attributes(operation, fields=["result"], client=client).get("result", {}))
    if "error" in result and result["error"]["code"] != 0:
        return result["error"]
    return None

def _create_operation_failed_error(operation, state):
    error = get_operation_error(operation.id, client=operation.client)
    stderrs = get_stderrs(operation.id, only_failed_jobs=True, client=operation.client)
    return YtOperationFailedError(
        id=operation.id,
        state=str(state),
        error=error,
        stderrs=stderrs,
        url=operation.url)

def get_operation_url(operation, client=None):
    proxy_url = get_proxy_url(required=False, client=client)
    if not proxy_url:
        return None

    return get_config(client)["proxy"]["operation_link_pattern"].format(
        proxy=get_proxy_url(client=client),
        id=operation)

class Operation(object):
    """Holds information about started operation."""
    def __init__(self, id, type=None, finalization_actions=None, abort_exceptions=(KeyboardInterrupt,), client=None):
        self.id = id
        self.type = type
        self.abort_exceptions = abort_exceptions
        self.finalization_actions = finalization_actions
        self.client = client
        self.printer = PrintOperationInfo(id, client=client)
        self.url = get_operation_url(id, client=client)

    def suspend(self):
        """Suspends operation."""
        suspend_operation(self.id, client=self.client)

    def resume(self):
        """Resumes operation."""
        resume_operation(self.id, client=self.client)

    def abort(self, reason=None):
        """Aborts operation."""
        abort_operation(self.id, reason, client=self.client)

    def complete(self):
        """Completes operation."""
        complete_operation(self.id, client=self.client)

    def get_state_monitor(self, time_watcher, action=lambda: None):
        """Returns iterator over operation progress states."""
        return get_operation_state_monitor(self.id, time_watcher, action, client=self.client)

    def get_attributes(self):
        """Returns all operation attributes."""
        return get_operation_attributes(self.id, client=self.client)

    def get_job_statistics(self):
        """Returns job statistics of operation."""
        try:
            return get_operation_attributes(self.id, fields=["progress"], client=self.client)["progress"]["job_statistics"]
        except YtResponseError as error:
            if error.is_resolve_error():
                return {}
            raise

    def get_progress(self):
        """Returns dictionary that represents number of different types of jobs."""
        return get_operation_progress(self.id, client=self.client)

    def get_state(self):
        """Returns object that represents state of operation."""
        return get_operation_state(self.id, client=self.client)

    def get_stderrs(self, only_failed_jobs=False):
        """Returns list of objects thar represents jobs with stderrs.
        Each object is dict with keys "stderr", "error" (if applyable), "host".

        :param bool only_failed_jobs: consider only failed jobs.
        """
        return get_stderrs(self.id, only_failed_jobs=only_failed_jobs, client=self.client)

    def exists(self):
        """Checks if operation attributes can be fetched from Cypress."""
        try:
            self.get_attributes()
        except YtResponseError as err:
            if err.is_resolve_error():
                return False
            raise

        return True

    def wait(self, check_result=True, print_progress=True, timeout=None):
        """Synchronously tracks operation, prints current progress and finalize at the completion.

        If operation failed, raises :class:`YtOperationFailedError <yt.wrapper.errors.YtOperationFailedError>`.
        If `KeyboardInterrupt` occurred, aborts operation, finalizes and reraises `KeyboardInterrupt`.

        :param bool check_result: get stderr if operation failed
        :param bool print_progress: print progress
        :param float timeout: timeout of operation in sec. `None` means operation is endlessly waited for.
        """

        finalization_actions = flatten(self.finalization_actions) if self.finalization_actions else []
        operation_poll_period = get_config(self.client)["operation_tracker"]["poll_period"] / 1000.0
        time_watcher = TimeWatcher(min_interval=operation_poll_period / 5.0,
                                   max_interval=operation_poll_period,
                                   slowdown_coef=0.1)
        print_info = self.printer if print_progress else lambda state: None

        def abort():
            for state in self.get_state_monitor(TimeWatcher(1.0, 1.0, 0.0), self.abort):
                print_info(state)

            for finalize_function in finalization_actions:
                finalize_function(state)

        abort_on_sigint = get_config(self.client)["operation_tracker"]["abort_on_sigint"]
        with ExceptionCatcher(self.abort_exceptions, abort, enable=abort_on_sigint):
            for state in self.get_state_monitor(time_watcher):
                print_info(state)

            for finalize_function in finalization_actions:
                finalize_function(state)

        if check_result and state.is_unsuccessfully_finished():
            error = _create_operation_failed_error(self, state)
            if get_config(self.client)["operation_tracker"]["enable_logging_failed_operation"]:
                logger.warning("***** Failed operation information:\n%s", str(error))
                if error.attributes["stderrs"]:
                    job_output = StringIO()
                    format_operation_stderr(error.attributes["stderrs"][0], job_output)
                    logger.warning("One of the failed jobs:\n%s", job_output.getvalue())
            raise error

        if get_config(self.client)["operation_tracker"]["log_job_statistics"]:
            statistics = self.get_job_statistics()
            if statistics:
                logger.info("Job statistics:\n" + yson.dumps(self.get_job_statistics(), yson_format="pretty"))

        stderr_level = logging.getLevelName(get_config(self.client)["operation_tracker"]["stderr_logging_level"])
        if logger.LOGGER.isEnabledFor(stderr_level):
            stderrs = get_stderrs(self.id, only_failed_jobs=False, client=self.client)
            if stderrs:
                logger.log(stderr_level, "\n" + format_operation_stderrs(stderrs))
