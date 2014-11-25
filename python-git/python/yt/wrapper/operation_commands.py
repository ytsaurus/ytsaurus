import config
from common import require, prefix, get_value
from errors import YtError, YtOperationFailedError, YtResponseError, format_error, YtTimeoutError
from driver import make_request
from keyboard_interrupts_catcher import KeyboardInterruptsCatcher
from tree_commands import get_attribute, exists, search
from file_commands import download_file
import yt.logger as logger

import os
import dateutil.parser
import logging
from datetime import datetime
from time import sleep, time
from cStringIO import StringIO

OPERATIONS_PATH = "//sys/operations"

class OperationState(object):
    """State of operation. (Simple wrapper for string name.)"""
    def __init__(self, name):
        self.name = name

    def is_final(self):
        return self.name in ["aborted", "completed", "failed"]

    def is_failed(self):
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
    """Class for proper sleeping in `WaitStrategy.process_operation`."""
    def __init__(self, min_interval, max_interval, slowdown_coef, timeout=None):
        """
        Initialise time watcher.

        :param min_interval: minimal sleeping interval
        :param max_interval: maximal sleeping interval
        :param slowdown_coef: growth coefficient of sleeping interval
        :param timeout: maximal total interval of waiting. If ``timeout`` is ``None``, time watcher wait for eternally.
        """
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.slowdown_coef = slowdown_coef
        self.total_time = 0.0
        self.timeout_time = (time() + timeout) if (timeout is not None) else None

    def _bound(self, interval):
        return min(max(interval, self.min_interval), self.max_interval)

    def _is_time_up(self, time):
        """Is passed time up?"""
        if not self.timeout_time:
            return False
        return time >= self.timeout_time

    def is_time_up(self):
        """Is time elapsed?"""
        return self._is_time_up(time())

    def wait(self):
        """Sleep proper time. If timeout occurred, wake up."""
        if self.is_time_up():
            return
        pause = self._bound(self.total_time * self.slowdown_coef)
        current_time = time()
        if self._is_time_up(current_time + pause):
            pause = self.timeout_time - current_time
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
                return time.seconds / 60 + 60 * 24 * time.days
            elapsed = total_minutes(datetime.now() - self._start_time)
            time = datetime.now()
            if time.microsecond > 0:
                time = time.isoformat(" ")[:-3]
            else:
                time = time.isoformat(" ")
            return "{0} ({1:2} min)".format(time, elapsed)


def get_operation_state(operation, client=None):
    """Return current state of operation.

    :param operation: (string) operation id.
    Raise `YtError` if operation doesn't exists
    """
    old_retry_count = config.http.REQUEST_RETRY_COUNT
    config.http.REQUEST_RETRY_COUNT = config.OPERATION_GET_STATE_RETRY_COUNT

    operation_path = os.path.join(OPERATIONS_PATH, operation)
    require(exists(operation_path, client=client), YtError("Operation %s doesn't exist" % operation))
    state = OperationState(get_attribute(operation_path, "state", client=client))

    config.http.REQUEST_RETRY_COUNT = old_retry_count

    return state

def get_operation_progress(operation, client=None):
    operation_path = os.path.join(OPERATIONS_PATH, operation)
    progress = get_attribute(operation_path, "progress/jobs", client=client)
    if isinstance(progress["aborted"], dict):
        progress["aborted"] = progress["aborted"]["total"]
    return progress

def order_progress(progress):
    keys = ["running", "completed", "pending", "failed", "aborted", "lost", "total"]
    result = []
    for key in keys:
        result.append((key, progress[key]))
    for key, value in progress.iteritems():
        if key not in keys:
            result.append((key, value))
    return result

class PrintOperationInfo(object):
    """Cache operation state and print info by update"""
    def __init__(self, operation, client=None):
        self.operation = operation
        self.state = None
        self.progress = None

        creation_time_str = get_attribute(os.path.join(OPERATIONS_PATH, self.operation), "creation_time", client=client)
        creation_time = dateutil.parser.parse(creation_time_str).replace(tzinfo=None)
        local_creation_time = creation_time + (datetime.now() - datetime.utcnow())

        self.formatter = OperationProgressFormatter(start_time=local_creation_time)

        self.client = client

    def __call__(self, state):
        logger.set_formatter(self.formatter)

        if state.is_running():
            progress = get_operation_progress(self.operation, client=self.client)
            if progress != self.progress:
                if config.USE_SHORT_OPERATION_INFO:
                    logger.info(
                        "operation %s: " % self.operation +
                        "c={completed!s}\tf={failed!s}\tr={running!s}\tp={pending!s}".format(**progress))
                else:
                    logger.info(
                        "operation %s: %s",
                        self.operation,
                        "\t".join("{0}={1}".format(k, v) for k, v in order_progress(progress)))
            self.progress = progress
        elif state != self.state:
            logger.info("operation %s %s", self.operation, state)
        self.state = state

        logger.set_formatter(logger.BASIC_FORMATTER)

def abort_operation(operation, client=None):
    """Abort operation.

    :param operation: (string) operation id.
    """
    #TODO(ignat): remove check!?
    if not get_operation_state(operation, client=client).is_final():
        make_request("abort_op", {"operation_id": operation}, client=client)

def suspend_operation(operation, client=None):
    """Suspend operation.

    :param operation: (string) operation id.
    """
    make_request("suspend_op", {"operation_id": operation}, client=client)

def resume_operation(operation, client=None):
    """Continue operation after suspending.

    :param operation: (string) operation id.
    """
    make_request("resume_op", {"operation_id": operation}, client=client)

def wait_final_state(operation, time_watcher, print_info, action=lambda: None, client=None):
    """
    Wait for final state of operation.

    If timeout occurred, abort operation and wait for final state anyway.

    :return: operation state.
    """
    while True:
        if time_watcher.is_time_up():
            abort_operation(operation, client=client)
            return wait_final_state(operation, TimeWatcher(1.0, 1.0, 0, timeout=None), print_info, client=client)

        action()

        state = get_operation_state(operation, client=client)
        print_info(state)
        if state.is_final():
            break

        time_watcher.wait()

    return state

def get_stderrs(operation, only_failed_jobs, limit=None, client=None):
    jobs_path = os.path.join(OPERATIONS_PATH, operation, "jobs")
    if not exists(jobs_path, client=client):
        return ""
    jobs_with_stderr = search(jobs_path, "map_node", object_filter=lambda obj: "stderr" in obj, attributes=["error"], client=client)
    if only_failed_jobs:
        jobs_with_stderr = filter(lambda obj: "error" in obj.attributes, jobs_with_stderr)

    output = StringIO()
    for path in prefix(jobs_with_stderr, get_value(limit, config.ERRORS_TO_PRINT_LIMIT)):
        output.write("Host: ")
        output.write(get_attribute(path, "address", client=client))
        output.write("\n")

        if only_failed_jobs:
            output.write("Error:\n")
            output.write(format_error(path.attributes["error"]))
            output.write("\n")

        try:
            stderr_path = os.path.join(path, "stderr")
            if exists(stderr_path, client=client):
                for line in download_file(stderr_path, client=client):
                    output.write(line)
            output.write("\n\n")
        except YtResponseError:
            if config.IGNORE_STDERR_IF_DOWNLOAD_FAILED:
                break
            else:
                raise

    return output.getvalue()

def get_operation_result(operation, client=None):
    operation_path = os.path.join(OPERATIONS_PATH, operation)
    result = get_attribute(operation_path, "result", client=client)
    if "error" in result:
        return format_error(result["error"])
    else:
        return result

class WaitStrategy(object):
    """Strategy synchronously wait operation, print current progress and finalize at the completion."""
    def __init__(self, check_result=True, print_progress=True, timeout=None):
        """
        :param check_result: (bool) get stderr if operation failed
        :param print_progress: (bool)
        :param timeout: (double) timeout of operation in sec. ``None`` means operation is endlessly waited for.
        """
        self.check_result = check_result
        self.print_progress = print_progress
        self.timeout = timeout

    def process_operation(self, type, operation, finalize=None, client=None):
        """Track running operation.

        If timeout occurred, raise `YtTimeoutError`.
        If operation failed, raise `YtOperationFailedError`.
        If `KeyboardInterrupt` occurred, abort operation, finalize and reraise `KeyboardInterrupt`.
        """
        finalize = finalize if finalize else lambda state: None
        time_watcher = TimeWatcher(min_interval=config.OPERATION_STATE_UPDATE_PERIOD / 5.0,
                                   max_interval=config.OPERATION_STATE_UPDATE_PERIOD,
                                   slowdown_coef=0.1, timeout=self.timeout)
        print_info = PrintOperationInfo(operation, client=client) if self.print_progress else lambda state: None

        def abort():
            state = wait_final_state(operation, TimeWatcher(1.0, 1.0, 0.0, timeout=None),
                                     print_info, lambda: abort_operation(operation, client=client), client=client)
            finalize(state)
            return state

        with KeyboardInterruptsCatcher(abort):
            state = wait_final_state(operation, time_watcher, print_info, client=client)
            timeout_occurred = time_watcher.is_time_up()
            finalize(state)
            if timeout_occurred:
                logger.info("Timeout occurred.")
                raise YtTimeoutError

        if self.check_result and state.is_failed():
            operation_result = get_operation_result(operation, client=client)
            stderrs = get_stderrs(operation, only_failed_jobs=True, client=client)
            message = "Operation {0} {1}. Result: {2}"\
                .format(operation, str(state),
                        operation_result)

            attributes = {}
            if stderrs:
                attributes["stderrs"] = stderrs
            raise YtOperationFailedError(message, attributes=attributes)

        stderr_level = logging._levelNames[config.STDERR_LOGGING_LEVEL]
        if logger.LOGGER.isEnabledFor(stderr_level):
            stderrs = get_stderrs(operation, only_failed_jobs=False, client=client)
            if stderrs:
                logger.log(stderr_level, "\n" + stderrs)


# TODO(ignat): Fix interaction with transactions
class AsyncStrategy(object):
    """Strategy just save some info about operations."""
    # TODO(improve this strategy)
    def __init__(self):
        self.operations = []

    def process_operation(self, type, operation, finalize, client=None):
        """Save operation info."""
        self.operations.append(tuple([type, operation, finalize]))

    def get_last_operation(self):
        """
        Return last operation.

        :raises `IndexError`: no operations
        """
        return self.operations[-1]

config.DEFAULT_STRATEGY = WaitStrategy()
