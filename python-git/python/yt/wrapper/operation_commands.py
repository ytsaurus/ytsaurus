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
from dateutil import tz

OPERATIONS_PATH = "//sys/operations"

class OperationState(object):
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
    """ Class for proper sleeping in WaitStrategy.process_operation """
    def __init__(self, min_interval, max_interval, slowdown_coef, timeout=None):
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.slowdown_coef = slowdown_coef
        self.total_time = 0.0
        self.timeout_time = (time() + timeout) if (timeout is not None) else None

    def _bound(self, interval):
        return min(max(interval, self.min_interval), self.max_interval)

    def _is_time_up(self, time):
        """ Is passed time up? """
        if not self.timeout_time:
            return False
        return time >= self.timeout_time

    def is_time_up(self):
        """ Is time elapsed? (Invoke _is_time_up with current time.) """
        return self._is_time_up(time())

    def wait(self):
        """ sleep proper time. If timeout occured, wake up. """
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


def get_operation_state(operation):
    old_request_timeout = config.http.REQUEST_TIMEOUT
    config.http.REQUEST_TIMEOUT = config.OPERATION_TRANSACTION_TIMEOUT

    operation_path = os.path.join(OPERATIONS_PATH, operation)
    require(exists(operation_path),
            YtError("Operation %s doesn't exist" % operation))
    state = OperationState(get_attribute(operation_path, "state"))

    config.http.REQUEST_TIMEOUT = old_request_timeout

    return state

def get_operation_progress(operation):
    operation_path = os.path.join(OPERATIONS_PATH, operation)
    progress = get_attribute(operation_path, "progress/jobs")
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
    """ Caches operation state and prints info by update"""
    def __init__(self, operation):
        self.operation = operation
        self.state = None
        self.progress = None

        creation_time_str = get_attribute(os.path.join(OPERATIONS_PATH, self.operation), "creation_time")
        creation_time = dateutil.parser.parse(creation_time_str).replace(tzinfo=None)

        creation_time = creation_time.replace(tzinfo=tz.tzutc())
        local_creation_time = creation_time.astimezone(tz.tzlocal()).replace(tzinfo=None)

        self.formatter = OperationProgressFormatter(start_time=local_creation_time)

    def __call__(self, state):
        logger.set_formatter(self.formatter)

        if state.is_running():
            progress = get_operation_progress(self.operation)
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


def abort_operation(operation):
    """ Aborts operation """
    #TODO(ignat): remove check!?
    if not get_operation_state(operation).is_final():
        make_request("abort_op", {"operation_id": operation})

def suspend_operation(operation):
    make_request("suspend_op", {"operation_id": operation})

def resume_operation(operation):
    make_request("resume_op", {"operation_id": operation})

def wait_final_state(operation, time_watcher, print_info, action=lambda: None):
    """
    Wait for final state of operation. If timeout occured, abort operation and wait for final state anyway.
    """
    while True:
        if time_watcher.is_time_up():
            abort_operation(operation)
            return wait_final_state(operation, TimeWatcher(1.0, 1.0, 0, timeout=None), print_info)

        action()

        state = get_operation_state(operation)
        print_info(state)
        if state.is_final():
            break

        time_watcher.wait()

    return state

def get_stderrs(operation, only_failed_jobs, limit=None):
    jobs_path = os.path.join(OPERATIONS_PATH, operation, "jobs")
    if not exists(jobs_path):
        return ""
    jobs_with_stderr = search(jobs_path, "map_node", object_filter=lambda obj: "stderr" in obj, attributes=["error"])
    if only_failed_jobs:
        jobs_with_stderr = filter(lambda obj: "error" in obj.attributes, jobs_with_stderr)

    output = StringIO()
    for path in prefix(jobs_with_stderr, get_value(limit, config.ERRORS_TO_PRINT_LIMIT)):
        output.write("Host: ")
        output.write(get_attribute(path, "address"))
        output.write("\n")

        if only_failed_jobs:
            output.write("Error:\n")
            output.write(format_error(path.attributes["error"]))
            output.write("\n")

        try:
            stderr_path = os.path.join(path, "stderr")
            if exists(stderr_path):
                for line in download_file(stderr_path):
                    output.write(line)
            output.write("\n\n")
        except YtResponseError:
            if config.IGNORE_STDERR_IF_DOWNLOAD_FAILED:
                break
            else:
                raise

    return output.getvalue()

def get_operation_result(operation):
    operation_path = os.path.join(OPERATIONS_PATH, operation)
    result = get_attribute(operation_path, "result")
    if "error" in result:
        return format_error(result["error"])
    else:
        return result

class WaitStrategy(object):
    """
    This strategy synchronously wait operation, print current progress and
    remove files at the completion.
    """
    def __init__(self, check_result=True, print_progress=True, timeout=None):
        self.check_result = check_result
        self.print_progress = print_progress
        self.timeout = timeout

    def process_operation(self, type, operation, finalize=None):
        """
        Run operation, wait for final state.
        If timeout occured, raise YtTimeoutError.
        If KeyboardInterrupt occured, abort operation, remove files and reraise KeyboardInterrupt.
        """
        finalize = finalize if finalize else lambda state: None
        time_watcher = TimeWatcher(config.OPERATION_STATE_UPDATE_PERIOD / 5.0, config.OPERATION_STATE_UPDATE_PERIOD, 0.1, self.timeout)
        print_info = PrintOperationInfo(operation) if self.print_progress else lambda state: None

        def abort():
            state = wait_final_state(operation, TimeWatcher(1.0, 1.0, 0.0, timeout=None), print_info, lambda: abort_operation(operation))
            finalize(state)
            return state

        with KeyboardInterruptsCatcher(abort):
            state = wait_final_state(operation, time_watcher, print_info)
            timeout_occured = time_watcher.is_time_up()
            finalize(state)
            if timeout_occured:
                logger.info("Timeout occured.")
                raise YtTimeoutError


        if self.check_result and state.is_failed():
            operation_result = get_operation_result(operation)
            stderrs = get_stderrs(operation, only_failed_jobs=True)
            message = "Operation {0} {1}.\n"\
                      "Operation result: {2}\n\n"\
                .format(operation, str(state),
                        operation_result)
            if stderrs:
                message += "Failed jobs:\n{0}\n\n".format(stderrs)
            raise YtOperationFailedError(message)

        stderr_level = logging._levelNames[config.STDERR_LOGGING_LEVEL]
        if logger.LOGGER.isEnabledFor(stderr_level):
            stderrs = get_stderrs(operation, only_failed_jobs=False)
            if stderrs:
                logger.log(stderr_level, "\n" + stderrs)


# TODO(ignat): Fix interaction with transactions
class AsyncStrategy(object):
    # TODO(improve this strategy)
    def __init__(self):
        self.operations = []

    def process_operation(self, type, operation, finalize):
        self.operations.append(tuple([type, operation, finalize]))

    def get_last_operation(self):
        return self.operations[-1]

config.DEFAULT_STRATEGY = WaitStrategy()