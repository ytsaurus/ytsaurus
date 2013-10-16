import config
from common import require, prefix, execute_handling_sigint, get_value
from errors import YtError, YtOperationFailedError, format_error
from driver import make_request
from tree_commands import get_attribute, exists, search
from file_commands import download_file
import yt.logger as logger

import os
import dateutil.parser
import logging
from datetime import datetime
from time import sleep
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

class Timeout(object):
    """ Strategy to calculate timeout between responses while waiting operation"""
    def __init__(self, min_timeout, max_timeout, slowdown_coef):
        self.min_timeout = min_timeout
        self.max_timeout = max_timeout
        self.slowdown_coef = slowdown_coef
        self.total_time = 0.0

    def wait(self):
        def bound(val, a, b):
            return min(max(val, a), b)
        res = bound(self.total_time * self.slowdown_coef, self.min_timeout, self.max_timeout)
        self.total_time += res
        sleep(res)

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
    old_retries_count = config.http.HTTP_RETRIES_COUNT
    config.HTTP_RETRIES_COUNT = config.WAIT_OPERATION_RETRIES_COUNT

    operation_path = os.path.join(OPERATIONS_PATH, operation)
    require(exists(operation_path),
            YtError("Operation %s doesn't exist" % operation))
    state = OperationState(get_attribute(operation_path, "state"))

    config.http.HTTP_RETRIES_COUNT = old_retries_count

    return state

def get_operation_progress(operation):
    operation_path = os.path.join(OPERATIONS_PATH, operation)
    progress = get_attribute(operation_path, "progress/jobs")
    if isinstance(progress["aborted"], dict):
        progress["aborted"] = progress["aborted"]["total"]
    return progress

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
                        "operation %s jobs: %s",
                        self.operation,
                        "\t".join("{0}={1}".format(k, v) for k, v in progress.iteritems()))
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

def wait_final_state(operation, timeout, print_info, action=lambda: None):
    while True:
        state = get_operation_state(operation)
        print_info(state)
        if state.is_final():
            break
        action()
        timeout.wait()
    return state

def wait_operation(operation, timeout=None, print_progress=True, finalize=lambda state: None):
    """ Wait operation and abort operation in case of keyboard interrupt """
    if timeout is None:
        timeout = Timeout(config.WAIT_TIMEOUT / 5.0, config.WAIT_TIMEOUT, 0.1)
    print_info = PrintOperationInfo(operation) if print_progress else lambda state: None

    def wait():
        result = wait_final_state(operation, timeout, print_info)
        finalize(result)
        return result

    def abort():
        if not config.KEYBOARD_ABORT:
            return
        result = wait_final_state(operation,
                                  Timeout(1.0, 1.0, 0.0),
                                  print_info,
                                  lambda: abort_operation(operation))
        finalize(result)

    return execute_handling_sigint(wait, abort)

def get_jobs_errors(operation, limit=None):
    jobs_path = os.path.join(OPERATIONS_PATH, operation, "jobs")
    if not exists(jobs_path):
        return ""
    jobs_with_errors = filter(lambda obj: "error" in obj.attributes, search(jobs_path, "map_node", attributes=["error"]))

    output = StringIO()
    for path in jobs_with_errors[:get_value(limit, config.ERRORS_TO_PRINT_LIMIT)]:
        output.write("Host: ")
        output.write(get_attribute(path, "address"))
        output.write("\n")

        output.write("Error:\n")
        output.write(format_error(path.attributes["error"]))
        output.write("\n")

        stderr_path = os.path.join(path, "stderr")
        if exists(stderr_path):
            output.write("Stderr:\n")
            for line in download_file(stderr_path):
                output.write(line)
            output.write("\n")
        output.write("\n")
    return output.getvalue()

def get_stderrs(operation, limit=None):
    jobs_path = os.path.join(OPERATIONS_PATH, operation, "jobs")
    if not exists(jobs_path):
        return ""
    jobs_with_stderr = search(jobs_path, "map_node", object_filter=lambda obj: "stderr" in obj)

    output = StringIO()
    for path in prefix(jobs_with_stderr, get_value(limit, config.ERRORS_TO_PRINT_LIMIT)):
        output.write("Host: ")
        output.write(get_attribute(path, "address"))
        output.write("\n")

        stderr_path = os.path.join(path, "stderr")
        if exists(stderr_path):
            for line in download_file(stderr_path):
                output.write(line)
        output.write("\n\n")
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
    def __init__(self, check_result=True, print_progress=True):
        self.check_result = check_result
        self.print_progress = print_progress

    def process_operation(self, type, operation, finalization=None):
        finalization = finalization if finalization is not None else lambda state: None
        state = wait_operation(operation, print_progress=self.print_progress, finalize=finalization)
        if self.check_result and state.is_failed():
            operation_result = get_operation_result(operation)
            jobs_errors = get_jobs_errors(operation)
            raise YtOperationFailedError(
                "Operation {0} {1}!\n"
                "Operation result: {2}\n\n"
                "Failed jobs:\n{3}\n\n".format(
                    operation,
                    str(state),
                    operation_result,
                    jobs_errors))
        if config.PRINT_STDERRS:
            logger.info(get_stderrs(operation))

# TODO(ignat): Fix interaction with transactions
class AsyncStrategy(object):
    # TODO(improve this strategy)
    def __init__(self):
        self.operations = []

    def process_operation(self, type, operation, finalization):
        self.operations.append(tuple([type, operation, finalization]))

    def get_last_operation(self):
        return self.operations[-1]

config.DEFAULT_STRATEGY = WaitStrategy()
