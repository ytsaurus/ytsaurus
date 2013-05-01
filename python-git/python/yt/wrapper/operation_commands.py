import config
import logger
from common import require, prefix, execute_handling_sigint, get_value
from errors import YtError, YtOperationFailedError, format_error
from http import make_request
from tree_commands import get_attribute, exists, search
from file_commands import download_file

import os
from time import sleep
from cStringIO import StringIO

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


def get_operation_state(operation):
    old_timeout = config.CONNECTION_TIMEOUT
    config.CONNECTION_TIMEOUT = config.WAIT_OPERATION_CONNECTION_TIMEOUT

    operation_path = os.path.join(OPERATIONS_PATH, operation)
    require(exists(operation_path),
            YtError("Operation %s doesn't exist" % operation))
    state = OperationState(get_attribute(operation_path, "state"))
    
    config.CONNECTION_TIMEOUT = old_timeout

    return state

def get_operation_progress(operation):
    operation_path = os.path.join(OPERATIONS_PATH, operation)
    return get_attribute(operation_path, "progress/jobs")

class PrintOperationInfo(object):
    """ Caches operation state and prints info by update"""
    def __init__(self):
        self.state = None
        self.progress = None
        self.formatter = logger.OperationProgressFormatter()

    def __call__(self, operation, state):
        logger.set_formatter(self.formatter)

        if state.is_running():
            progress = get_operation_progress(operation)
            if progress != self.progress:
                if config.USE_SHORT_OPERATION_INFO:
                    logger.info(
                        "operation %s: " % operation +
                        "c={completed!s}\tf={failed!s}\tr={running!s}\tp={pending!s}".format(**progress))
                else:
                    logger.info(
                        "operation %s jobs: %s",
                        operation,
                        "\t".join("{0}={1}".format(k, v) for k, v in progress.iteritems()))
            self.progress = progress
        elif state != self.state:
            logger.info("operation %s %s", operation, state)
        self.state = state

        logger.set_formatter(logger.BASIC_FORMATTER)


def abort_operation(operation):
    """ Aborts operation """
    #TODO(ignat): remove check!?
    if not get_operation_state(operation).is_final():
        make_request("abort_op", {"operation_id": operation})

def wait_final_state(operation, timeout, print_info, action=lambda: None):
    while True:
        state = get_operation_state(operation)
        print_info(operation, state)
        if state.is_final():
            break
        action()
        timeout.wait()
    return state

def wait_operation(operation, timeout=None, print_progress=True, finalize=lambda: None):
    """ Wait operation and abort operation in case of keyboard interrupt """
    if timeout is None:
        timeout = Timeout(config.WAIT_TIMEOUT / 5.0, config.WAIT_TIMEOUT, 0.1)
    print_info = PrintOperationInfo() if print_progress else lambda operation, state: None

    def wait():
        result = wait_final_state(operation, timeout, print_info)
        finalize()
        return result

    def abort():
        if not config.KEYBOARD_ABORT:
            return
        wait_final_state(operation,
                         Timeout(1.0, 1.0, 0.0),
                         print_info,
                         lambda: abort_operation(operation))
        finalize()

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

        output.write("Stderr:\n")
        stderr_path = os.path.join(path, "stderr")
        if exists(stderr_path):
            for line in download_file(stderr_path):
                output.write(line)
        output.write("\n\n")
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
        finalization = finalization if finalization is not None else lambda: None
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
