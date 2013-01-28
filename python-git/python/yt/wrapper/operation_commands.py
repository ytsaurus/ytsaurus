import config
import logger
from common import require, YtError, YtOperationFailedError, prefix, dump_to_json, execute_handling_sigint
from http import make_request
from tree_commands import get_attribute, exists, search, get
from file_commands import download_file

import os
from time import sleep

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
    operation_path = os.path.join(OPERATIONS_PATH, operation)
    require(exists(operation_path),
            YtError("Operation %s doesn't exist" % operation))
    return OperationState(get_attribute(operation_path, "state"))

def get_operation_progress(operation):
    operation_path = os.path.join(OPERATIONS_PATH, operation)
    return get_attribute(operation_path, "progress/jobs")

class PrintOperationInfo(object):
    """ Caches operation state and prints info by update"""
    def __init__(self):
        self.state = None
        self.progress = None

    def __call__(self, operation, state):
        if state.is_running():
            progress = get_operation_progress(operation)
            if progress != self.progress:
                if config.USE_SHORT_OPERATION_INFO:
                    logger.info(
                        "operation %s: " % operation +
                        "c={completed!s}\tf={failed!s}\tr={running!s}\tp={pending!s}".format(**progress))
                else:
                    logger.info(
                        "jobs of operation %s: %s",
                        operation,
                        "\t".join("{0}={1}".format(k, v) for k, v in progress.iteritems()))
            self.progress = progress
        elif state != self.state:
            logger.info("operation %s %s", operation, state)
        self.state = state


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

def get_operation_stderr(operation, limit=None):
    if limit is None: limit = config.ERRORS_TO_PRINT_LIMIT
    jobs_path = os.path.join(OPERATIONS_PATH, operation, "jobs")
    if not exists(jobs_path):
        return ""
    stderr_paths = search(jobs_path, "file", path_filter=lambda path: path.endswith("stderr"))
    return "\n\n".join("".join(download_file(path))
                       for path in stderr_paths[:config.ERRORS_TO_PRINT_LIMIT])

def get_operation_result(operation):
    operation_path = os.path.join(OPERATIONS_PATH, operation)
    return get_attribute(operation_path, "result")

def get_jobs_errors(operation, limit=None):
    #def format_error(error):
    #    return "{0}\n{1}".format(
    #                error["message"],
    #                "\n".join("{0}: {1}".format(k, v)
    #                    for k, v in error.iteritems()
    #                    if k != "message"))

    if limit is None: limit = config.ERRORS_TO_PRINT_LIMIT
    jobs_path = os.path.join(OPERATIONS_PATH, operation, "jobs")
    if not exists(jobs_path):
        return ""
    jobs = get(jobs_path, attributes=["error"])
    errors = filter(None, [job.attributes.get("error") for job in jobs.values()])
    return "\n\n".join(map(dump_to_json, prefix(errors, limit)))


""" Strategy represents actions for processing already ran operation."""
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
            stderr = get_operation_stderr(operation)
            raise YtOperationFailedError(
                "Operation {0} failed!\n"
                "Operation result: {1}\n\n"
                "JOB RESULTS:\n{2}\n\n"
                "STDERRORS:\n{3}\n\n".format(
                    operation,
                    operation_result,
                    jobs_errors,
                    stderr))
        if config.PRINT_STDERRS:
            logger.info(get_operation_stderr(operation))

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
