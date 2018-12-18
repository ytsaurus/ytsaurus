from __future__ import print_function

from . import executor
from . import scheduling

from functools import partial
import execnet
import os
import random
import string
import sys
import time
import traceback

try:
    from itertools import izip
except ImportError:  # Python 3
    izip = zip

import pytest
from _pytest import runner

try:
    xrange
except NameError:  # Python 3
    xrange = range

MAX_PROCESS_COUNT = 24
PROCESS_RESTARTS_LIMIT = 10

WRAP_PYTHON_SCRIPT = os.path.join(os.path.dirname(os.path.realpath(__file__)), "wrap_python")

def pytest_addoption(parser):
    parser.addoption("--process-count", type=int,
                     help="number of executor processes that will run tests")
    parser.addoption("--pytest-timeout", type=int, default=6 * 60 * 60,
                     help="timeout in seconds for the whole pytest execution")

def get_pytest_item_location_str(item):
    file_path, line_number, function_name = item.location
    return "{} at {}:{}".format(function_name, file_path, line_number)

class TestProcess(object):
    def __init__(self, gateway, channel):
        self.gateway = gateway
        self.channel = channel
        self.last_started_test_index = -1
        self.last_started_test_stderrs_paths = None

class YtParallelTestsRunnerPlugin(object):
    # Thread affinity: main
    def __init__(self, config, process_count, pytest_timeout):
        self.config = config
        self.terminal = self.config.pluginmanager.getplugin("terminalreporter")
        self.timeout = pytest_timeout
        self.group = execnet.Group()
        self.process_restart_count = 0
        self.processes_to_restart = []

        self.process_count = process_count
        if process_count <= 0 or process_count > MAX_PROCESS_COUNT:
            raise RuntimeError("Process count should be positive and less than or equal to {0}"
                               .format(MAX_PROCESS_COUNT))

    # Thread affinity: any
    def _log_to_terminal(self, line):
        if self.config.option.verbose > 0:
            self.terminal.write_line(line)

    # Thread affinity: main
    def _terminate_test_session(self, error):
        self._log_to_terminal("Terminating test session due to error:\n    {}".format(error))
        self.group.terminate(timeout=3)
        raise RuntimeError("Test session was terminated due to error: {}".format(error))

    # Thread affinity: main
    def _make_process(self, process_index):
        suffix = "".join(random.sample(string.ascii_letters + string.digits, 5))

        strace_file_name = os.path.join(
            os.environ.get("TESTS_SANDBOX", ""),
            "executor_{0}.strace{1}".format(process_index, suffix))

        gateway = self.group.makegateway("popen//python={0} {1} {2}".format(
            WRAP_PYTHON_SCRIPT, strace_file_name, sys.executable))
        channel = gateway.remote_exec(executor)

        return TestProcess(gateway, channel)

    # Thread affinity: main
    def _initialize_process(self, process_index):
        channel = self.processes[process_index].channel
        # Main process configuration (command-line args, etc.)
        channel.send((vars(self.config.option), self.config.args))
        # Checking that started process has the same number of tests
        msg = channel.receive()
        assert msg["type"] == "test_count"
        discovered_test_count = msg["data"]

        if discovered_test_count != len(self.session.items):
            raise RuntimeError("Process {0} has different number of tests. "
                               "Expected: {1}, actual: {2}"\
                               .format(process_index, len(self.session.items), discovered_test_count))

        # NB! Executes callback in the current thread for already queued messages.
        #     Future messages will be processed in receiver threads.
        channel.setcallback(partial(self._receive_callback, process_index), endmarker={
            "type": "endmarker",
            "data": None
        })

    # Thread affinity: any
    def _get_remaining_tasks(self, process_index):
        last_started_test_index = self.processes[process_index].last_started_test_index
        process_tasks = self.processes_tasks[process_index]
        return process_tasks[last_started_test_index + 1:]

    # Thread affinity: main
    def _restart_process_or_terminate_test_session(self, process_index):
        self._log_to_terminal("Executor {0} will be restarted".format(process_index))

        if self.process_restart_count >= PROCESS_RESTARTS_LIMIT:
            self._terminate_test_session("Executor restart count has reached the limit of {} times"
                .format(PROCESS_RESTARTS_LIMIT))

        self.process_restart_count += 1

        self.processes[process_index] = self._make_process(process_index)
        self._initialize_process(process_index)

        remaining_tasks = self._get_remaining_tasks(process_index)
        self.processes_tasks[process_index] = remaining_tasks
        self.processes[process_index].channel.send(remaining_tasks)

        self._log_to_terminal("Executor {0} was successfully restarted".format(process_index))

    # Thread affinity: any
    def _receive_callback_impl(self, process_index, event):
        if event["type"] == "report":
            report = runner.TestReport(**event["data"])
            self.session.config.hook.pytest_runtest_logreport(report=report)
        elif event["type"] == "next_test_index":
            self.processes[process_index].last_started_test_index = event["data"]
        elif event["type"] == "endmarker":
            self._handle_endmarker(process_index)
        elif event["type"] == "next_test_stderrs_paths":
            self.processes[process_index].last_started_test_stderrs_paths = event["data"]
        else:
            raise RuntimeError("Incorrect event type")

    # Thread affinity: any
    def _receive_callback(self, process_index, event):
        try:
            self._receive_callback_impl(process_index, event)
        except:
            self._log_to_terminal("Exception occurred while receiving event {} from executor {}:\n{}".format(
                event,
                process_index,
                traceback.format_exc()))
            raise

    # Thread affinity: any
    def _handle_endmarker(self, process_index):
        last_started_test_index = self.processes[process_index].last_started_test_index
        process_tasks = self.processes_tasks[process_index]

        if self._get_remaining_tasks(process_index):
            crashed_test = self.session.items[process_tasks[last_started_test_index]]
            message = "Executor {0} crashed on test {1}. For example, this can happen " \
                      "if pytest execution timed out or driver crashed or " \
                      "some service (master, scheduler, etc.) died during test session. " \
                      "See build log and core dumps for more details." \
                      .format(process_index, get_pytest_item_location_str(crashed_test))

            stderrs_paths = self.processes[process_index].last_started_test_stderrs_paths
            if stderrs_paths is not None:
                stderrs = []
                for path in stderrs_paths:
                    if not os.path.exists(path):
                        continue

                    for file_ in os.listdir(path):
                        content = open(os.path.join(path, file_)).read()
                        if content:
                            stderrs.append("{0}:\n{1}\n".format(file_, content))

                if stderrs:
                    message += "\n" + "\n".join(stderrs)

            self._log_to_terminal(message)
            report = runner.TestReport(
                nodeid=crashed_test.nodeid,
                location=crashed_test.location,
                keywords=(),
                outcome="failed",
                longrepr=message,
                when=""
            )
            self.session.config.hook.pytest_runtest_logreport(report=report)

            self._log_to_terminal("Request executor {} restart".format(process_index))
            self.processes_to_restart.append(process_index)
        else:
            self.finished_processes.append(process_index)

    # Thread affinity: main
    def pytest_runtestloop(self, session):
        self._log_to_terminal("Parallel tests runner plugin is started. "
                              "Process count: {0}, test count: {1}"
                              .format(self.process_count, len(session.items)))

        self.session = session

        self.processes_tasks = scheduling.get_scheduling_func()(session.items, self.process_count)
        for index, tasks in enumerate(self.processes_tasks):
            self._log_to_terminal("Process (id {0}) will run {1} tests".format(index, len(tasks)))

        self.finished_processes = []

        self.processes = []

        start_time = time.time()
        while True:
            if len(self.finished_processes) >= len(self.processes_tasks):
                break

            if time.time() - start_time >= self.timeout:
                self._terminate_test_session("Test session takes longer than specified timeout of {} seconds".format(self.timeout))

            while len(self.processes) - len(self.finished_processes) < self.process_count:
                if len(self.processes) >= len(self.processes_tasks):
                    break

                process_index = len(self.processes)
                self.processes.append(self._make_process(process_index))
                self._initialize_process(process_index)

                self.processes[process_index].channel.send(self.processes_tasks[process_index])

            # NB! For correctness here we need to have no more than one consumer (main loop thread in that particular case) and
            #     provided by CPython GIL atomicity of list append / pop / len function calls.
            while len(self.processes_to_restart) > 0:
                process_index = self.processes_to_restart.pop()
                self._restart_process_or_terminate_test_session(process_index)

            time.sleep(1)

        return True

@pytest.mark.trylast
def pytest_configure(config):
    process_count = config.getoption("process_count", default=None)
    pytest_timeout = config.getoption("pytest_timeout")
    if process_count is not None:
        plugin = YtParallelTestsRunnerPlugin(config, process_count, pytest_timeout)
        config.pluginmanager.register(plugin)
