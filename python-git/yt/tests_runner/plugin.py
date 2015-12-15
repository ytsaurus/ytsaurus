import executor
import scheduling

import time
import execnet
from functools import partial

import pytest
from _pytest import runner

MAX_PROCESS_COUNT = 24

def pytest_addoption(parser):
    parser.addoption("--process-count", type=int,
                     help="number of executor processes that will run tests")

class TestProcess(object):
    def __init__(self, gateway, channel):
        self.gateway = gateway
        self.channel = channel
        self.last_started_test_index = -1

class YtParallelTestsRunnerPlugin(object):
    def __init__(self, config, process_count):
        self.config = config
        self.terminal = self.config.pluginmanager.getplugin("terminalreporter")
        self.group = execnet.Group()

        self.process_count = process_count

        if process_count <= 0 or process_count > MAX_PROCESS_COUNT:
            raise RuntimeError("Process count should be positive and less than {0}"
                               .format(MAX_PROCESS_COUNT))

    def _log_to_terminal(self, line):
        if self.config.option.verbose > 0:
            self.terminal.write_line(line)

    def _make_process(self):
        gateway = self.group.makegateway()
        channel = gateway.remote_exec(executor)
        return TestProcess(gateway, channel)

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

        channel.setcallback(partial(self._receive_callback, process_index), endmarker={
            "type": "endmarker",
            "data": None
        })

    def _receive_callback(self, process_index, event):
        if event["type"] == "report":
            report = runner.TestReport(**event["data"])
            self.session.config.hook.pytest_runtest_logreport(report=report)
        elif event["type"] == "next_test_index":
            self.processes[process_index].last_started_test_index = event["data"]
        elif event["type"] == "endmarker":
            self._handle_endmarker(process_index)
        else:
            raise RuntimeError("Incorrect event type")

    def _restart_process(self, process_index):
        self.processes[process_index] = self._make_process()
        self._initialize_process(process_index)

    def _handle_endmarker(self, process_index):
        last_started_test_index = self.processes[process_index].last_started_test_index
        process_tasks = self.processes_tasks[process_index]

        remaining_tasks = process_tasks[last_started_test_index + 1:]

        if remaining_tasks:
            crashed_test = self.session.items[process_tasks[last_started_test_index]]
            message = "Executor {0} crashed on test {1}. For example, this can happen " \
                      "if driver crashed or some service (master, scheduler, etc.) " \
                      "died during test session. See build log and core dumps for more details." \
                      .format(process_index, crashed_test)

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

            self._log_to_terminal("Executor {0} will be restarted".format(process_index))
            self._restart_process(process_index)

            self.processes_tasks[process_index] = remaining_tasks
            self.processes[process_index].channel.send(remaining_tasks)
            self._log_to_terminal("Executor {0} was successfully restarted".format(process_index))
        else:
            self.finished_processes.append(process_index)

    def pytest_runtestloop(self, session):
        self._log_to_terminal("Parallel tests runner plugin is started. "
                              "Process count: {0}, test count: {1}"
                              .format(self.process_count, len(session.items)))

        self.session = session
        self.finished_processes = []

        self.processes_tasks = scheduling.get_scheduling_func()(session.items, self.process_count)
        for index, tasks in enumerate(self.processes_tasks):
            self._log_to_terminal("Process (id {0}) will run {1} tests".format(index, len(tasks)))

        self.processes = []
        for process_index in xrange(self.process_count):
            self.processes.append(self._make_process())
            self._initialize_process(process_index)

        for process, tasks in zip(self.processes, self.processes_tasks):
            process.channel.send(tasks)

        while len(self.finished_processes) < self.process_count:
            time.sleep(0.1)

        return True

@pytest.mark.trylast
def pytest_configure(config):
    process_count = config.getoption("process_count", default=None)
    if process_count is not None:
        plugin = YtParallelTestsRunnerPlugin(config, process_count)
        config.pluginmanager.register(plugin)

