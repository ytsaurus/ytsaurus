from yt_odin.odinserver.common import round_down_to_minute
from yt_odin.logserver import UNKNOWN_STATE, TERMINATED_STATE, FAILED_STATE
from yt_odin.logging import TaskLoggerAdapter

from six import PY3
from six.moves import cPickle as pickle

import os
import errno
import signal
import logging
import subprocess
from copy import deepcopy

# Logger of Odin server
odin_logger = logging.getLogger("Odin")

# Logger of Odin check tasks
tasks_logger = logging.getLogger("Odin tasks")


class CheckTask(object):
    CHECK_PROCESS_JOIN_TIMEOUT = 2

    def __init__(self, id, process, timestamp, task_log_handler, timeout, service):
        self.id = id
        self.process = process
        self.start_timestamp = timestamp
        self.task_log_handler = task_log_handler
        self.timeout = timeout
        self.service = service
        self._result = None
        self.task_logger = TaskLoggerAdapter(tasks_logger, self.id)

    def get_result(self):
        return deepcopy(self._result)

    def try_process_result(self):
        returncode = self.process.poll()
        if returncode is None:
            return False

        self._result = dict(state=FAILED_STATE, duration=None, description=None)
        if returncode == 0:
            stdout = self.process.stdout.read()
            try:
                self._result = pickle.loads(stdout)
            except Exception:
                self.task_logger.error("Failed to parse check result (id: %s). Stdout: %s",
                                       self.id, repr(stdout))
            else:
                numeric_types = [float, int]
                if not PY3:
                    numeric_types.append(long)  # noqa
                if not isinstance(self._result["state"], tuple(numeric_types)):
                    self.task_logger.warning("Check returned incorrect result, state should be integer "
                                             " or floating-point number (id: %s, state: %r)",
                                             self.id, self._result["state"])
                    self._result["state"] = FAILED_STATE
                else:
                    self.task_logger.info("Check finished (id: %s, duration: %s ms)",
                                          self.id, self._result["duration"])
        else:
            # NB: this currently happens only if the check runner itself has crashed.
            self.task_logger.error("Check terminated with non-zero exit code "
                                   "(id: %s, service: %s, exit code: %d). Stderr: %s",
                                   self.id, self.service, returncode, repr(self.process.stderr.read()))

        # NB: result["description"] is used only for juggler, do not push it to the database.
        self.task_log_handler.send_control_message(task_id=self.id, state=self._result["state"],
                                                   duration=self._result["duration"])
        return True

    def terminate(self):
        if self.process.poll() is None:
            self.process.terminate()
        self.task_log_handler.send_control_message(task_id=self.id, state=TERMINATED_STATE)
        self._result = {"state": TERMINATED_STATE, "description": None, "duration": None}

    def finalize(self):
        if self.process.poll() is None:
            try:
                os.kill(self.process.pid, signal.SIGKILL)
            except OSError as err:
                if err.errno != errno.ESRCH:
                    raise

    @classmethod
    def start(cls, task_id, service, command, current_timestamp, socket_handler, stdin_arguments, timeout, env):
        rounded_timestamp = round_down_to_minute(current_timestamp)

        try:
            import prctl
            preexec_fn = lambda: prctl.set_pdeathsig(signal.SIGTERM)  # noqa
        except ImportError:
            preexec_fn = None

        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, close_fds=True,
                                   preexec_fn=preexec_fn,
                                   env=dict(os.environ, **env))
        pickle.dump(stdin_arguments, process.stdin, protocol=2)
        process.stdin.close()

        odin_logger.info("Started Odin check process (pid: %d, service: %s, command: %s, arguments: %s)",
                         process.pid, service, str(command), str(stdin_arguments))

        socket_handler.send_control_message(
            task_id=task_id,
            start_timestamp=rounded_timestamp,
            service=service,
            state=UNKNOWN_STATE)

        return cls(task_id, process, current_timestamp, socket_handler, timeout, service)
