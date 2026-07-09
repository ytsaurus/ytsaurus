import logging
import os
import random
import signal
import threading
import time
import ctypes

import yatest.common

from dataclasses import dataclass, field

log = logging.getLogger("bullied_process")


@dataclass
class ProblemsConfig:
    interval_seconds: int
    problems_max_count: int
    stop_seconds: int = 5
    soft_restarts: bool = field(default=False)
    start_delay: float | None = field(default=None)


class ProcessDiedException(Exception):
    execution_exception = None

    def __init__(self):
        super(ProcessDiedException, self).__init__(self.execution_exception)


class ProcessExitedNormallyException(Exception):
    pass


class BulliedProcess(object):
    """
    BulliedProcess is a class for launching a process and doing some bullying to it.
    1) Kill and restart.
    2) Send SIGINT, wait and restart. Check graceful shutdown here.
    3) Send SIGSTOP+SIGCONT. Check random timeouts here.

    BulliedProcess is designed for using from 2 threads.
    First thread is main. It launches process via start() and stops the process via stop().
    Second thread is watcher. It monitors the process state, and raises `ProcessDiedException`
    in the main thread if the process fails unexpectedly.

    Main methods to use: start(), [kill(),] [restart(),] stop().
    stop() can be called at most once.
    Do not use kill()/restart()/send_sig*() is you are using |problems_config|.
    """

    def __init__(
        self,
        launch_cmd,
        cwd=None,
        env=None,
        check_exit_code=True,
        stdout=None,
        stderr=None,
        start_watcher_thread=True,
        normal_exit_is_ok: bool = False,
        problems_config: ProblemsConfig | None = None,
        random_generator: random.Random | None = None,
        name: str = "",  # For use in logging.
    ):
        if not start_watcher_thread:
            assert problems_config is None, "Problems config is set, but watcher thread is not started"

        # Immutable fields
        self._cmd = launch_cmd
        self._cwd = cwd
        self._cmd_str = " ".join(self._cmd)
        self._env = env or os.environ
        self._check_exit_code = check_exit_code
        self._start_watcher_thread = start_watcher_thread
        self._normal_exit_is_ok = normal_exit_is_ok
        self._problems_config = problems_config
        assert not (
            self._normal_exit_is_ok and self._problems_config
        ), "`normal_exit_is_ok` and `problems_config` are mutually exclusive"
        self._name = name if name else self._cmd_str
        # This is not an OS native id, just some random number to identify thread
        # https://docs.python.org/3/library/threading.html#threading.get_ident
        self._main_thread_id = threading.get_ident()

        # Mutable fields
        self._lock = threading.RLock()
        self._proc = None
        self._watcher_thread = None
        self._stdout = stdout
        self._stderr = stderr

        # Synchronization between watcher and main threads
        self._stop_event = threading.Event()

        self._random = random_generator or random.Random()

    def is_running(self):
        with self._lock:
            return self._proc and self._proc.running

    def ensure_running(self):
        with self._lock:
            if not self._proc:
                raise RuntimeError("Process was not launched (name: {})".format(self._name))
            if not self._proc.running:
                # Wait for saving stderr / stdout of running process.
                proc = self._join_process()
                raise ProcessExitedNormallyException(
                    "Process exited normally (name: {}).\nstderr:\n{}".format(self._name, proc.stderr)
                )

    def start(self):
        """
        Start process execution and optionally start a watcher thread to create problems
        and watch for unexpected failures

        All arguments are passed in constructor to support context manager interface
        """
        with self._lock:
            self._execute_process()

            if not self._start_watcher_thread:
                return

            assert self._watcher_thread is None

            # Register trivial but non-default signal handler, as exception from watcher thread
            # is delivered asynchronously and will be handled only if interpreter is doing some work.
            # Without this, main thread will not handle pending exceptions if it waits on some blocking
            # syscall like `sleep` or `read`/`write` from socket
            if signal.getsignal(signal.SIGUSR1) == signal.SIG_DFL:
                signal.signal(signal.SIGUSR1, handler=lambda *args: ...)

            self._watcher_thread = threading.Thread(
                target=self._start_watcher,
                daemon=True,
            )
            self._watcher_thread.start()
            log.info("Process watcher started (name: %s, problems_config: %s)", self._name, self._problems_config)

    def stop(self, soft=False, gen_coredump=False):
        """
        Kill and check that state is valid. Always guarantees that process is killed, even if it raises exception.
        soft - if True, try to kill with SIGINT.
        gen_coredump - if True, try to kill with SIGQUIT and, if it works, throw exception
        """
        log.info(
            "Stop process (name: %s, soft: %r, gen_coredump: %r)",
            self._name,
            soft,
            gen_coredump,
        )
        if self._stop_event.is_set():
            return
        self._stop_event.set()
        if self._watcher_thread is not None:
            self._watcher_thread.join()
            self._watcher_thread = None
        with self._lock:
            if self.is_running():
                self.kill(soft=soft, gen_coredump=gen_coredump)
            elif self._proc:
                self._join_process()

    def kill(self, soft=False, gen_coredump=False):
        assert not (soft and gen_coredump)
        if soft:
            signal_number = signal.SIGINT
        elif gen_coredump:
            signal_number = signal.SIGQUIT
        else:
            signal_number = signal.SIGKILL
        with self._lock:
            if self._proc is not None:
                self.ensure_running()
                self.send_sigcont()
                os.kill(self._proc.process.pid, signal_number)
                self._join_process(killed_with_signal=signal_number)

    def restart(self, soft=False, dont_start_stopped=False):
        log.debug("Trying to restart (name: %s, soft=%s, dont_start_stopped=%s)", self._name, soft, dont_start_stopped)
        with self._lock:
            if self.is_running() or not dont_start_stopped:
                log.debug("Do restart (name: %s, soft=%s, dont_start_stopped=%s)", self._name, soft, dont_start_stopped)
                self.kill(soft)
                self._execute_process()

    def send_sigstop(self):
        with self._lock:
            if self.is_running():
                log.debug("Send SIGSTOP (name: %s)", self._name)
                os.kill(self._proc.process.pid, signal.SIGSTOP)

    def send_sigcont(self):
        with self._lock:
            if self.is_running():
                log.debug("Send SIGCONT (name: %s)", self._name)
                os.kill(self._proc.process.pid, signal.SIGCONT)

    def _execute_process(self):
        assert self._proc is None
        self._proc = yatest.common.execute(
            self._cmd, cwd=self._cwd, stdout=self._stdout, stderr=self._stderr, env=self._env, wait=False
        )
        log.info(
            "Bullied process launched (name: %s, pid: %d, cmd: %r, cwd: %r)",
            self._name,
            self._proc.process.pid,
            self._cmd,
            self._cwd
        )

    def _start_watcher(self):
        try:
            if self._problems_config is not None:
                self._emulate_problems_unsafe()
            log.debug(
                "Watching the process, problems emulation is disabled (NormalExitIsOK: %d)", self._normal_exit_is_ok
            )
            try:
                self._checking_wait()
            except ProcessExitedNormallyException:
                if self._normal_exit_is_ok:
                    log.info("Process exited normally (name: %s)", self._name)
                else:
                    raise
        except Exception as exc:
            log.info("Watcher caught an exception. Sending to main thread (name: %s)", self._name)
            self._send_exception_to_main_thread(exc)

    def _send_exception_to_main_thread(self, exception):
        exception_type = type(
            "SpecificProcessDiedException", (ProcessDiedException,), {"execution_exception": exception}
        )
        threads_modified = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(self._main_thread_id), ctypes.py_object(exception_type)
        )

        if threads_modified > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(self._main_thread_id), 0)

            error_message = "SetAsyncExc unexpectedly modified more than one thread"
            log.fatal(error_message)
            raise RuntimeError(error_message)

        signal.pthread_kill(self._main_thread_id, signal.SIGUSR1)

    def _checking_wait(self, timeout=None):
        start = time.time()
        while not self._stop_event.is_set() and (timeout is None or time.time() < start + timeout):
            self.ensure_running()
            time.sleep(0.1)

    def _emulate_problems_unsafe(self):
        assert self._problems_config is not None

        class TimeInterval:
            def __init__(self, interval_seconds):
                self.interval_end = time.time() + interval_seconds

            def get_rem_time(self):
                return max(self.interval_end - time.time(), 0)

        if self._problems_config.start_delay is not None:
            self._checking_wait(self._problems_config.start_delay)

        problems_left = self._problems_config.problems_max_count

        while self.is_running() and problems_left > 0:
            interval = TimeInterval(self._problems_config.interval_seconds)
            self._checking_wait(self._random.random() * interval.get_rem_time())
            # Equal probability for soft restart, hard restart and stop-continue.
            action = self._random.randint(0, 2)
            if action == 0:
                self.restart(soft=False, dont_start_stopped=True)
            elif action == 1:
                if not self._problems_config.soft_restarts:
                    continue
                self.restart(soft=True, dont_start_stopped=True)
            else:
                self.send_sigstop()
                self._checking_wait(
                    min(interval.get_rem_time(), self._random.random() * self._problems_config.stop_seconds)
                )
                self.send_sigcont()
            self._checking_wait(interval.get_rem_time())
            problems_left -= 1

        if not self.is_running() and problems_left > 0:
            with self._lock:
                if not self._stop_event.is_set():
                    assert self._proc is not None
                    self._join_process()
        log.info(
            "Problems emulation finished normally (name: %s, is_running: %r, remained_problems_count: %r)",
            self._name,
            self.is_running(),
            problems_left,
        )
        log.info("Exit problem emulator for process (name: %s)", self._name)

    def _join_process(self, killed_with_signal=None):
        proc = self._proc
        try:
            check_exit_code = self._check_exit_code or killed_with_signal is not None
            proc.wait(check_exit_code=check_exit_code)
        except yatest.common.ExecutionError as e:
            exit_code = e.execution_result.exit_code
            log.info(
                "Process finished with error (code_type: %r, code: %r)",
                type(exit_code),
                exit_code,
            )
            if killed_with_signal is None:
                raise
            # Now killed_with_signal is not None, so exit code was forcefully checked.
            if exit_code != -killed_with_signal:
                # Process hadn't died because of sent signal. So raise if exit code checking is enabled.
                if self._check_exit_code:
                    raise
            else:
                # Process was killed by signal, raise if this signal is SIGQUIT.
                if exit_code == signal.SIGQUIT:
                    raise
        finally:
            self._proc = None
        return proc

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
