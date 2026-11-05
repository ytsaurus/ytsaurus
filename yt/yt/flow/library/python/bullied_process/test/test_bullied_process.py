import os
import pytest
import tempfile
import time

import yatest.common

from yt.yt.flow.library.python.bullied_process import (
    BulliedProcess,
    ProcessDiedException,
    ProblemsConfig,
)


def test_simple():
    process = BulliedProcess(launch_cmd=["true"], normal_exit_is_ok=True)

    # Was not started.
    assert not process.is_running()
    with pytest.raises(RuntimeError):
        process.ensure_running()

    process.start()
    time.sleep(3)  # bash will exit in time, I hope.

    # Already finished.
    assert not process.is_running()
    with pytest.raises(RuntimeError):
        process.ensure_running()

    process.stop()


def test_crash_no_watcher():
    process = BulliedProcess(launch_cmd=["bash", "-c", "exit 42"], start_watcher_thread=False)
    with pytest.raises(yatest.common.process.ExecutionError) as excinfo:
        process.start()
        time.sleep(3)  # bash will exit in time, I hope.
        process.stop()

    assert "code 42" in str(excinfo.value)


def test_crash_watcher():
    process = BulliedProcess(launch_cmd=["bash", "-c", "exit 42"])

    start = time.time()
    with pytest.raises(ProcessDiedException) as excinfo:
        process.start()
        time.sleep(1_000)
        process.stop()
    end = time.time()

    assert end - start < 1_000
    assert "code 42" in str(excinfo.type.execution_exception)


def test_hard_stop():
    process = BulliedProcess(launch_cmd=["bash", "-c", "sleep 100000"])
    process.start()
    time.sleep(0.1)
    assert process.is_running()
    process.ensure_running()
    process.stop()


def test_soft_stop():
    def test_handled_signal(exit_code):
        process = BulliedProcess(
            launch_cmd=[
                "python3",
                "-c",
                "import signal, sys;"
                f"signal.signal(signal.SIGINT, lambda *args: sys.exit({exit_code}));"
                "signal.pause()",
            ]
        )
        process.start()
        time.sleep(3)
        process.stop(soft=True)

    # Expect process to finish gracefully. "KeyboardInterrupt" flaps are OK.
    while True:
        try:
            test_handled_signal(exit_code=0)  # Must be handled correctly
        except yatest.common.process.ExecutionError as e:
            if "KeyboardInterrupt" in str(e):
                continue  # Retry test (python may not set signal handler in time).
            raise
        break

    # Expect process to crash. But graceful finish flaps are OK.
    with pytest.raises(yatest.common.process.ExecutionError):
        # SIGINT is handled badly.
        for _ in range(10):  # 10 attempts (python may not set default signal handler in time).
            test_handled_signal(exit_code=42)

    # If process is killed with not handled SIGINT, do not raise exceptions about it.
    process = BulliedProcess(launch_cmd=["bash", "-c", "sleep 1000"])
    process.start()
    time.sleep(1)
    process.stop(soft=True)


def test_normal_process_with_problems():
    problems_config = ProblemsConfig(interval_seconds=1, problems_max_count=10)
    process = BulliedProcess(launch_cmd=["bash", "-c", "sleep 100000"], problems_config=problems_config)

    process.start()
    time.sleep(10)
    process.stop()


def test_no_freezes_when_stopped_during_problems_emulation():
    problems_config = ProblemsConfig(interval_seconds=1000000000, problems_max_count=10)
    process = BulliedProcess(launch_cmd=["bash", "-c", "sleep 100000"], problems_config=problems_config)

    start_time = time.time()
    process.start()
    time.sleep(1)
    process.stop()

    assert (time.time() - start_time) < 100


def test_set_working_directory():
    with tempfile.TemporaryDirectory() as temp_dir:
        stdout_file = os.path.join(temp_dir, "pwd_output.txt")

        process = BulliedProcess(
            launch_cmd=["pwd"],
            cwd=temp_dir,
            start_watcher_thread=False,
            stdout=stdout_file,
        )

        process.start()
        process._join_process()

        with open(stdout_file, "r") as f:
            output = f.read().strip()

        expected_dir = os.path.abspath(temp_dir)
        actual_dir = os.path.abspath(output)

        assert actual_dir == expected_dir, f"Expected working directory '{expected_dir}', but got '{actual_dir}'"


# Add parametrization to make sure that python code specified in launch_cmd is actually doing something
@pytest.mark.parametrize("emulate_problems", [False, True])
def test_emulate_soft_restart_problem(emulate_problems):
    while True:
        problems_config = ProblemsConfig(
            interval_seconds=0.1, problems_max_count=100, start_delay=1, soft_restarts=True
        )
        process = BulliedProcess(
            launch_cmd=[
                "python3",
                "-c",
                "import signal, sys, time;"
                "signal.signal(signal.SIGINT, lambda *args: sys.exit(42));"
                "time.sleep(100000000);",
            ],
            problems_config=problems_config if emulate_problems else None,
        )

        process.start()

        if emulate_problems:
            with pytest.raises(ProcessDiedException) as excinfo:
                time.sleep(10)
            if "KeyboardInterrupt" in str(excinfo.type.execution_exception):
                continue  # Retry test (python may not set signal handler in time).
            assert "code 42" in str(excinfo.type.execution_exception)
        else:
            time.sleep(5)
            process.stop()
        return


@pytest.mark.parametrize("emulate_problems", [False, True])
def test_emulate_stop_continue_problem(emulate_problems):
    problems_config = ProblemsConfig(interval_seconds=1.5, problems_max_count=100, soft_restarts=False)
    process = BulliedProcess(
        launch_cmd=[
            "python3",
            "-c",
            "import time;"
            "[exec('"
            "t = time.time();"
            "time.sleep(0.1);"
            'assert (time.time() - t) < 0.3, "LONG" + "WAIT";'
            "') for i in range(100000000)]",
        ],
        problems_config=problems_config if emulate_problems else None,
    )

    process.start()

    if emulate_problems:
        with pytest.raises(ProcessDiedException) as excinfo:
            time.sleep(1_000)
        assert "LONGWAIT" in str(excinfo.type.execution_exception)
    else:
        time.sleep(5)
        process.stop()


def test_context_manager():
    with BulliedProcess(launch_cmd=["true"], normal_exit_is_ok=True):
        time.sleep(1)

    with BulliedProcess(launch_cmd=["sleep", "100000"]):
        time.sleep(1)

    with BulliedProcess(launch_cmd=["bash", "-c", "exit 0"], start_watcher_thread=False):
        time.sleep(1)

    with BulliedProcess(launch_cmd=["bash", "-c", "exit 42"], check_exit_code=False, start_watcher_thread=False):
        time.sleep(1)

    with pytest.raises(yatest.common.process.ExecutionError) as excinfo:
        with BulliedProcess(launch_cmd=["bash", "-c", "exit 42"], start_watcher_thread=False):
            time.sleep(3)  # bash will exit in time, I hope.
    assert "code 42" in str(excinfo.value)
