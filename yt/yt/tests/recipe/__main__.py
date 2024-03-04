from yt_recipe_common import Pipe, PIPE_FROM_RUNNER_TO_RECIPE, PIPE_FROM_RECIPE_TO_RUNNER

from yt.test_helpers import get_build_root

from library.python.testing.recipe import declare_recipe

import os
import subprocess
import sys
from typing import List

# Since YTEnvSetup cannot connect to running YT environment it has to run until
# test is finished. But recipe is finished before the test starts. So recipe and
# YT environment have to be launched in separate processes. To achieve this,
# recipe starts separate process with YT env and uses POSIX FIFO to communicate
# with it.
#
# Note that YT env runner may use yatest package which requires initialization
# via command line arguments. So recipe have to pass all its arguments to runner.
#
# recipe:         yt_env_runner:    tests:
# start runner    -                 -
# wait "ready"    setup YT env      -
# wait "ready"    send "ready"      -
# exit            wait "stop"       -
# -               wait "stop"       running
# stop()          wait "stop"       -
# send "stop"     wait "stop"       -
# wait "stopped"  shutdown YT env   -
# wait "stopped"  send "stopped"    -


################################################################################


RUNNER_PIPE = Pipe(PIPE_FROM_RUNNER_TO_RECIPE, PIPE_FROM_RECIPE_TO_RUNNER)
RUNNER_PID_FILE_NAME = "runner_pid"


def remove_tmp_files() -> None:
    for f in (PIPE_FROM_RECIPE_TO_RUNNER, PIPE_FROM_RUNNER_TO_RECIPE, RUNNER_PID_FILE_NAME):
        try:
            os.remove(f)
        except OSError:
            pass


def create_pipes() -> None:
    remove_tmp_files()
    os.mkfifo(PIPE_FROM_RECIPE_TO_RUNNER)
    os.mkfifo(PIPE_FROM_RUNNER_TO_RECIPE)


def start(args: List[str]) -> None:
    create_pipes()

    # To initialize yatest we need to pass original args.
    runner_args = [os.path.join(get_build_root(), "yt/yt/tests/recipe/yt_env_runner/yt_env_runner")]
    # NB: program name and "start" are skipped.
    runner_args += sys.argv[1:]

    runner = subprocess.Popen(runner_args, start_new_session=True)

    # YT env runner's PID is used to determine if process is still alive.
    with open(RUNNER_PID_FILE_NAME, "w") as f:
        print(runner.pid, file=f)

    RUNNER_PIPE.wait("ready")


def shutdown_runner() -> None:
    # YT environment runner can be already finished or crashed. In such
    # cases try to avoid deadlock on communication with runner.
    if not os.path.exists(RUNNER_PID_FILE_NAME):
        return

    with open(RUNNER_PID_FILE_NAME, "r") as f:
        pid = int(f.readline())

    # Check process liveness.
    try:
        os.kill(pid, 0)
    except OSError:
        return

    # Shutdown YT environment gracefully.
    RUNNER_PIPE.send("stop")
    RUNNER_PIPE.wait("stopped")


def stop(args: List[str]) -> None:
    try:
        shutdown_runner()
    finally:
        remove_tmp_files()


################################################################################


if __name__ == "__main__":
    declare_recipe(start, stop)
