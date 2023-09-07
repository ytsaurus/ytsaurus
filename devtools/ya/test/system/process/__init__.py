# coding: utf-8
import os
import subprocess

import six
import yatest.common
import exts.process
import exts.windows
from yatest.common import TimeoutError, ExecutionError, ExecutionTimeoutError, SignalInterruptionError, wait_for  # noqa


def execute(
    command,
    check_exit_code=True,
    shell=False,
    timeout=None,
    cwd=None,
    env=None,
    stdin=None,
    stdout=None,
    stderr=None,
    creationflags=exts.process.subprocess_flags(),
    wait=True,
    process_progress_listener=None,
    create_new_process_group=False,
    stdout_to_stderr=False,
    on_timeout=None,
    text=False if six.PY2 else True,
):
    collect_cores = False
    check_sanitizer = False
    preexec_fn = None
    if create_new_process_group and exts.windows.on_win():
        creationflags |= subprocess.CREATE_NEW_PROCESS_GROUP
        create_new_process_group = False
    if create_new_process_group or stdout_to_stderr:
        def preexec_fn():
            if create_new_process_group:
                os.setpgrp()
            if stdout_to_stderr:
                os.dup2(2, 1)

    _locals = locals()
    _locals.pop("stdout_to_stderr")
    _locals.pop("create_new_process_group")

    return yatest.common.execute(**_locals)
