#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import sys
import time

from mapreduce.yt.python.yt_stuff import yt_stuff
import yatest.common

TEST_PROGRAM = yatest.common.binary_path('mapreduce/yt/tests/common/error_exit/test_program/test_program')

def get_operation_by_cmd_pattern(yt_wrapper, pattern, attributes=None):
    if attributes is None:
        attributes = set()
    else:
        attributes = set(attributes)
    attributes.add('spec')

    result = []
    for operation in yt_wrapper.list('//sys/operations', attributes=list(attributes)):
        cmd = operation.attributes.get('spec', {}).get('started_by', {}).get('command', [])
        if pattern in cmd:
            result.append(operation)
    if len(result) != 1:
        raise RuntimeError, "Found {0} operations satisfying pattern".format(len(result))
    return result[0]

def test_abort_transactions_on_operation_fail(yt_stuff):
    yt_wrapper = yt_stuff.get_yt_wrapper()

    yatest.common.execute(
        # Argument has no meaning for program
        # we need it to find our operation later.
        [TEST_PROGRAM, "on_operation_fail"],
        check_exit_code=False,
        collect_cores=False,
        env={
            'MR_RUNTIME': 'YT',
            'YT_PROXY': yt_stuff.get_server(),
            'SLEEP_SECONDS': '0',
            'INPUT_TABLE': "test-operation-fail-input",
            'OUTPUT_TABLE': "test-operation-fail-output",
        },
    )

    operation = get_operation_by_cmd_pattern(yt_wrapper, pattern='on_operation_fail', attributes=['state'])
    assert operation.attributes['state'] == 'failed'

    # Check that //test-output is not locked.
    yt_wrapper.create_table("//test-operation-fail-output")

def test_abort_transactions_on_signal(yt_stuff):
    yt_wrapper = yt_stuff.get_yt_wrapper()

    process = yatest.common.execute(
        # Argument has no meaning for program
        # we need it to find our operation later.
        [TEST_PROGRAM, "on_signal"],
        check_exit_code=False,
        collect_cores=False,
        env={
            'MR_RUNTIME': 'YT',
            'YT_PROXY': yt_stuff.get_server(),
            'SLEEP_SECONDS': '30',
            'INPUT_TABLE': "test-signal-input",
            'OUTPUT_TABLE': "test-signal-output",
        },
        wait=False,
    )

    while True:
        try:
            operation = get_operation_by_cmd_pattern(yt_wrapper, pattern='on_signal', attributes=['state'])
            if operation.attributes['state'] != 'running':
                continue
            break
        except RuntimeError:
            time.sleep(0.5)

    process.process.send_signal(15)

    # Check that output table is not locked.
    yt_wrapper.create_table("//test-signal-output")
