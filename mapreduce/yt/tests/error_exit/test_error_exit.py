#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import sys
import time

from mapreduce.yt.python.yt_stuff import yt_stuff
import yatest.common

TEST_PROGRAM = yatest.common.binary_path('mapreduce/yt/tests/error_exit/test_program/test_program')

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
        raise RuntimeError, 'Found {0} operations satisfying pattern'.format(len(result))
    return result[0]

def check_table_is_not_locked(yt_wrapper, path):
    tab_id = yt_wrapper.get(path + '/@id')
    locks = list(yt_wrapper.search(root='//sys/locks',
                                   attributes=['node_id', 'mode'],
                                   object_filter=lambda x:x.attributes.get('node_id')==tab_id))
    assert len(locks) == 0

def get_transactions_with_title(yt_wrapper, title):
    return list(yt_wrapper.search(root='//sys/transactions',
                                  attributes=['title'],
                                  object_filter=lambda x: x.attributes.get('title') == title))

def check_transaction_will_die(yt_wrapper, title, timeout):
    transactions = get_transactions_with_title(yt_wrapper, title)
    assert len(transactions) <= 1
    if len(transactions) == 0:
        return
    transaction_path = transactions[0]
    time_passed = 0
    while time_passed < timeout:
        time.sleep(0.2)
        if not yt_wrapper.exists(transaction_path):
            return
        time_passed += 0.2
    raise RuntimeError, 'Transaction {0} lives longer than {1} seconds'.format(transaction_path, timeout)

def test_abort_operations_and_transactions_on_operation_fail(yt_stuff):
    yt_wrapper = yt_stuff.get_yt_wrapper()

    yatest.common.execute(
        # Argument has no meaning for program
        # we need it to find our operation later.
        [TEST_PROGRAM, 'on_operation_fail'],
        check_exit_code=False,
        collect_cores=False,
        env={
            'YT_LOG_LEVEL': 'DEBUG',
            'MR_RUNTIME': 'YT',
            'YT_PROXY': yt_stuff.get_server(),
            'SLEEP_SECONDS': '0',
            'YT_CLEANUP_ON_TERMINATION': '1',
            'TRANSACTION_TITLE': 'test-operation-fail',
            'INPUT_TABLE': '//test-operation-fail-input',
            'OUTPUT_TABLE': '//test-operation-fail-output',
        },
    )

    operation = get_operation_by_cmd_pattern(yt_wrapper, pattern='on_operation_fail', attributes=['state'])
    assert operation.attributes['state'] == 'failed'

    check_table_is_not_locked(yt_wrapper, '//test-operation-fail-output')
    check_transaction_will_die(yt_wrapper, 'test-operation-fail', 5)

def test_abort_operations_and_transactions_on_signal(yt_stuff):
    yt_wrapper = yt_stuff.get_yt_wrapper()

    process = yatest.common.execute(
        # Argument has no meaning for program
        # we need it to find our operation later.
        [TEST_PROGRAM, 'on_signal'],
        check_exit_code=False,
        collect_cores=False,
        env={
            'YT_LOG_LEVEL': 'DEBUG',
            'MR_RUNTIME': 'YT',
            'YT_PROXY': yt_stuff.get_server(),
            'SLEEP_SECONDS': '30',
            'YT_CLEANUP_ON_TERMINATION': '1',
            'TRANSACTION_TITLE': 'test-signal',
            'INPUT_TABLE': '//test-signal-input',
            'OUTPUT_TABLE': '//test-signal-output',
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
    process.process.wait()

    check_table_is_not_locked(yt_wrapper, '//test-signal-output')
    check_transaction_will_die(yt_wrapper, 'test-signal', 5)
