#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yatest.common

TEST_PROGRAM = yatest.common.binary_path('yt/cpp/mapreduce/tests_core_http/native/check_initialized/test_program/test_program')  # noqa


def test_check_initialized(yt_stuff):
    try:
        yatest.common.execute(
            [TEST_PROGRAM],
            check_exit_code=True,
            collect_cores=False,
            env={
                'YT_LOG_LEVEL': 'DEBUG',
                'MR_RUNTIME': 'YT',
                'YT_PROXY': yt_stuff.get_server(),
                'INITIALIZE': 'false'
            },
        )
        assert False, "The program must have been crashed"
    except Exception as e:
        message = 'NYT::Initialize() must be called prior to any operation'
        assert message in str(e), "This exception must have been raised"
