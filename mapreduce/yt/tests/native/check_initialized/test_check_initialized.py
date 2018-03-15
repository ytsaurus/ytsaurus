#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import sys
import time

from mapreduce.yt.python.yt_stuff import yt_stuff
import yatest.common

TEST_PROGRAM = yatest.common.binary_path('mapreduce/yt/tests/check_initialized/test_program/test_program')

def test_check_initialized(yt_stuff):
    yt_wrapper = yt_stuff.get_yt_wrapper()
    try:
        process = yatest.common.execute(
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
        assert 'NYT::Initialize() must be called prior to any operation' in str(e), "This exception must have been raised"
