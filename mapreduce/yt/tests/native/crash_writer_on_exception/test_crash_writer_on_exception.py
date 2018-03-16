#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from mapreduce.yt.python.yt_stuff import yt_stuff
import yatest.common

TEST_PROGRAM = yatest.common.binary_path('mapreduce/yt/tests/native/crash_writer_on_exception/test_program/test_program')

@pytest.mark.parametrize("throw_exception,expected_exit_code", [(True, 0), (False, -6)])
def test_crashes(yt_stuff, throw_exception, expected_exit_code):
    yt_wrapper = yt_stuff.get_yt_wrapper()

    result = yatest.common.execute(
        # Argument has no meaning for program
        # we need it to find our operation later.
        [TEST_PROGRAM],
        check_exit_code=False,
        collect_cores=False,
        env={
            'YT_LOG_LEVEL': 'DEBUG',
            'YT_PROXY': yt_stuff.get_server(),
            "THROW_EXCEPTION": str(throw_exception),
        },
    )
    assert result.exit_code == expected_exit_code
