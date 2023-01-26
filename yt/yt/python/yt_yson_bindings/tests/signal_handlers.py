from yt.testlib import authors

import yatest.common

import pytest


class TestSignalHandlers(object):
    TEST_PROGRAM_PATH = "yt/yt/python/yt_yson_bindings/tests/test_program/test_program"
    TEST_PROGRAM = yatest.common.binary_path(TEST_PROGRAM_PATH)

    @authors("aleexfi")
    @pytest.mark.parametrize("custom_signal_handler", [False, True])
    def test_override_signal_handler(self, custom_signal_handler):
        process = yatest.common.execute(
            [self.TEST_PROGRAM],
            check_exit_code=False,
            collect_cores=False,
            timeout=5,
            env={
                "USE_CUSTOM_SIGNAL_HANDLER": str(int(custom_signal_handler)),
            }
        )

        if custom_signal_handler:
            # custom user signal handler
            expected_str = "SIGSEGV handled"
        else:
            # yt/yt/core default signal handler
            expected_str = "*** Terminating ***"

        assert expected_str in process.stderr.decode("UTF-8")
