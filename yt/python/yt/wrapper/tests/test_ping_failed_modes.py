# -*- coding: utf-8 -*-

from __future__ import print_function

from .conftest import authors
from .helpers import set_config_option, dumps_yt_config, get_test_file_path, get_python

import yt.wrapper as yt

try:
    from yt.packages.six import PY3
except ImportError:
    from six import PY3

import yt.subprocess_wrapper as subprocess

from flaky import flaky
import pytest

import os
import sys
import time


get_time = time.monotonic if hasattr(time, 'monotonic') else time.time


def reproduce_transaction_loss(
    must_interrupt_sleep=False,
    expects_exception=False,
    delay_time=1.5,
    wait_time=5.0,
    proxy_request_timeout=0.1,
):
    with set_config_option("transaction_timeout", int(proxy_request_timeout * 1000)):
        client = yt.YtClient(token=yt.config["token"], config=yt.config.config)
        tx_context_manager = client.Transaction(
            ping_period=0,
            ping_timeout=proxy_request_timeout * 1000)
        tx = tx_context_manager.__enter__()
        time.sleep(delay_time)

        wait_begin = get_time()
        wait_end = wait_begin + wait_time
        first_exception = None
        first_sleep_duration = wait_time
        aborted = False

        while True:
            # Since aborted transaction may cause multiple interruptions,
            # we sleep for wait_time catching all exceptions,
            # and then reraise the first caught one.
            try:
                if not aborted:
                    aborted = True
                    yt.YtClient(token=yt.config["token"], config=yt.config.config).abort_transaction(tx.transaction_id)

                remaining_wait_time = wait_end - get_time()
                if remaining_wait_time <= 0:
                    if expects_exception:
                        exception_raised_in_time = True
                        if first_exception is None:
                            try:
                                # Give some additional time for this case.
                                time.sleep(10.0)
                            except BaseException as exception:
                                first_exception = exception
                            else:
                                exception_raised_in_time = False
                    break
                time.sleep(min(remaining_wait_time, 0.1))
            except BaseException as exception:
                if first_exception is None:
                    first_exception = exception
                    first_sleep_duration = get_time() - wait_begin

        if expects_exception:
            assert first_exception is not None
            assert exception_raised_in_time, "Exception has not raised in time"

        try:
            tx_context_manager.__exit__(*sys.exc_info())
        except yt.errors.YtNoSuchTransaction:
            pass

        if must_interrupt_sleep:
            assert first_sleep_duration < 0.9 * wait_time, (first_sleep_duration, wait_time)

        if first_exception:
            raise first_exception


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestPingFailedModes(object):
    @authors("marat-khalili")
    def test_invalid_value(self):
        with set_config_option("ping_failed_mode", "invalid_value"):
            with pytest.raises(yt.YtError):
                yt.Transaction()

    @authors("marat-khalili")
    def test_call_function_missing(self):
        with set_config_option("ping_failed_mode", "call_function"):
            with pytest.raises(yt.YtError):
                yt.Transaction()

    @authors("marat-khalili")
    def test_call_function_invalid(self):
        with set_config_option("ping_failed_mode", "call_function"), set_config_option("ping_failed_function", 1):
            with pytest.raises(yt.YtError):
                yt.Transaction()

    @authors("marat-khalili")
    def test_call_function(self):
        called = []
        assert not called

        def func():
            called.append(True)
        with set_config_option("ping_failed_mode", "call_function"), set_config_option("ping_failed_function", func):
            reproduce_transaction_loss()
        assert called

    @authors("ignat")
    def test_interrupt_main(self):
        if not PY3:
            pytest.skip()
        env = {
            "YT_CONFIG_PATCHES": dumps_yt_config(),
            "PYTHONPATH": os.environ["PYTHONPATH"],
        }
        binary = get_test_file_path("main_interrupted_by_ping_failed.py")
        process = subprocess.Popen(
            [get_python(), binary, "--exit-code", "42"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        assert process.returncode == 42, "Stdout:\n{}\nStderr:\n{}".format(stdout, stderr)

    @authors("marat-khalili")
    def test_pass(self):
        with set_config_option("ping_failed_mode", "pass"):
            reproduce_transaction_loss()

    @authors("marat-khalili")
    @flaky(max_runs=3)
    def test_send_signal(self):
        # YT-16628: logging cannot properly handle signals that can lead to deadlock.
        if not PY3:
            pytest.skip()
        with set_config_option("ping_failed_mode", "send_signal"):
            with pytest.raises(yt.YtTransactionPingError):
                reproduce_transaction_loss(expects_exception=True, must_interrupt_sleep=True)
