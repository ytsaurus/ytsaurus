#!/usr/bin/env python3

from yt_odin_checks.lib.check_runner import main

import time


def run_check(logger, yt_client, options, states):
    time.sleep(5.0)
    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
