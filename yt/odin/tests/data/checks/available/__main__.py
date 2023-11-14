#!/usr/bin/env python3

from yt_odin_checks.lib.check_runner import main


def run_check(logger, yt_client, options, states):
    return states.FULLY_AVAILABLE_STATE, "Dummy check"


if __name__ == "__main__":
    main(run_check)
