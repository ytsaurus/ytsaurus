#!/usr/bin/env python3

from yt_odin_checks.lib.check_runner import main


def run_check(secrets, states):
    assert secrets["the_ultimate_question"] == 42
    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
