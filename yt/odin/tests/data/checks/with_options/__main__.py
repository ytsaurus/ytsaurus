#!/usr/bin/env python3

from yt_odin_checks.lib.check_runner import main


def run_check(logger, yt_client, options, states):
    assert options["key"] == "value"
    temp_table = yt_client.create_temp_table()
    yt_client.remove(temp_table)
    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
