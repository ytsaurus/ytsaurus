#!/usr/bin/env python3

from yt_odin_checks.lib.check_runner import main


# The following check has invalid signature.
def run_check(yt_client, logger, states, options=None):
    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
