from yt_odin_checks.lib.system_quotas import run_check_impl
from yt_odin_checks.lib.check_runner import main


def run_check(yt_client, logger, options, states):
    return run_check_impl(yt_client, logger, options, states)


if __name__ == "__main__":
    main(run_check)
