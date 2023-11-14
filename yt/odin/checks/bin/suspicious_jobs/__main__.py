from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.suspicious_jobs import run_check_impl


def run_check(yt_client, logger, options, states):
    return run_check_impl(yt_client, logger, options, states)


if __name__ == "__main__":
    main(run_check)
