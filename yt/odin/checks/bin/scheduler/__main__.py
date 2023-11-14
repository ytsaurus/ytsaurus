from yt_odin_checks.lib.check_runner import main


def run_check(yt_client, logger, options, states):
    yt_client.get("//sys/scheduler/orchid/")
    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
