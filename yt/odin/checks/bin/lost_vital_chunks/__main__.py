from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.virtual_chunk_map_helpers import check_virtual_map_size


def run_check(yt_client, logger, options, states):
    return check_virtual_map_size(
        yt_client,
        logger,
        options,
        states,
        "lost_vital"
    )


if __name__ == "__main__":
    main(run_check)
