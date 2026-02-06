from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.virtual_chunk_map_helpers import check_virtual_map_age

from datetime import timedelta

DEFAULT_MAX_AGE_MINUTES = 4 * 60
DEFAULT_CHUNK_COUNT_THRESHOLD = 1000000


def run_check(yt_client, logger, options, states):
    message = check_virtual_map_age(yt_client, logger, options, "inconsistently_placed")
    if message is None:
        return states.UNKNOWN_STATE

    max_age = timedelta(minutes=options.get("max_age_minutes", DEFAULT_MAX_AGE_MINUTES))
    chunk_count_threshold = options.get("chunk_count_threshold", DEFAULT_CHUNK_COUNT_THRESHOLD)

    if message["age"] is None or message["count"] is None:
        return states.FULLY_AVAILABLE_STATE
    if message["age"] >= max_age:
        return states.UNAVAILABLE_STATE, message
    elif message["count"] >= chunk_count_threshold:
        return states.PARTIALLY_AVAILABLE_STATE, message
    else:
        return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
