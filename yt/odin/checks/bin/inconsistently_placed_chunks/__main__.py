from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.virtual_chunk_map_helpers import check_virtual_map_age

from datetime import timedelta

DEFAULT_MAX_AGE_MINUTES = 4 * 60
DEFAULT_CHUNK_COUNT_THRESHOLD = 1000000


def run_check(yt_client, logger, options, states):
    message = check_virtual_map_age(yt_client, logger, options, "inconsistently_placed")
    if message is None:
        return states.UNKNOWN_STATE
    if message["age"] is None or message["count"] is None:
        return states.FULLY_AVAILABLE_STATE

    max_age_minutes = options.get("max_age_minutes", DEFAULT_MAX_AGE_MINUTES)
    max_age = timedelta(minutes=max_age_minutes)
    chunk_count_threshold = options.get("chunk_count_threshold", DEFAULT_CHUNK_COUNT_THRESHOLD)
    error_message = {
        "count": message["count"],
        "chunk_count_threshold": chunk_count_threshold,
    }
    if "since" in message:
        error_message["max_age_minutes"] = max_age_minutes,
        error_message["since"] = message["since"]
    error_message = str(error_message)

    if message["age"] >= max_age:
        return states.UNAVAILABLE_STATE, error_message
    elif message["count"] >= chunk_count_threshold:
        return states.PARTIALLY_AVAILABLE_STATE, error_message
    else:
        return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
