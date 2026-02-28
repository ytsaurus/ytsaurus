from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.virtual_chunk_map_helpers import check_virtual_map_age

from datetime import timedelta

DEFAULT_MAX_AGE_MINUTES = 4 * 60
DEFAULT_CHUNK_COUNT_THRESHOLD = 1000000


def run_check(yt_client, logger, options, states):
    max_age_minutes = options.get("max_age_minutes", DEFAULT_MAX_AGE_MINUTES)
    max_age = timedelta(minutes=max_age_minutes)
    chunk_count_threshold = options.get("chunk_count_threshold", DEFAULT_CHUNK_COUNT_THRESHOLD)

    messages = {}
    error_messages = {}
    for check_name in ["data_missing", "parity_missing"]:
        message = check_virtual_map_age(yt_client, logger, options, check_name)
        if message is not None:
            messages[check_name] = message
        else:
            return states.UNKNOWN_STATE

        error_messages[check_name] = {
            "count": message["count"],
            "chunk_count_threshold": chunk_count_threshold,
        }
        if "since" in message:
            error_messages[check_name]["since"] = message["since"]
            error_messages[check_name]["max_age_minutes"] = max_age_minutes

    error_messages = str(error_messages)

    if any([message["age"] is not None and message["age"] >= max_age for message in messages.values()]):
        return states.UNAVAILABLE_STATE, error_messages
    elif any([message["count"] is not None and message["count"] >= chunk_count_threshold for message in messages.values()]):
        return states.PARTIALLY_AVAILABLE_STATE, error_messages
    else:
        return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
