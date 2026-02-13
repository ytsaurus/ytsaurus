from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.virtual_chunk_map_helpers import check_virtual_map_age

from datetime import timedelta

DEFAULT_MAX_AGE_MINUTES = 4 * 60
DEFAULT_CHUNK_COUNT_THRESHOLD = 1000000


def run_check(yt_client, logger, options, states):
    messages = {}
    for check_name in ["data_missing", "parity_missing"]:
        message = check_virtual_map_age(yt_client, logger, options, check_name)
        if message is not None:
            messages[check_name] = message
        else:
            return states.UNKNOWN_STATE

    max_age = timedelta(minutes=options.get("max_age_minutes", DEFAULT_MAX_AGE_MINUTES))
    chunk_count_threshold = options.get("chunk_count_threshold", DEFAULT_CHUNK_COUNT_THRESHOLD)

    if any([message["age"] is not None and message["age"] >= max_age for message in messages.values()]):
        return states.UNAVAILABLE_STATE, messages
    elif any([message["count"] is not None and message["count"] >= chunk_count_threshold for message in messages.values()]):
        return states.PARTIALLY_AVAILABLE_STATE, messages
    else:
        return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
