from yt_odin_checks.lib.check_runner import main

from yt.common import datetime_to_string, date_string_to_timestamp
from datetime import datetime, timedelta

FLAG_TEMPLATE = "//sys/admin/odin/{}_present_since_flag"
MAX_AGE = timedelta(hours=4)
AGE_THRESHOLD = datetime.utcnow() - MAX_AGE
MISSING_CHUNKS_COUNT_THRESHOLD = 1000000

STATE = {}


def run_check(yt_client, logger, options, states):
    def process_state(chunk_type):
        path = FLAG_TEMPLATE.format(chunk_type)
        count = yt_client.get("//sys/{}/@count".format(chunk_type))

        STATE[chunk_type] = {}
        STATE[chunk_type]["count"] = count
        STATE[chunk_type]["sample"] = [c for c in yt_client.list("//sys/{}".format(chunk_type), max_size=10)]

        if count >= MISSING_CHUNKS_COUNT_THRESHOLD:
            if not yt_client.exists(path):
                yt_client.create("map_node", path)
                timestamp = datetime_to_string(datetime.utcnow())
            else:
                timestamp = yt_client.get("{}/@creation_time".format(path))
            STATE[chunk_type]["timestamp"] = timestamp
        else:
            if yt_client.exists(path):
                yt_client.remove(path)

    def is_age_critical(timestamp):
        created_at = datetime.utcfromtimestamp(date_string_to_timestamp(timestamp))
        if created_at <= AGE_THRESHOLD:
            return True
        else:
            return False

    for chunk_type in ["data_missing_chunks", "parity_missing_chunks"]:
        process_state(chunk_type)

    message = {}
    for k, v in STATE.items():
        message[k] = {"count": v["count"], "threshold": MISSING_CHUNKS_COUNT_THRESHOLD}
        if "timestamp" in v:
            human_time = datetime.fromtimestamp(date_string_to_timestamp(v["timestamp"]))
            message[k]["since"] = str(human_time)
            logger.info("{} count is {} since: {}".format(k, v["count"], human_time))
        else:
            logger.info("{} count is {}".format(k, v["count"]))
        logger.info("threshold is {}".format(MISSING_CHUNKS_COUNT_THRESHOLD))
        logger.info("Sample of {}: {}".format(k, v["sample"]))

    message = str(message)

    if any(["timestamp" in v and is_age_critical(v["timestamp"]) for v in STATE.values()]):
        return states.UNAVAILABLE_STATE, message

    if any([(v["count"] >= MISSING_CHUNKS_COUNT_THRESHOLD) for v in STATE.values()]):
        return states.PARTIALLY_AVAILABLE_STATE, message

    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
