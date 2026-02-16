from datetime import datetime
import yt.common

DEFAULT_MAX_SAMPLE_SIZE = 10
DEFAULT_CHUNK_COUNT_THRESHOLD = 1000000


def get_chunk_sample(yt_client, options, check_name, logger):
    yt_client.config["proxy"]["request_timeout"] = 57000

    sample_path = f"//sys/{check_name}_chunks_sample"
    count_path = f"//sys/@{check_name}_chunk_count"
    full_list_path = f"//sys/{check_name}_chunks"

    max_sample_size = options.get("max_size", DEFAULT_MAX_SAMPLE_SIZE)

    # COMPAT(grphil): some checks have no virtual maps, some checks have no maps at all.
    if yt_client.exists(sample_path):
        chunk_ids = yt_client.list(sample_path, max_size=max_sample_size)
        chunk_count = yt_client.get(count_path)
    elif yt_client.exists(full_list_path):
        logger.warning(f"Using full chunk list for {check_name}, may have slow response time")
        chunk_ids = yt_client.list(full_list_path, max_size=max_sample_size)
        chunk_count = yt_client.get(f"{full_list_path}/@count")
    else:
        logger.warning(f"Can not load chunk list for {check_name}")
        return None, None

    return chunk_ids, int(chunk_count)


def check_virtual_map_size(yt_client, logger, options, states, check_name, count_threshold=0):
    chunk_ids, chunk_count = get_chunk_sample(yt_client, options, check_name, logger)
    if chunk_count is None:
        return states.UNKNOWN_STATE

    logger.info(f"{check_name} chunk number is %s", chunk_count)
    if chunk_count > count_threshold:
        logger.info(f"Sample of {check_name} chunks: %s", " ".join(chunk_ids))

        batch_client = yt_client.create_batch_client()
        batch_responses = {}
        for chunk_id in chunk_ids:
            batch_responses[chunk_id] = batch_client.get(f"#{chunk_id}/@last_seen_replicas")
        batch_client.commit_batch()

        for chunk_id, response in batch_responses.items():
            if response.is_ok():
                logger.info("Last seen replicas for chunk {}: {}".format(chunk_id, response.get_result()))
            else:
                logger.exception("Failed to get last seen replicas for chunk {}: '{}'".format(chunk_id, response.get_error()))
        return states.UNAVAILABLE_STATE
    else:
        return states.FULLY_AVAILABLE_STATE


def check_virtual_map_age(yt_client, logger, options, check_name):
    chunk_ids, chunk_count = get_chunk_sample(yt_client, options, check_name, logger)

    if chunk_count is None:
        return None

    logger.info(f"{check_name} chunk count is {chunk_count}")
    if chunk_count > 0:
        logger.info(f"Sample of {check_name} chunks: {" ".join(chunk_ids)}")

    flag_path = f"//sys/admin/odin/{check_name}_chunks_present_since_flag"

    count_threshold = options.get("chunk_count_threshold", DEFAULT_CHUNK_COUNT_THRESHOLD)

    if chunk_count >= count_threshold:
        if not yt_client.exists(flag_path):
            yt_client.create("map_node", flag_path)
            timestamp = yt.common.datetime_to_string(datetime.utcnow())
        else:
            timestamp = yt_client.get(f"{flag_path}/@creation_time")
    else:
        if yt_client.exists(flag_path):
            yt_client.remove(flag_path)
        return {"count": chunk_count, "age": None}

    logger.info(f"{check_name}_chunk_count is not less than {count_threshold} since {timestamp}")

    age = datetime.utcnow() - datetime.utcfromtimestamp(yt.common.date_string_to_timestamp(timestamp))

    return {"count": chunk_count, "age": age}
