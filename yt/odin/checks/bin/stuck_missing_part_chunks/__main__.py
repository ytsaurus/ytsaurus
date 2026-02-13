from datetime import datetime, timedelta, UTC

from yt.common import date_string_to_timestamp

from yt_odin_checks.lib.check_runner import main


UNAVAILABLE_AGE = timedelta(minutes=180)
WARNING_AGE = timedelta(minutes=120)

SAMPLE_PATH = "//sys/oldest_part_missing_chunks_sample"


def run_check(yt_client, logger, options, states):
    result = states.FULLY_AVAILABLE_STATE
    unavailable_threshold = datetime.now(UTC) - UNAVAILABLE_AGE
    warning_threshold = datetime.now(UTC) - WARNING_AGE

    if yt_client.exists(SAMPLE_PATH):
        chunk_ids = yt_client.list(SAMPLE_PATH)

        batch_client = yt_client.create_batch_client()
        batch_responses = {}
        for chunk_id in chunk_ids:
            batch_responses[chunk_id] = batch_client.get(f"#{chunk_id}", attributes=["part_loss_time", "vital"])
        batch_client.commit_batch()

        chunks = {}
        for chunk_id, response in batch_responses.items():
            # Chunk sample is calculated asynchronously, some chunks may be already dead.
            if response.is_ok():
                chunks[chunk_id] = response.get_result()
    else:
        logger.warning("Using full chunk list for oldest_part_missing_chunks, may have slow response time")
        response_list = yt_client.list("//sys/oldest_part_missing_chunks", attributes=["part_loss_time", "vital"])
        chunks = {str(response): response for response in response_list}

    oldest_part_loss_time_utc = None
    oldest_part_loss_chunk_id = None

    for chunk_id, chunk in chunks.items():
        if not chunk.attributes["vital"]:
            continue
        if not chunk.attributes["part_loss_time"]:
            continue
        part_loss_timestamp = date_string_to_timestamp(chunk.attributes["part_loss_time"])
        part_loss_datetime_utc = datetime.fromtimestamp(part_loss_timestamp, UTC)

        if oldest_part_loss_time_utc is None or part_loss_datetime_utc < oldest_part_loss_time_utc:
            oldest_part_loss_time_utc = part_loss_datetime_utc
            oldest_part_loss_chunk_id = chunk_id

        if part_loss_datetime_utc < warning_threshold:
            logger.info("Chunk {} has parts missing since {}".format(chunk_id, datetime.fromtimestamp(part_loss_timestamp)))

    if oldest_part_loss_time_utc is None:
        logger.info("No chunks with parts missing")
    elif oldest_part_loss_time_utc < unavailable_threshold:
        result = states.UNAVAILABLE_STATE
    elif oldest_part_loss_time_utc < warning_threshold and result != states.UNAVAILABLE_STATE:
        result = states.PARTIALLY_AVAILABLE_STATE
    else:
        logger.info(f"Oldest chunk with parts missing since {oldest_part_loss_time_utc} is {oldest_part_loss_chunk_id}")

    return result


if __name__ == "__main__":
    main(run_check)
