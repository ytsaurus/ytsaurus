from datetime import datetime, timedelta

from yt.common import date_string_to_timestamp

from yt_odin_checks.lib.check_runner import main


UNAVAILABLE_AGE = timedelta(minutes=180)
WARNING_AGE = timedelta(minutes=120)


def run_check(yt_client, logger, options, states):
    result = states.FULLY_AVAILABLE_STATE
    unavailable_threshold = datetime.utcnow() - UNAVAILABLE_AGE
    warning_threshold = datetime.utcnow() - WARNING_AGE
    for chunk in yt_client.list("//sys/oldest_part_missing_chunks", attributes=["part_loss_time"]):
        if not chunk.attributes["part_loss_time"]:
            continue
        part_loss_timestamp = date_string_to_timestamp(chunk.attributes["part_loss_time"])
        part_loss_datetime_utc = datetime.utcfromtimestamp(part_loss_timestamp)
        if part_loss_datetime_utc < warning_threshold:
            logger.info("Chunk {} has parts missing since {}".format(chunk, datetime.fromtimestamp(part_loss_timestamp)))
            if part_loss_datetime_utc < unavailable_threshold:
                result = states.UNAVAILABLE_STATE
            elif part_loss_datetime_utc < warning_threshold and result != states.UNAVAILABLE_STATE:
                result = states.PARTIALLY_AVAILABLE_STATE

    return result


if __name__ == "__main__":
    main(run_check)
