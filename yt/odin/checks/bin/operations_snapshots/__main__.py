from yt_odin_checks.lib.check_runner import main

from yt.common import date_string_to_datetime, YT_DATETIME_FORMAT_STRING
import yt.wrapper as yt

from datetime import datetime


def process_response(rsp):
    if rsp.is_ok():
        return rsp.get_result()

    error = yt.YtResponseError(rsp.get_error())
    if not error.is_resolve_error():
        raise error


def run_check(yt_client, logger, options, states):
    now = datetime.utcnow()
    operations = yt_client.list("//sys/scheduler/orchid/scheduler/operations")

    responses = {}
    batch_client = yt.create_batch_client(client=yt_client, max_batch_size=100)

    for operation in operations:
        # TODO(asaitgalin): Migrate to new operations storage scheme.
        op_path = "//sys/operations/" + operation
        responses[(operation, "events")] = batch_client.get(op_path + "/@events")
        responses[(operation, "last_successful_snapshot_time")] = \
            batch_client.get(op_path + "/@progress/last_successful_snapshot_time")

    batch_client.commit_batch()

    operations_without_snapshots = []

    for operation in operations:
        events = process_response(responses[(operation, "events")])
        last_successful_snapshot_time = process_response(
            responses[(operation, "last_successful_snapshot_time")])

        if events is None or last_successful_snapshot_time is None:
            continue

        last_successful_snapshot_time = date_string_to_datetime(last_successful_snapshot_time)

        last_running_state_time = None
        for event in reversed(events):
            if event["state"] == "running":
                last_running_state_time = date_string_to_datetime(event["time"])
                break

        if last_running_state_time is None:
            continue

        running_duration = (now - last_running_state_time).total_seconds()
        duration_without_snapshot_time = (now - last_successful_snapshot_time).total_seconds()

        threshold = options["critical_time_without_snapshot_threshold"]
        if duration_without_snapshot_time >= threshold and running_duration >= threshold:
            operations_without_snapshots.append((operation, last_successful_snapshot_time))

    logger.info("Number of long running operations without built snapshots: %d", len(operations_without_snapshots))
    if operations_without_snapshots:
        for operation, last_successful_snapshot_time in operations_without_snapshots[:10]:
            dt = datetime \
                .utcfromtimestamp(last_successful_snapshot_time) \
                .strftime(YT_DATETIME_FORMAT_STRING)

            logger.info("  Operation %s had its last snapshot built at %s", operation, dt)

        return states.UNAVAILABLE_STATE

    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
