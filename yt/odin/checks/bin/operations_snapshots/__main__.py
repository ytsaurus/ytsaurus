import logging
from datetime import datetime
from typing import Iterable, Any
from itertools import islice

import yt.wrapper as yt
from yt.common import date_string_to_datetime, YT_DATETIME_FORMAT_STRING
from yt.wrapper.batch_client import BatchClient

from yt_odin_checks.lib.check_runner import main


BATCH_SIZE = 50


# TODO: replace with itertools.batched when python is updated to 3.12.
def batched(iterable: Iterable[Any], batch_size: int):
    it = iter(iterable)
    while True:
        batch = tuple(islice(it, batch_size))
        if not batch:
            return
        yield batch


def process_operation_batch(
        batched_operations: Iterable[str],
        batch_client: BatchClient,
        options: dict[str, Any],
        logger: logging.Logger,
        now: datetime,
) -> list[tuple[str, Any]]:
    operations_without_snapshots = []

    responses = {}

    for operation in batched_operations:
        args = {
            "operation_id": operation,
            "attributes": ["events", "progress"],
        }
        responses[operation] = batch_client.get_operation(**args)

    batch_client.commit_batch()

    for operation in batched_operations:
        op_resp = responses[operation]

        if not op_resp.is_ok():
            error = yt.YtResponseError(op_resp.get_error())
            if error.is_resolve_error():
                logger.warning("Error resolving path for operation (operation_id: %s, error: %s)", operation, error)
                continue
            else:
                raise error

        result = op_resp.get_result()
        try:
            events = result["events"]
            last_successful_snapshot_time = result["progress"]["last_successful_snapshot_time"]
        except KeyError as err:
            logger.warning("Error getting operation info (operation_id: %s, error: %s)", operation, err)
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

    return operations_without_snapshots


def run_check(yt_client, logger, options, states):
    now = datetime.utcnow()
    operations = [
        op for op in yt_client.list("//sys/scheduler/orchid/scheduler/operations")
        if not op.startswith("*")
    ]
    batch_client = yt.create_batch_client(client=yt_client, max_batch_size=100)
    operations_without_snapshots = []

    batch_size = options.get("batch_size", BATCH_SIZE)

    for batched_operations in batched(operations, batch_size):
        operations_without_snapshots += process_operation_batch(
            batched_operations=batched_operations,
            batch_client=batch_client,
            options=options,
            logger=logger,
            now=now,
        )

    logger.info("Number of long running operations without built snapshots: %d", len(operations_without_snapshots))
    if operations_without_snapshots:
        for operation, last_successful_snapshot_time in operations_without_snapshots[:10]:
            dt = last_successful_snapshot_time.strftime(YT_DATETIME_FORMAT_STRING)
            logger.info("  Operation %s had its last snapshot built at %s", operation, dt)

        return states.UNAVAILABLE_STATE

    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
