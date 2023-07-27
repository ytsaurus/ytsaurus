from yt.tools.scheduler_simulator.scripts.lib import (
    set_default_config,
    create_default_parser,
    compose_path_templated,
    is_operation_started_event,
    is_job_aborted_event,
    extract_timeframe,
    print_info,
    time_filter
)

import yt.wrapper as yt

from functools import partial


def extract_pool_and_preemption_info(row):
    if is_operation_started_event(row):
        yield {
            "event_type": row["event_type"],
            "timestamp": row["timestamp"],
            "operation_id": row["operation_id"],
            "pool": row["pool"] if "pool" in row else "research",
        }
    elif is_job_aborted_event(row):
        if "preempted_for" in row:
            yield {
                # Here we add a symbol with a large code so that the "operation_started" events go first after sort.
                "event_type": "~" + row["event_type"],
                "timestamp": row["timestamp"],
                "preempted_operation_id": row["operation_id"],
                "preemptor_operation_id": row["preempted_for"]["operation_id"],
                "operation_id": row["operation_id"],  # reduce key
            }
    return


def reduce_preempted_pool(key, rows):
    pool = None
    for row in rows:
        if is_operation_started_event(row):
            pool = row["pool"]
            yield row
        else:
            assert row["event_type"] == "~job_aborted"
            row["preempted_pool"] = pool
            row["operation_id"] = row["preemptor_operation_id"]
            yield row


def reduce_preemptor_pool(key, rows):
    pool = None
    for row in rows:
        if is_operation_started_event(row):
            pool = row["pool"]
        else:
            assert row["event_type"] == "~job_aborted"
            row["event_type"] = "job_aborted"
            row["preemptor_pool"] = pool
            del row["operation_id"]
            yield row


def create_filtering_mapper(filters=()):
    def mapper(row):
        if not all(predicate(row) for predicate in filters):
            return
        yield row
    return mapper


def main():
    set_default_config(yt.config)

    parser = create_default_parser({
        "description": "Extract preemption info by leaf pools",
        "name": "preemption",
    })

    args = parser.parse_args()
    compose_path = partial(compose_path_templated, args)
    yt.config["remote_temp_tables_directory"] = args.output_dir

    filters = []
    if args.filter_by_timestamp:
        lower_limit, upper_limit = extract_timeframe(args)
        print_info("Timeframe: {} UTC - {} UTC".format(lower_limit, upper_limit))
        filters.append(partial(time_filter, lower_limit, upper_limit))

    preemption_events_table = compose_path("events")

    yt.run_map_reduce(
        extract_pool_and_preemption_info,
        reduce_preempted_pool,
        args.input,
        preemption_events_table,
        reduce_by=["operation_id"],
        sort_by=["operation_id", "event_type", "timestamp"],
    )

    yt.run_map_reduce(
        None,
        reduce_preemptor_pool,
        preemption_events_table,
        preemption_events_table,
        reduce_by=["operation_id"],
        sort_by=["operation_id", "event_type", "timestamp"],
    )

    yt.run_map(
        create_filtering_mapper(filters),
        preemption_events_table,
        preemption_events_table,
    )

    yt.run_sort(
        preemption_events_table,
        preemption_events_table,
        sort_by=["timestamp"],
    )


if __name__ == "__main__":
    main()
