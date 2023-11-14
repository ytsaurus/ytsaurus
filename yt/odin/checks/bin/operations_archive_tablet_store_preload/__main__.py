from yt_odin_checks.lib.check_runner import main

tables = ["job_specs", "jobs", "fail_contexts", "ordered_by_id", "stderrs", "ordered_by_start_time"]

EXAMPLE_COUNT = 3


def run_check(yt_client, logger, options, states):
    batch_client = yt_client.create_batch_client()
    query = []
    for table_name in tables:
        table = "//sys/operations_archive/" + table_name
        if not yt_client.exists(table):
            continue
        batch = []
        batch.append(batch_client.exists(table))
        batch.append(batch_client.get("{}/@in_memory_mode".format(table)))
        batch.append(batch_client.get("{}/@tablet_statistics".format(table)))
        query.append([table, batch])
    batch_client.commit_batch()

    broken = {}
    for table, responses in query:
        exist, in_memory_mode, stat = map(lambda response: response.get_result(), responses)
        if exist and in_memory_mode not in ["none", None]:
            if stat["preload_pending_store_count"] > 0 or stat["preload_failed_store_count"] > 0:
                broken[table] = {
                    "completed": stat["preload_completed_store_count"],
                    "pending": stat["preload_pending_store_count"],
                    "failed": stat["preload_failed_store_count"],
                    "total": stat["chunk_count"]
                }
    count = len(broken)
    if count > 0:
        message = str(list(broken.items())[:EXAMPLE_COUNT])
        if count > EXAMPLE_COUNT:
            message += " and {} more are not preloaded.".format(count - EXAMPLE_COUNT)
        logger.info("Tables with tablet store preload issues (only first 42 items):\n%s", dict(list(broken.items())[:42]))
        return states.UNAVAILABLE_STATE, message
    else:
        return states.FULLY_AVAILABLE_STATE, "OK"


if __name__ == "__main__":
    main(run_check)
