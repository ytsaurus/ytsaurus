import yt.wrapper as yt

import time


SHUFFLED_DATA = b"x=93555\nx=46019\nx=29585\nx=44977\nx=54943\nx=24629\nx=64884\nx=18978\nx=04137\nx=33367\n"
SORTED_DATA = b"x=04137\nx=18978\nx=24629\nx=29585\nx=33367\nx=44977\nx=46019\nx=54943\nx=64884\nx=93555\n"


def run_check_impl(yt_client, logger, options, states, cloud=False):
    temp_path = options["temp_tables_path"]
    soft_sort_timeout = options["soft_sort_timeout"]

    check_result = states.UNAVAILABLE_STATE

    if not yt_client.exists(temp_path):
        logger.info("Creating %s", temp_path)
        yt_client.mkdir(temp_path, recursive=True)

    logger.info("Creating temporary table")
    table_path = yt_client.create_temp_table(path=temp_path)
    logger.info("Created %s table", table_path)

    try:
        start_time = int(time.time())
        table_reader = {
            "workload_descriptor": {
                "category": "user_interactive",
            },
        }
        table_writer = {
            "enable_early_finish": True,
            "upload_replication_factor": 3,
            "min_upload_replication_factor": 2,
            "workload_descriptor": {
                "category": "user_interactive",
            },
            "sync_on_close": False,
        }

        yt_client.write_table(table_path, SHUFFLED_DATA, format="dsv", raw=True, table_writer=table_writer)
        spec = {
            "time_limit": 60 * 1000 * 2,
            "merge_job_io": {
                "table_reader": table_reader,
                "table_writer": table_writer,
            },
            "partition_job_io": {
                "table_reader": table_reader,
                "table_writer": table_writer,
            },
            "sort_job_io": {
                "table_reader": table_reader,
                "table_writer": table_writer,
            },
        }

        if cloud:
            spec["pool_trees"] = options.get("pool_config", {}).get(options["cluster_name"], ["cloud"])

        logger.info("Starting sort operation")
        operation = yt_client.run_sort(table_path, sort_by="x", sync=False, spec=spec)
        logger.info("Sort operation id '%s'", operation.id)

        try:
            operation.wait(print_progress=False)
        except yt.YtOperationFailedError as error:
            logger.error("Sort operation '%s' failed", operation.id)
            logger.exception(error)
        else:
            result = yt_client.read_table(table_path, format="dsv", raw=True).read()
            if result == SORTED_DATA:
                now = int(time.time())
                if now - start_time > soft_sort_timeout:
                    logger.warning("Table is sorted correctly but operation took "
                                   "more than %d seconds", soft_sort_timeout)
                    check_result = states.PARTIALLY_AVAILABLE_STATE
                else:
                    logger.info("Table is sorted correctly")
                    check_result = states.FULLY_AVAILABLE_STATE
            else:
                logger.error("Table is sorted incorrectly;\nExpected:\n%s\nReceived:\n%s", SORTED_DATA, result)
    finally:
        yt_client.remove(table_path, force=True)

    return check_result
