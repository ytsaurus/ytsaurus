from yt_odin_checks.lib.check_runner import main

import time


DEFAULT_TEMP_TABLES_PATH = "//sys/admin/odin/write_static_table_commands"
TABLE_EXPIRATION_TIMEOUT_MILLISECONDS = 30 * 60 * 1000


def _create_temp_table(yt_client, logger, temp_tables_path):
    if not yt_client.exists(temp_tables_path):
        logger.info('Creating "%s"', temp_tables_path)
        yt_client.mkdir(temp_tables_path, recursive=True)

    table_path = yt_client.create_temp_table(
        path=temp_tables_path,
        expiration_timeout=TABLE_EXPIRATION_TIMEOUT_MILLISECONDS,
    )
    logger.info("Created temporary table %s", table_path)
    return table_path


def run_check(yt_client, logger, options, states):
    temp_tables_path = options.get("temp_tables_path", DEFAULT_TEMP_TABLES_PATH)
    row = {
        "cluster": options.get("cluster_name", "unknown"),
        "timestamp": int(time.time()),
    }
    table_path = None
    rows = None

    try:
        table_path = _create_temp_table(yt_client, logger, temp_tables_path)
        logger.info("Writing row to %s", table_path)
        yt_client.write_table(table_path, [row])
        logger.info("Reading %s", table_path)
        rows = list(yt_client.read_table(table_path))
    finally:
        if table_path is not None:
            yt_client.remove(table_path, force=True)

    if rows != [row]:
        message = "Unexpected yt read-table output for {}: expected {}, got {}".format(
            table_path,
            [row],
            rows,
        )
        logger.error(message)
        return states.UNAVAILABLE_STATE, message

    return states.FULLY_AVAILABLE_STATE, "yt write-table/yt read-table succeeded for {}".format(table_path)


if __name__ == "__main__":
    main(run_check)
