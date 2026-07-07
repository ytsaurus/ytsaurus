from yt_odin_checks.lib.check_runner import main

import yt.wrapper as yt


DEFAULT_TABLE_PATH = "//sys/admin/odin/read_static_table_commands/table"
SEED_ROW = {"key": "read_static_table_commands", "value": "seed"}


def _ensure_table_exists(yt_client, logger, table_path):
    if yt_client.exists(table_path):
        return

    logger.info("Creating static table %s", table_path)
    yt_client.create("table", table_path, recursive=True, ignore_existing=True)


def _ensure_seed_row_present(yt_client, logger, table_path):
    rows = list(yt_client.read_table(table_path))
    if SEED_ROW in rows:
        return rows

    logger.info("Appending seed row to static table %s", table_path)
    yt_client.write_table(yt.TablePath(table_path, append=True), [SEED_ROW])
    return list(yt_client.read_table(table_path))


def run_check(yt_client, logger, options, states):
    table_path = options.get("table_path", DEFAULT_TABLE_PATH)

    _ensure_table_exists(yt_client, logger, table_path)

    logger.info("Checking existence of %s", table_path)
    if not yt_client.exists(table_path):
        message = "Static table {} does not exist after creation attempt".format(table_path)
        logger.error(message)
        return states.UNAVAILABLE_STATE, message

    logger.info("Reading %s", table_path)
    rows = _ensure_seed_row_present(yt_client, logger, table_path)

    if SEED_ROW not in rows:
        message = "Static table {} does not contain expected seed row".format(table_path)
        logger.error(message)
        return states.UNAVAILABLE_STATE, message

    return states.FULLY_AVAILABLE_STATE, "yt exists/yt read-table succeeded for {}".format(table_path)


if __name__ == "__main__":
    main(run_check)
