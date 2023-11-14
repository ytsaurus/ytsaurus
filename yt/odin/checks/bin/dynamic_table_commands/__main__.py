from yt_odin_checks.lib.check_runner import main

import yt.wrapper as yt

from copy import deepcopy
from datetime import datetime, timedelta

TABLE_EXPIRATION_TIME_DELTA_MINUTES = 30


def run_check(yt_client, logger, options, states):
    config = deepcopy(yt_client.config)
    config["backend"] = "rpc"
    rpc_client = yt.YtClient(config=config)
    yt_client = None

    temp_path = options["temp_tables_path"]

    check_result = states.FULLY_AVAILABLE_STATE

    if not rpc_client.exists(temp_path):
        rpc_client.mkdir(temp_path, recursive=True)
        logger.info('Created temp directory %s', temp_path)

    now = datetime.now()
    table_index = (now.second / 10) + now.minute * 6
    table = "{}/table_{}".format(temp_path, table_index)
    rpc_client.remove(table, force=True)

    try:
        rpc_client.create("table", table, attributes={
            "dynamic": True,
            "schema": [
                {"name": "x", "type": "string"},
                {"name": "y", "type": "string"}
            ],
            "tablet_cell_bundle": options["tablet_cell_bundle"],
            "expiration_time": "{}".format(datetime.utcnow() +
                                           timedelta(minutes=TABLE_EXPIRATION_TIME_DELTA_MINUTES)),
        })
        logger.info("Created table %s", table)

        correct_rows = [{"x": "hello", "y": "world"}]
        rpc_client.mount_table(table, sync=True)
        logger.info("Mounted table")

        with rpc_client.Transaction(type="tablet"):
            rpc_client.insert_rows(table, correct_rows)
            logger.info("Inserted rows")

        result_rows = list(rpc_client.select_rows("x, y from [{}]".format(table)))

        if result_rows != correct_rows:
            logger.warning("Incorrect result (expected: %s, received: %s)", correct_rows, result_rows)
            check_result = states.UNAVAILABLE_STATE
        else:
            logger.info("Result is correct")

    finally:
        rpc_client.unmount_table(table, sync=True)
        logger.info("Unmounted table")
        rpc_client.remove(table, force=True)

    return check_result


if __name__ == "__main__":
    main(run_check)
