import yt.yson as yson


import time


class Data():
    def __init__(self, schema, source_data, result_data, dynamic=False):
        self.schema = schema
        self.source_data = source_data
        self.result_data = result_data
        self.dynamic = dynamic


POLL_FREQUENCY = 0.1
LOG_FREQUENCY = 1.0
TEMP_PATH = "//sys/admin/odin/query_tracker_liveness"
TABLE_EXPIRATION_TIMEOUT_MILLISECONDS = 360 * 1000


def get_query_state(yt_client, query_id, stage):
    query = yt_client.get_query(query_id, attributes=["state", "error"], stage=stage)
    return query["state"]


def check_failed_state(state):
    return state in ("failed", "aborted")


def check_finished_state(state):
    return state in ("completed")


def check_terminal_state(state):
    return check_failed_state(state) or check_finished_state(state)


def log_query(logger, query_id, state):
    logger.info(f"Query {query_id}: {state}")


def track_query(yt_client, logger, query_id, stage):
    last_log_time = time.time()
    while True:
        state = get_query_state(yt_client, query_id, stage)

        current_time = time.time()
        if current_time - last_log_time >= LOG_FREQUENCY:
            log_query(logger, query_id, state)
            last_log_time = current_time

        if check_terminal_state(state):
            return state

        time.sleep(POLL_FREQUENCY)


def run_check_impl(
    query_tracker_client,
    engine_client,
    logger,
    stage,
    states,
    soft_timeout,
    engine,
    query,
    data,
    settings={},
):
    temp_path = TEMP_PATH

    check_result = states.UNAVAILABLE_STATE

    try:
        if not engine_client.exists(temp_path):
            logger.info('Creating "%s".', temp_path)
            engine_client.mkdir(temp_path, recursive=True)

        source_table_path = engine_client.create_temp_table(path=temp_path, attributes={"expiration_timeout": TABLE_EXPIRATION_TIMEOUT_MILLISECONDS, "dynamic": data.dynamic, "schema": data.schema})
        if data.dynamic:
            engine_client.mount_table(source_table_path, sync=True)
            engine_client.insert_rows(source_table_path, data.source_data)
        else :
            engine_client.write_table(source_table_path, data.source_data)
        logger.info("Created %s table.", source_table_path)

        query_id = query_tracker_client.start_query(engine, query.format(table=source_table_path), settings=settings, stage=stage)
        query_start_time = time.time()
        final_query_state = track_query(query_tracker_client, logger, query_id, stage)
        query_execution_time = int(time.time() - query_start_time)

        log_query(logger, query_id, final_query_state)

        if check_failed_state(final_query_state):
            check_result = states.UNAVAILABLE_STATE
            logger.info("Query %s failed", query_id)
        else:
            logger.info("Query %s finished in %d seconds", query_id, query_execution_time)
            result_bytes = query_tracker_client.read_query_result(query_id, 0, stage=stage).read()
            result = list(yson.loads(result_bytes, yson_type="list_fragment"))

            if result != data.result_data:
                check_result = states.UNAVAILABLE_STATE
                logger.error("Query %s returned an incorrect result; expected:\n%s\nreceived:\n%s", query_id, data.result_data, result)
            elif query_execution_time > soft_timeout:
                check_result = states.PARTIALLY_AVAILABLE_STATE
                logger.error("Query %s finished correctly, but took more than %d seconds to complete", query_id, soft_timeout)
            else:
                check_result = states.FULLY_AVAILABLE_STATE
                logger.info("Query %s finished correctly", query_id)

    except Exception as error:
        logger.info('Can\'t check query tracker with error: {}'.format(str(error)))

    finally:
        engine_client.remove(source_table_path, force=True)

    return check_result
