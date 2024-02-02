from yt.common import YtError
import yt.yson as yson


import time


class Data():
    def __init__(self, schema, source_data, result_data, dynamic=False):
        self.schema = schema
        self.source_data = source_data
        self.result_data = result_data
        self.dynamic = dynamic


POLL_FREQUENCY = 0.1
TEMP_PATH = "//sys/admin/odin/query_tracker_liveness"
QUERY_TIMEOUT_SECONDS = 30
TABLE_EXPIRATION_TIMEOUT_MILLISECONDS = 60 * 1000


def track_query(yt_client, logger, query_id, stage):
    counter = 0

    max_iterations = QUERY_TIMEOUT_SECONDS / POLL_FREQUENCY
    while counter < max_iterations:
        query = yt_client.get_query(query_id, attributes=["state", "error"], stage=stage)
        state = query["state"]
        if counter % 10 == 0 or state in ("failed", "aborted", "completed"):
            logger.info(f"Query {query_id}: {state}")

        if state in ("failed", "aborted"):
            raise YtError.from_dict(query["error"])
        elif state == "completed":
            return

        time.sleep(POLL_FREQUENCY)
        counter += 1


def run_check_impl(
    query_tracker_client,
    engine_client,
    logger,
    stage,
    states,
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
        if data.dynamic :
            engine_client.mount_table(source_table_path, sync=True)
            engine_client.insert_rows(source_table_path, data.source_data)
        else :
            engine_client.write_table(source_table_path, data.source_data)
        logger.info("Created %s table.", source_table_path)

        query_id = query_tracker_client.start_query(engine, query.format(table=source_table_path), settings=settings, stage=stage)

        try:
            track_query(query_tracker_client, logger, query_id, stage)
        except YtError as error:
            logger.error("Query '%s' failed", query_id)
            logger.exception(error)
            return states.UNAVAILABLE_STATE

        result_bytes = query_tracker_client.read_query_result(query_id, 0, stage=stage).read()
        result = list(yson.loads(result_bytes, yson_type="list_fragment"))

        if result == data.result_data:
            check_result = states.FULLY_AVAILABLE_STATE
            logger.info("Query finished correctly")
        else:
            check_result = states.UNAVAILABLE_STATE
            logger.error("Query returned an incorrect result; expected:\n%s\nreceived:\n%s", data.result_data, result)

    except Exception as error:
        logger.info('Can\'t check query tracker with error: {}'.format(str(error)))

    finally:
        engine_client.remove(source_table_path, force=True)

    return check_result
