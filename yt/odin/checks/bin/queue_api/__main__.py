from yt.yson.yson_types import YsonEntity
from yt_odin_checks.lib.check_runner import main
from yt.wrapper import YtClient
from yt.ypath import parse_ypath

from yt.test_helpers import wait

from logging import Logger
from datetime import datetime


def run_check(secrets, yt_client: YtClient, logger: Logger, options, states):
    client = yt_client
    cluster_name = options["cluster_name"]
    temp_path = options["temp_tables_path"]
    # NB(apachee): Timeout for wait on requests, which depend on registration cache.
    wait_timeout = options["wait_timeout"]
    now = datetime.now()
    table_index = now.minute * 60 + now.second
    queue_path = f"{temp_path}/queue_{table_index}"
    consumer_path = f"{temp_path}/consumer_{table_index}"

    def ignore_exception(functor):
        try:
            functor()
        except Exception as ex:
            logger.warning("Exception during cleanup: %s", ex)

    def cleanup():
        ignore_exception(lambda: client.remove(queue_path, force=True))
        ignore_exception(lambda: client.remove(consumer_path, force=True))
        logger.info("Cleanup completed")

    logger.info("Checking Queue API for cluster %s", cluster_name)

    # NB(apachee): This part of the check should be fast,
    # but if it isn't, check will return partially available state,
    # since this is only the setup without the use of Queue API.
    try:
        if not client.exists(temp_path):
            client.mkdir(temp_path, recursive=True)
            logger.info("Created temp directory %s", temp_path)

        # XXX(apachee): Not sure whether unregister_queue_consumer should be called here.
        client.remove(queue_path, force=True)
        client.remove(consumer_path, force=True)

        queue_schema = [{"name": "data", "type": "string"}]
        client.create("table", queue_path, attributes={
            "dynamic": True,
            "schema": queue_schema,
        })

        # NB(apachee): If mounting is too slow, then either consumer creation
        # will raise an exception (request timeout) or wait for queue mounting
        # will timeout.
        before_mounting_ts = datetime.now()
        client.mount_table(queue_path, sync=False)

        try:
            client.create("queue_consumer", consumer_path)
        except Exception as ex:
            logger.warning("Failed to create queue_consumer using queue_consumer type handler: %s", ex)

            client.create("table", consumer_path, attributes={
                "dynamic": True,
                "schema": [
                    {"name": "queue_cluster", "type": "string", "sort_order": "ascending", "required": True},
                    {"name": "queue_path", "type": "string", "sort_order": "ascending", "required": True},
                    {"name": "partition_index", "type": "uint64", "sort_order": "ascending", "required": True},
                    {"name": "offset", "type": "uint64", "required": True},
                    {"name": "meta", "type": "any", "required": False},
                ],
            })
            client.mount_table(consumer_path, sync=True)

        queue_mount_timeout = max(10 - (datetime.now() - before_mounting_ts).total_seconds(), 0)
        wait(lambda: client.get(f"{queue_path}/@tablet_state") == "mounted", timeout=queue_mount_timeout, error_message="Wait for queue tablet state to be mounted timed out")

        logger.info("Created queue %s and consumer %s", queue_path, consumer_path)
    except Exception as ex:
        logger.error(f"Check setup failed: {ex}")
        cleanup()
        return states.PARTIALLY_AVAILABLE_STATE, f"Check setup failed: {ex}"

    logger.info("Check setup complete")

    should_unregister_queue_consumer = True

    try:
        client.register_queue_consumer(queue_path, consumer_path, vital=True)

        expected_registrations = [{
            "queue_path": parse_ypath(f"{cluster_name}:{queue_path}"),
            "consumer_path": parse_ypath(f"{cluster_name}:{consumer_path}"),
            "vital": True,
            "partitions": YsonEntity(),
        }]

        logger.info(f"Expected registrations: {expected_registrations}")

        def check_registrations():
            registrations = client.list_queue_consumer_registrations(queue_path=queue_path, consumer_path=consumer_path)

            logger.debug(f"Actual registrations: {registrations}")

            return expected_registrations == registrations

        wait(check_registrations, error_message="Check for list_queue_consumer_registration timed out", timeout=wait_timeout)

        logger.info("Registered queue consumer")

        rows = [{"data": "anime"}, {"data": "manga"}]
        expected_rows = [{**row, "$row_index": i, "$tablet_index": 0} for i, row in enumerate(rows)]

        client.insert_rows(queue_path, rows)

        logger.info("Inserted rows in %s", queue_path)

        def guard_check(exception_handler):
            def guard_check_factory(check_function):
                def guarded_check_function(*args, **kwargs):
                    try:
                        return check_function(*args, **kwargs)
                    except AssertionError as ex:
                        raise ex
                    except Exception as ex:
                        exception_handler(ex)
                    return False
                return guarded_check_function
            return guard_check_factory

        @guard_check(exception_handler=lambda ex: logger.debug("Pulling from consumer failed: %s", ex))
        def check_pull_consumer(expected_row, offset):
            pulled_rows = list(client.pull_consumer(consumer_path, queue_path, max_row_count=1,
                                                    offset=offset, partition_index=0))
            assert [expected_row] == pulled_rows, f"Actual pulled rows {pulled_rows} are different from expected rows {expected_row}"
            return True

        @guard_check(exception_handler=lambda ex: logger.debug("Advancing consumer failed: %s", ex))
        def check_advance_consumer():
            client.advance_consumer(consumer_path, queue_path, 0, 0, 1, client_side=False)
            return True

        wait(lambda: check_pull_consumer(expected_rows[0], 0), error_message="Check for pull_consumer timed out", timeout=wait_timeout)
        logger.info("Pulled first row from queue %s using consumer %s", queue_path, consumer_path)
        wait(check_advance_consumer, error_message="Check for advance_consumer timed out", timeout=wait_timeout)
        logger.info("Advanced consumer %s for queue %s", consumer_path, queue_path)
        wait(lambda: check_pull_consumer(expected_rows[1], 1), error_message="Check for pull_consumer timed out", timeout=wait_timeout)
        logger.info("Pulled second row from queue %s using consumer %s", queue_path, consumer_path)

        # NB(apachee): With this, unregister is called only once.
        should_unregister_queue_consumer = False
        client.unregister_queue_consumer(queue_path, consumer_path)

        cleanup()

        return states.FULLY_AVAILABLE_STATE
    except Exception as err:
        logger.warning("Failed to check Queue API on cluster %s", cluster_name)

        if should_unregister_queue_consumer:
            ignore_exception(lambda: client.unregister_queue_consumer(queue_path, consumer_path))
        cleanup()

        raise err


if __name__ == "__main__":
    main(run_check)
