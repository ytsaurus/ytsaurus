#!/usr/bin/python2

from yt.tools.atomic import process_tasks_from_list

import yt.logger as yt_logger

import yt.wrapper as yt

import logging
from argparse import ArgumentParser

logger = logging.getLogger("Yt.compression.worker")

def compress(task):
    table = task["table"]
    try:
        if not yt.exists(table):
            return

        if yt.check_permission("cron", "write", table)["action"] != "allow":
            logger.warning("Have no permission to write table %s", table)
            return

        revision = yt.get_attribute(table, "revision")
        spec = {"pool": task["pool"]}

        logger.info("Compressing table %s", table)

        temp_table = yt.create_temp_table(prefix="compress")
        try:
            # To copy all attributes of node
            yt.remove(temp_table)
            yt.copy(table, temp_table, preserve_account=True)
            yt.run_erase(temp_table)

            transformed = yt.transform(table,
                                       temp_table,
                                       erasure_codec=task["erasure_codec"],
                                       compression_codec=task["compression_codec"],
                                       spec=spec)

            if not transformed:
                logger.info("Table %s is not transformed, skipping it", table)
                return

            if yt.exists(table):
                client = yt.YtClient(config=yt.config.config)
                client.config["start_operation_retries"]["retry_count"] = 1
                with client.Transaction():
                    client.lock(table)
                    if client.get_attribute(table, "revision") == revision:
                        client.run_merge(temp_table, table, spec=spec)
                        if client.has_attribute(table, "force_nightly_compress"):
                            client.remove(table + "/@force_nightly_compress")
                        client.set_attribute(table, "nightly_compressed", True)
                        for codec in ("compression", "erasure"):
                            client.set_attribute(table, codec + "_codec", task[codec + "_codec"])
                    else:
                        logger.info("Table %s has changed while compression", table)
        finally:
            yt.remove(temp_table, force=True)
    except yt.YtError:
        logger.exception("Failed to merge table %s", table)

def configure_logging(args):
    formatter = logging.Formatter("%(asctime)-15s\t{}\t%(levelname)s\t%(message)s".format(args.id))

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger.handlers = [handler]
    logger.setLevel(logging.INFO)

    yt_logger.set_formatter(formatter)
    yt_logger.BASIC_FORMATTER = formatter

def configure_client():
    command_params = yt.config.get_option("COMMAND_PARAMS", None)
    command_params["suppress_access_tracking"] = True

def main():
    parser = ArgumentParser(description="Run compression")
    parser.add_argument("--tasks-path", required=True, help="path to compression tasks list")
    parser.add_argument("--id", required=True, help="worker id, useful for logging")
    parser.add_argument("--process-tasks-and-exit", action="store_true", default=False, help="do not run forever")
    args = parser.parse_args()

    configure_client()
    configure_logging(args)
    process_tasks_from_list(args.tasks_path, compress, process_forever=not args.process_tasks_and_exit)

if __name__ == "__main__":
    main()
