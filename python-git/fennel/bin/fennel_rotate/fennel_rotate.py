#!/usr/bin/env python

from yt.common import get_value
from yt.wrapper.common import get_disk_space_from_resources
import yt.wrapper as yt

import re
import sys
import logging
from argparse import ArgumentParser

TABLE_NAME = None

logger = logging.getLogger("Fennel")

def get_tables():
    pattern = re.compile(TABLE_NAME + "(\.\d*)?$")
    return [str(obj)
            for obj in yt.list(yt.config["prefix"][:-1], attributes=["type"])
            if obj.attributes.get("type") == "table"
                and pattern.match(str(obj))]

def get_archive_number(table):
    if table == TABLE_NAME:
        return 0
    else:
        return int(table.split(".")[1])

def get_next_table(table):
    if table == TABLE_NAME:
        return TABLE_NAME + ".1"
    else:
        return TABLE_NAME + "." + str(int(table.split(".")[1]) + 1)

def get_prev_table(table):
    num = int(table.split(".")[1]) - 1
    if num == 0:
        return TABLE_NAME
    else:
        return TABLE_NAME + "." + str(num)

def get_size(table):
    return get_disk_space_from_resources(yt.get(table + "/@resource_usage"))

def get_disk_space_per_row(table):
    attributes = yt.get(table + "/@")
    if attributes["row_count"]:
        return get_disk_space_from_resources(attributes["resource_usage"]) // attributes["row_count"]
    return 0

def get_archive_disk_space_ratio(example_of_archived_table):
    disk_space_per_row = get_disk_space_per_row(TABLE_NAME)
    if yt.exists(example_of_archived_table):
        archive_disk_space_per_row = get_disk_space_per_row(example_of_archived_table)
    else:
        archive_disk_space_per_row = 0
    if archive_disk_space_per_row == 0 or disk_space_per_row == 0:
        return 0.2
    else:
        return float(archive_disk_space_per_row) / disk_space_per_row

def get_archive_compression_ratio(example_of_archived_table):
    try:
        compression_ratio = yt.get(example_of_archived_table + "/@compression_ratio")
        if compression_ratio > 0.0:
            return compression_ratio
        else:
            return 0.05
    except yt.YtResponseError as err:
        if err.is_resolve_error():
            return 0.05
        raise

def get_processed_row_count(attributes=None):
    if attributes is None:
        attributes = yt.get(TABLE_NAME + "/@")
    if "processed_row_count" in attributes:
        return attributes["processed_row_count"]
    else:
        return attributes["row_count"]

def get_possible_size_to_archive():
    attributes = yt.get(TABLE_NAME + "/@")
    if attributes["row_count"] == 0:
        return 0
    return (get_processed_row_count(attributes) * get_disk_space_from_resources(attributes["resource_usage"])) // attributes["row_count"]

def get_desired_row_count_to_archive(desired_archive_size, example_of_archived_table):
    free_archive_size = desired_archive_size
    if yt.exists(TABLE_NAME + ".1"):
        free_archive_size -= get_disk_space_from_resources(yt.get(TABLE_NAME + ".1/@resource_usage"))
    archive_ratio = get_archive_disk_space_ratio(example_of_archived_table)
    size_to_archive = min(get_possible_size_to_archive(), free_archive_size) / archive_ratio
    logger.info("Archive ratio is %f, size to archive is %d", archive_ratio, size_to_archive)

    attributes = yt.get(TABLE_NAME + "/@")
    if attributes["row_count"] == 0:
        return 0
    return int(min(get_processed_row_count(attributes), (attributes["row_count"] * size_to_archive) / get_disk_space_from_resources(attributes["resource_usage"])))

def fennel_exists():
    try:
        yt.get(TABLE_NAME + "/@processed_row_count")
        return True
    except yt.YtResponseError as err:
        if err.is_resolve_error():
            return False
        raise

def get_archived_row_count():
    try:
        return yt.get(TABLE_NAME + "/@archived_row_count")
    except yt.YtResponseError as err:
        if err.is_resolve_error():
            return 0
        raise

def erase_archived_prefix():
    with yt.Transaction():
        archived_row_count = get_archived_row_count()
        if archived_row_count == 0:
            return True

        logger.info("Erasing archived prefix of event log table")

        processed_row_count = get_processed_row_count()

        assert archived_row_count <= processed_row_count
        try:
            yt.lock(TABLE_NAME, mode="exclusive")
        except yt.YtResponseError as err:
            if err.is_concurrent_transaction_lock_conflict():
                return False
            raise
        logger.info("Selected %d archived rows to erase", archived_row_count)
        yt.run_erase(yt.TablePath(TABLE_NAME, start_index=0, end_index=archived_row_count))
        yt.set(TABLE_NAME + "/@archived_row_count", 0)
        if yt.exists(TABLE_NAME + "/@processed_row_count"):
            yt.set(TABLE_NAME + "/@processed_row_count", processed_row_count - archived_row_count)
            if yt.exists(TABLE_NAME + "/@table_start_row_index"):
                yt.set(TABLE_NAME + "/@table_start_row_index", yt.get(TABLE_NAME + "/@table_start_row_index") + archived_row_count)
        return True

def rotate_archives(archive_size_limit, min_portion_to_archive):
    if yt.exists(TABLE_NAME + ".1") and get_size(TABLE_NAME + ".1") >= archive_size_limit - min_portion_to_archive:
        logger.info("Rotate event log archives")
        tables = get_tables()
        tables.sort(key=get_archive_number)
        for table in reversed(tables[1:]):
            if get_prev_table(table) in tables:
                logger.info("Moving %s to %s", table, get_next_table(table))
                yt.move(table, get_next_table(table))

def archive_table(archive_size_limit):
    logger.info("Start archivation prefix of event log table")

    tables = get_tables()
    tables.sort(key=get_archive_number)
    if len(tables) > 1:
        example_archive = tables[1]
    else:
        example_archive = TABLE_NAME + ".1"

    row_count_to_archive = get_desired_row_count_to_archive(archive_size_limit, example_archive)

    logger.info("Selected first %d rows to archive", row_count_to_archive)
    archived_row_count = get_archived_row_count()
    assert archived_row_count == 0

    with yt.Transaction():
        data_size_per_job = min(10 * 1024 ** 3, int((1024 ** 3) / get_archive_compression_ratio(example_archive)))
        yt.create("table", TABLE_NAME + ".1", attributes={"erasure_codec": "lrc_12_2_2", "compression_codec": "zlib6"}, ignore_existing=True)
        yt.run_merge(yt.TablePath(TABLE_NAME, start_index=0, end_index=row_count_to_archive), yt.TablePath(TABLE_NAME + ".1", append=True),
                     spec={"force_transform": True, "data_size_per_job": data_size_per_job})
        yt.set(TABLE_NAME + "/@archived_row_count", row_count_to_archive)

def configure_logging(args):
    if args.log_file is not None:
        handler = logging.handlers.WatchedFileHandler(args.log_file)
    else:
        handler = logging.StreamHandler()
    level = logging.__dict__[get_value(args.log_level, "INFO")]

    def configure_logger(logger):
        logger.propagate = False
        logger.setLevel(level)
        logger.handlers = [handler]
        logger.handlers[0].setFormatter(logging.Formatter("%(asctime)-15s\t%(levelname)s\t%(name)s\t%(message)s"))

    configure_logger(logging.getLogger("Fennel"))
    configure_logger(logging.getLogger("Yt"))

def main():
    parser = ArgumentParser(description="Script to rotate scheduler event logs")
    parser.add_argument("--archive-size-limit", type=int, default=500 * 1024 ** 3)
    parser.add_argument("--min-portion-to-archive", type=int, default=10 * 1024 ** 3)
    parser.add_argument("--skip-fennel-check", action="store_true", default=False)
    parser.add_argument("--table-directory", default="//sys/scheduler/")
    parser.add_argument("--table-name", default="event_log")
    parser.add_argument("--log-file", help="path to log file, stderr if not specified")
    parser.add_argument("--log-level", help="log level")

    args = parser.parse_args()

    configure_logging(args)

    yt.config["prefix"] = args.table_directory

    global TABLE_NAME
    TABLE_NAME = args.table_name

    if not args.skip_fennel_check and not fennel_exists():
        logger.error("Event log is not processed by fennel, it is impossible to safely rotate it")
        return 1

    if get_possible_size_to_archive() < args.min_portion_to_archive:
        logger.info("There is now enough data to archive")
        return 0

    if erase_archived_prefix():
        rotate_archives(args.archive_size_limit, args.min_portion_to_archive)
        archive_table(args.archive_size_limit)
        erase_archived_prefix()
    else:
        logger.warning("Failed to erase archived prefix of 'event_log' due to lock conflict. Skip rotation.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
