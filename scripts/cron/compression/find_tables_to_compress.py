#!/usr/bin/python2

from yt.common import date_string_to_timestamp
from yt.wrapper.common import parse_bool, get_value, filter_dict

import yt.logger as logger
import yt.yson as yson

from yt.packages.six.moves import xrange

import yt.wrapper as yt

from collections import defaultdict
from copy import deepcopy
import argparse
import time

DEFAULT_COMPRESSION_CODEC = "gzip_best_compression"
DEFAULT_ERASURE_CODEC = "lrc_12_2_2"

DEFAULT_QUEUE_NAME = "default"
DEFAULT_POOL = "cron_compression"

# XXX(asaitgalin): See https://st.yandex-team.ru/YT-5015
CODECS_SYNONYMS = {
    "gzip_best_compression": ["zlib_9"],
    "gzip_normal": ["zlib_6"],
    "zlib6": ["zlib_6"],
    "zlib9": ["zlib_9"],
    "brotli3": ["brotli_3"],
    "brotli5": ["brotli_5"],
    "brotli8": ["brotli_8"]
}

WORKER_TASKS_SET_ATTEMPT_COUNT = 3

def has_proper_codecs(table, erasure_codec, compression_codec):
    compression_stats = table.attributes["compression_statistics"]
    erasure_stats = table.attributes["erasure_statistics"]

    chunk_count = table.attributes["chunk_count"]
    if chunk_count == 0:
        return True

    chunk_count_in_erasure = erasure_stats.get(erasure_codec, {}).get("chunk_count", 0)
    if chunk_count_in_erasure != chunk_count:
        return False

    codecs = CODECS_SYNONYMS.get(compression_codec, []) + [compression_codec]
    for codec in codecs:
        compressed_chunk_count = compression_stats.get(codec, {}).get("chunk_count", 0)
        if compressed_chunk_count == chunk_count:
            return True

    return False


def safe_get(path, **kwargs):
    try:
        return yt.get(path, **kwargs)
    except yt.YtResponseError as err:
        if err.is_access_denied():
            logger.warning("Failed to get node %s, access denied", path)
        else:
            raise

    return yson.YsonMap({}, attributes={"type": "map_node"})

def make_compression_task(table, compression_codec=None, erasure_codec=None, pool=None):
    compression_codec = get_value(compression_codec, DEFAULT_COMPRESSION_CODEC)
    erasure_codec = get_value(erasure_codec, DEFAULT_ERASURE_CODEC)
    pool = get_value(pool, DEFAULT_POOL)
    return {
        "table": table,
        "compression_codec": compression_codec,
        "erasure_codec": erasure_codec,
        "pool": pool
    }

def find_tables_to_compress(root):
    compression_queues = defaultdict(list)

    ignore_nodes = ["//sys", "//home/qe"]

    requested_attributes = ["type", "opaque", "force_nightly_compress",
                            "uncompressed_data_size", "nightly_compression_settings",
                            "nightly_compressed", "compression_statistics",
                            "erasure_statistics", "chunk_count", "creation_time"]

    compression_settings_allowed_keys = set(["min_table_size", "min_table_age", "enabled",
                                             "compression_codec", "erasure_codec",
                                             "force_recompress_to_specified_codecs",
                                             "queue", "pool"])

    def walk(path, object, compression_settings=None):
        if path in ignore_nodes:
            return

        if object.attributes["type"] == "table":
            if parse_bool(object.attributes.get("force_nightly_compress", "false")):
                compression_queues[DEFAULT_QUEUE_NAME].append(make_compression_task(path))
                return

            if compression_settings is None or not isinstance(compression_settings, dict):
                return

            params = filter_dict(lambda k, v: k in compression_settings_allowed_keys, deepcopy(compression_settings))
            min_table_size = params.pop("min_table_size", 0)
            min_table_age = params.pop("min_table_age", 0)
            queue = params.pop("queue", DEFAULT_QUEUE_NAME)
            enabled = parse_bool(params.pop("enabled", "false"))
            force_recompress_to_specified_codecs = \
                parse_bool(params.pop("force_recompress_to_specified_codecs", "true"))

            if parse_bool(object.attributes.get("nightly_compressed", "false")) and \
                    not force_recompress_to_specified_codecs:
                return

            task = make_compression_task(path, **params)

            if has_proper_codecs(object, task["erasure_codec"], task["compression_codec"]):
                logger.info("Table %s already has proper compression and erasure codecs", path)
                return

            table_age = time.time() - date_string_to_timestamp(object.attributes["creation_time"])
            table_size = object.attributes["uncompressed_data_size"]
            if enabled and table_size > min_table_size and table_age > min_table_age:
                compression_queues[queue].append(task)
        elif object.attributes["type"] == "map_node":
            if parse_bool(object.attributes.get("opaque", "false")):
                object = safe_get(path, attributes=requested_attributes)

            compression_settings = object.attributes.get("nightly_compression_settings",
                                                         compression_settings)

            for key, value in object.iteritems():
                walk(yt.ypath_join(path, key), value, compression_settings)
        else:
            logger.debug("Skipping %s %s", object.attributes["type"], path)

    root_obj = safe_get(root, attributes=requested_attributes)
    walk(root, root_obj)

    total_table_count = sum(len(compression_queues[queue]) for queue in compression_queues)
    logger.info("Collected %d tables for compression", total_table_count)

    return compression_queues, total_table_count

def main():
    parser = argparse.ArgumentParser(description="Script finds tables to compress on cluster")
    parser.add_argument("--search-root", default="/", help='path to search tables, default is "/"')
    parser.add_argument("--tasks-root", required=True, help="compression task lists root path")

    args = parser.parse_args()

    compression_queues, total_table_count = find_tables_to_compress(args.search_root)
    if total_table_count == 0:
        logger.info("Nothing to compress, exiting")
        return

    alive_worker_ids = yt.get_attribute(args.tasks_root, "alive_workers", None)
    if alive_worker_ids is None:
        logger.warning("No alive workers detected, exiting")
        return

    # NOTE: Tasks scheduling algorithm below assumes this.
    assert len(alive_worker_ids) >= len(compression_queues)

    tasks = []
    for queue_name, queue in compression_queues.iteritems():
        ratio = 1.0 * len(queue) / total_table_count
        worker_count = int(max(1.0, ratio * len(alive_worker_ids)))

        tasks_per_worker = max(1, len(queue) / worker_count)
        for i in xrange(worker_count):
            begin_index = i * tasks_per_worker
            end_index = begin_index + tasks_per_worker

            tasks.append(queue[begin_index:end_index])

    for worker_id, task_list in zip(alive_worker_ids, tasks):
        set_successfully = False
        # Worker can extract tasks atomically under exclusive lock so
        # this request should be retried.
        for _ in xrange(WORKER_TASKS_SET_ATTEMPT_COUNT):
            try:
                yt.set(yt.ypath_join(args.tasks_root, worker_id), task_list)
                set_successfully = True
                break
            except yt.YtResponseError as err:
                if err.is_concurrent_transaction_lock_conflict():
                    logger.warning('Failed to set tasks for worker "%s"', worker_id)
                    time.sleep(1.0)
                else:
                    raise

        if not set_successfully:
            raise yt.YtError('Failed to set tasks for worker "%s" after %d attempts',
                             worker_id, WORKER_TASKS_SET_ATTEMPT_COUNT)

        logger.info('Successfully set %d tasks for worker "%s"', len(task_list), worker_id)

    logger.info("All tasks are successfully assigned to workers")

if __name__ == "__main__":
    main()
