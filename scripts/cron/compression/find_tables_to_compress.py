#!/usr/bin/python2

from yt.common import date_string_to_timestamp
from yt.wrapper.common import parse_bool, get_value, filter_dict
from yt.wrapper.retries import Retrier

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

STATISTICS_REQUEST_BATCH_SIZE = 100

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
        elif err.is_resolve_error():
            logger.warning("Failed to get node %s, node does not exist", path)
        else:
            raise

    return yson.YsonMap({}, attributes={"type": "map_node"})

class CompressionTask(object):
    __slots__ = ["table", "compression_codec", "erasure_codec", "pool", "queue"]

    def __init__(self, table, compression_codec=None, erasure_codec=None, pool=None, queue=None):
        self.table = table
        self.compression_codec = get_value(compression_codec, DEFAULT_COMPRESSION_CODEC)
        self.erasure_codec = get_value(erasure_codec, DEFAULT_ERASURE_CODEC)
        self.pool = get_value(pool, DEFAULT_POOL)
        self.queue = get_value(queue, DEFAULT_QUEUE_NAME)

    def as_dict(self, ignore_keys=None):
        return dict((k, getattr(self, k)) for k in self.__slots__
                    if k not in get_value(ignore_keys, []))

class OptimisticLockingFailedError(Exception):
    pass

class StatisticsRequestRetrier(Retrier):
    def __init__(self):
        retry_config = {
            "enable": True,
            "count": 5,
            "backoff": {
                "policy": "constant_time",
                "constant_time": 5 * 1000
            }
        }

        super(StatisticsRequestRetrier, self).__init__(
            retry_config,
            exceptions=(OptimisticLockingFailedError,))

        self.index_table_pairs = None
        self.responses = None

    def action(self):
        requests = []
        for _, table in self.index_table_pairs:
            request = {
                "command": "get",
                "parameters": {
                    "path": table,
                    "attributes": ["compression_statistics", "erasure_statistics", "chunk_count"],
                    "suppress_access_tracking": True
                }
            }
            requests.append(request)

        responses = yt.execute_batch(requests)

        index_table_pairs = []

        for rsp, index_table_pair in zip(responses, self.index_table_pairs):
            if "error" in rsp:
                error = yt.YtResponseError(rsp["error"])

                if error.is_resolve_error():
                    logger.info('Table %s does not exist', table)
                    continue

                if error.is_concurrent_transaction_lock_conflict():
                    logger.info('Table %s is locked', table)
                    continue

                # Optimistic locking failed
                if error.contains_code(720):
                    index_table_pairs.append(index_table_pair)
                else:
                    raise error
            else:
                self.responses[index_table_pair[0]] = rsp["output"]

        self.index_table_pairs = index_table_pairs
        if self.index_table_pairs:
            raise OptimisticLockingFailedError()

    def collect_statistics(self, tables):
        self.index_table_pairs = list(enumerate(tables))
        self.responses = [None] * len(tables)
        self.run()
        return self.responses

def filter_out_tables_with_proper_codecs(tasks):
    result = []
    retrier = StatisticsRequestRetrier()

    batch_count = (len(tasks) + STATISTICS_REQUEST_BATCH_SIZE - 1) / STATISTICS_REQUEST_BATCH_SIZE

    for batch_index in xrange(batch_count):
        start_index = batch_index * STATISTICS_REQUEST_BATCH_SIZE
        end_index = min(start_index + STATISTICS_REQUEST_BATCH_SIZE, len(tasks))

        batch = tasks[start_index:end_index]
        tables_in_batch = [task.table for task in batch]

        tables_with_statistics = retrier.collect_statistics(tables_in_batch)

        for task, table in zip(batch, tables_with_statistics):
            if table is None:  # table does not exist or locked
                continue

            if not has_proper_codecs(table, task.erasure_codec, task.compression_codec):
                result.append(task)

    return result

def find_tables_to_compress(root):
    ignore_nodes = ["//sys", "//home/qe"]

    requested_attributes = ["type", "opaque", "force_nightly_compress",
                            "uncompressed_data_size", "nightly_compression_settings",
                            "nightly_compressed", "creation_time"]

    compression_settings_allowed_keys = set(["min_table_size", "min_table_age", "enabled",
                                             "compression_codec", "erasure_codec",
                                             "force_recompress_to_specified_codecs",
                                             "queue", "pool"])

    tasks = []

    def walk(path, object, compression_settings=None):
        if path in ignore_nodes:
            return

        if object.attributes["type"] == "table":
            if parse_bool(object.attributes.get("force_nightly_compress", "false")):
                tasks.append(CompressionTask(path))
                return

            if compression_settings is None or not isinstance(compression_settings, dict):
                return

            params = filter_dict(lambda k, v: k in compression_settings_allowed_keys, deepcopy(compression_settings))
            min_table_size = params.pop("min_table_size", 0)
            min_table_age = params.pop("min_table_age", 0)
            enabled = parse_bool(params.pop("enabled", "false"))
            force_recompress_to_specified_codecs = \
                parse_bool(params.pop("force_recompress_to_specified_codecs", "true"))

            if parse_bool(object.attributes.get("nightly_compressed", "false")) and \
                    not force_recompress_to_specified_codecs:
                return

            task = CompressionTask(path, **params)

            table_age = time.time() - date_string_to_timestamp(object.attributes["creation_time"])
            table_size = object.attributes["uncompressed_data_size"]

            if not enabled or table_size <= min_table_size or table_age <= min_table_age:
                return

            tasks.append(task)
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

    logger.info("Total task count before filtering: %d", len(tasks))

    tasks = filter_out_tables_with_proper_codecs(tasks)
    total_table_count = len(tasks)

    compression_queues = defaultdict(list)
    for task in tasks:
        compression_queues[task.queue].append(task.as_dict(ignore_keys=["queue"]))

    logger.info("Collected %d tables for compression", total_table_count)

    return compression_queues, total_table_count

def configure_client():
    command_params = yt.config.get_option("COMMAND_PARAMS", None)
    command_params["suppress_access_tracking"] = True

def main():
    parser = argparse.ArgumentParser(description="Script finds tables to compress on cluster")
    parser.add_argument("--search-root", default="/", help='path to search tables, default is "/"')
    parser.add_argument("--tasks-root", required=True, help="compression task lists root path")

    args = parser.parse_args()

    configure_client()

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
