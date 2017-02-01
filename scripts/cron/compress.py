#!/usr/bin/env python

from yt.tools.atomic import process_tasks_from_list
from yt.tools.conversion_tools import convert_to_erasure

from yt.common import date_string_to_timestamp

from yt.wrapper.cli_helpers import die
from yt.wrapper.common import parse_bool, get_value, filter_dict

import yt.logger as logger
import yt.wrapper as yt
import yt.yson as yson

from copy import deepcopy
from argparse import ArgumentParser
import time

DEFAULT_COMPRESSION_CODEC = "gzip_best_compression"
DEFAULT_ERASURE_CODEC = "lrc_12_2_2"

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

def compress(task):
    table = task["table"]
    try:
        if not yt.exists(table):
            return

        if yt.check_permission("cron", "write", table)["action"] != "allow":
            logger.warning("Have no permission to write table %s", table)
            return

        logger.info("Compressing table %s", table)
        convert_to_erasure(table,
                           erasure_codec=task["erasure_codec"],
                           compression_codec=task["compression_codec"])

        if yt.exists(table + "/@force_nightly_compress"):
            yt.remove(table + "/@force_nightly_compress")
        yt.set_attribute(table, "nightly_compressed", True)
    except yt.YtError:
        logger.exception("Failed to merge table %s", table)

def safe_get(path, **kwargs):
    try:
        return yt.get(path, **kwargs)
    except yt.YtResponseError as err:
        if err.is_access_denied():
            logger.warning("Failed to get node %s, access denied", path)
        else:
            raise

    return yson.YsonMap({}, attributes={"type": "map_node"})

def escape(s):
    def escape_char(ch):
        return "\\" + ch if ch in ["\\", "/", "@", "&", "[", "{"] else ch

    return "".join(escape_char(char) for char in s)

def make_compression_task(table, compression_codec=None, erasure_codec=None):
    compression_codec = get_value(compression_codec, DEFAULT_COMPRESSION_CODEC)
    erasure_codec = get_value(erasure_codec, DEFAULT_ERASURE_CODEC)
    return {
        "table": table,
        "compression_codec": compression_codec,
        "erasure_codec": erasure_codec
    }

def find(root):
    tables = []
    ignore_nodes = ["//sys", "//home/qe"]

    requested_attributes = ["type", "opaque", "force_nightly_compress", "uncompressed_data_size",
                            "nightly_compression_settings", "nightly_compressed", "compression_statistics",
                            "erasure_statistics", "chunk_count", "creation_time"]

    compression_settings_allowed_keys = set(["min_table_size", "min_table_age", "enabled", "compression_codec",
                                             "erasure_codec", "force_recompress_to_specified_codecs"])

    def walk(path, object, compression_settings=None):
        if path in ignore_nodes:
            return

        if object.attributes["type"] == "table":
            if parse_bool(object.attributes.get("force_nightly_compress", "false")):
                tables.append(make_compression_task(path))
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

            task = make_compression_task(path, **params)

            if has_proper_codecs(object, task["erasure_codec"], task["compression_codec"]):
                logger.info("Table %s already has proper compression and erasure codecs", path)
                return

            table_age = time.time() - date_string_to_timestamp(object.attributes["creation_time"])
            table_size = object.attributes["uncompressed_data_size"]
            if enabled and table_size > min_table_size and table_age > min_table_age:
                tables.append(task)
        elif object.attributes["type"] == "map_node":
            if parse_bool(object.attributes.get("opaque", "false")):
                object = safe_get(path, attributes=requested_attributes)

            compression_settings = object.attributes.get("nightly_compression_settings",
                                                         compression_settings)

            for key, value in object.iteritems():
                walk("{0}/{1}".format(path, escape(key)), value, compression_settings)
        else:
            logger.debug("Skipping %s %s", object.attributes["type"], path)

    root_obj = safe_get(root, attributes=requested_attributes)
    walk(root, root_obj)

    logger.info("Collected %d tables for compression", len(tables))

    return tables

def main():
    parser = ArgumentParser(description="Find tables to compress and run compression")
    parser.add_argument("action", help="Action should be 'find' or 'run'")
    parser.add_argument("--queue", required=True, help="Path to the queue with tables")
    parser.add_argument("--path", default="/", help='Search path. Default is cypress root "/"')
    args = parser.parse_args()

    if args.action == "find":
        yt.set(args.queue, find(args.path))
    elif args.action == "run":
        process_tasks_from_list(args.queue, compress)
    else:
        die("Incorrect action: " + args.action)

if __name__ == "__main__":
    main()
