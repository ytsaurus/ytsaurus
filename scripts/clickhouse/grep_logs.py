#!/usr/bin/python

import yt.wrapper as yt

import argparse
import logging
import sys

logger = logging.getLogger(__name__)

LOG_LEVEL_LONG_NAMES = ["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "FATAL"]
LOG_LEVEL_SHORT_NAMES = [level[:1] for level in LOG_LEVEL_LONG_NAMES]

def setup_logging():
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(levelname)s\t%(message)s")
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.DEBUG)
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)


def parse_log_level(log_level):
    for level in LOG_LEVEL_LONG_NAMES:
        if log_level.upper() in (level, level[:1]):
            return level[:1]
    return None


def get_suitable_log_levels(log_level):
    index = LOG_LEVEL_SHORT_NAMES.index(log_level)
    return "(" + ','.join("'" + level + "'" for level in LOG_LEVEL_SHORT_NAMES[index:]) + ")"


def get_input_table_path(artifact_path, trace_id_present=False, log_level=None):
    if log_level in ("T", "D"):
        log_level_suffix = ".debug"
    else:
        log_level_suffix = ""
    if trace_id_present:
        table_kind_suffix = ".ordered_by_trace_id"
    else:
        table_kind_suffix = ""
    return "{}/clickhouse{}.log{}".format(artifact_path, log_level_suffix, table_kind_suffix)


def parse_trace_id(trace_id):
    return trace_id.replace("-", "").lower()


def select_rows(query, limit, batch_size):
    for index in xrange(0, limit, batch_size):
        subquery = "{} LIMIT {} OFFSET {}".format(query, batch_size, index)
        logger.debug("Running batch from %d to %d", index, index + batch_size)
        empty = True
        for row in yt.select_rows(query):
            empty = False
            yield row
        if empty:
            return

def main():
    setup_logging()

    parser = argparse.ArgumentParser(description="Grep logs from log tailer dynamic table")
    parser.add_argument("operation_alias", help="Operation alias of clique to grep (either with * or without)")
    parser.add_argument("--message-like", help="Message pattern substring")
    parser.add_argument("--artifact-path", help="Path of artifact directory of clique; by default "
                                                "//home/clickhouse-kolkhoz/<operation_alias>")
    parser.add_argument("--trace-id", help="Trace id (either with dashes or without)")
    parser.add_argument("--log-level", help="Minimum log level to grep, default is debug; case-insensitive, either "
                                            "first char or full level name", default="debug")
    parser.add_argument("--expression", help="Arbitrary boolean condition to append to query")
    parser.add_argument("--limit", help="Row count limit", default=10**9)
    parser.add_argument("--batch-size", help="Select query batch size; default is 10000", default=10000)
    parser.add_argument("--omit-job-id", help="Do not print job-id", action="store_true")
    parser.add_argument("--job-id", help="Job id to filter; implies --omit-job-id")
    args = parser.parse_args()

    logger.debug("Arguments: %s", args)

    log_level = parse_log_level(args.log_level)
    if log_level is None:
        parser.error("Invalid log level: {}".format(log_level))
        sys.exit(1)

    operation_alias = args.operation_alias
    if operation_alias.startswith("*"):
        operation_alias = operation_alias[1:]
    logger.info("Operation alias: %s", operation_alias)

    artifact_path = args.artifact_path or "//home/clickhouse-kolkhoz/" + operation_alias
    logger.info("Artifact path: %s", artifact_path)

    trace_id = None
    if args.trace_id:
        trace_id = parse_trace_id(args.trace_id)
        logger.info("Trace id: %s", trace_id)

    input_table_path = get_input_table_path(artifact_path, trace_id_present=trace_id is not None, log_level=log_level)
    logger.info("Input table: %s", input_table_path)

    omit_job_id = args.omit_job_id or args.job_id is not None
    logger.info("Omit job id: %s", omit_job_id)

    conditions = []

    if trace_id:
        conditions.append("trace_id = '{}'".format(trace_id))

    if args.message_like:
        conditions.append("is_substr('{}', message)".format(args.message_like))

    suitable_log_levels = get_suitable_log_levels(log_level)
    conditions.append("log_level in {}".format(suitable_log_levels))

    if args.expression:
        conditions.append(args.expression)

    if args.job_id:
        conditions.append("job_id = '{}'".format(args.job_id))

    query = "* FROM [{}] WHERE {} ORDER BY (timestamp, line_index) LIMIT 10000".format(input_table_path, " AND ".join(conditions))
    logger.info("Query: %s", query)
    print >>sys.stderr, "=" * 80

    rows = select_rows(query, args.limit, args.batch_size)
    for row in rows:
        job_id_prefix = "[{}]\t".format(row["job_id"]) if not omit_job_id else ""
        print "{}{}\t{}\t{}\t{}\t{}\t{}\t{}".format(job_id_prefix, row["timestamp"], row["log_level"],
                                                    row["category"], row["message"], row["thread_id"], row["fiber_id"],
                                                    row["trace_id"])

if __name__ == "__main__":
    main()
