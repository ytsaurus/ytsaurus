#!/usr/bin/python

import yt.wrapper as yt

import argparse
import logging
import sys
import datetime
import dateutil.parser
import pytz

logger = logging.getLogger(__name__)

LOG_LEVEL_LONG_NAMES = ["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "FATAL"]
LOG_LEVEL_SHORT_NAMES = [level[:1] for level in LOG_LEVEL_LONG_NAMES]

MST = pytz.timezone("Europe/Moscow")

def setup_logging(verbose):
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(levelname)s\t%(message)s")
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel([logging.FATAL, logging.INFO, logging.DEBUG][min(verbose, 2)])
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


def select_rows(query):
    logger.debug("Making query: %s", query)
    return yt.select_rows(query)


def parse_timedelta(timedelta):
    try:
        quantity = int(timedelta[:-1])
    except ValueError:
        return None
    if timedelta.endswith("s"):
        return datetime.timedelta(seconds=quantity)
    elif timedelta.endswith("m"):
        return datetime.timedelta(minutes=quantity)
    elif timedelta.endswith("h"):
        return datetime.timedelta(hours=quantity)
    elif timedelta.endswith("d"):
        return datetime.timedelta(days=quantity)
    else:
        return None


def parse_ts(ts, default=None):
    if ts is None:
        return default
    add = datetime.timedelta()
    if ts.endswith("Z"):
        ts = ts[:-1]
        add = datetime.timedelta(hours=3)
    try:
        return MST.localize(dateutil.parser.parse(ts) + add)
    except ValueError:
        return None


def format_ts(ts):
    return ts.astimezone(MST).isoformat(" ").replace(".", ",")

LIMIT = 1000000

def format_query(input_table_path, conditions):
    return "* FROM [{}] WHERE {} ORDER BY (timestamp, increment) LIMIT {}".format(input_table_path, " AND ".join("({})".format(condition) for condition in conditions), LIMIT)


def get_current_time():
    # :))))))
    try:
        yt.list("//non/existent/path")
    except yt.YtHttpResponseError as err:
        return parse_ts(err.inner_errors[0]["attributes"]["datetime"])


def select_rows_batched(input_table_path, conditions, from_ts, to_ts, window_size, tail):
    if window_size is None:
        logger.debug("Window batching is disabled, making single query")
        for row in select_rows(format_query(input_table_path, conditions)):
            yield row
    else:
        cur_ts = from_ts
        batch_index = 0
        while cur_ts <= to_ts:
            lhs = format_ts(cur_ts)
            rhs = format_ts(min(to_ts, cur_ts + window_size))
            logger.debug("Running batch of size %s from %s to %s", window_size, lhs, rhs)
            new_conditions = conditions + ["timestamp BETWEEN '{}' AND '{}'".format(lhs, rhs)]
            subquery = format_query(input_table_path, new_conditions)
            logger.debug("Subquery: %s", subquery)
            row_count = 0
            last_row_ts = None
            incomplete = False
            for row in yt.select_rows(subquery):
                row_count += 1
                last_row_ts = parse_ts(row["timestamp"])
                if batch_index > 0 or not tail:
                    yield row
            logger.debug("Last batch consisted of %d rows, last row ts = %s", row_count, last_row_ts)
            if row_count == LIMIT:
                incomplete = True
                new_window_size = window_size // 2
                logger.warning("Last batch hit the limit, decreasing window-size from %s to %s", window_size, new_window_size)
                window_size = new_window_size
            elif tail:
                incomplete = True
            if incomplete:
                if not last_row_ts:
                    logger.debug("Retrying last batch")
                    continue
                else:
                    logger.debug("Moving current ts to last row ts")
                    batch_index += 1
                    cur_ts = last_row_ts
            else:
                batch_index += 1
                cur_ts += window_size

def print_row(row, omit_job_id=False, raw=False, file=sys.stdout):
    job_id_prefix = "[{}]\t".format(row["job_id"]) if not omit_job_id else ""
    line = None
    if raw:
        line = "{}{}".format(job_id_prefix, row)
    else:
        line = "{}{}\t{}\t{}\t{}\t{}\t{}\t{}".format(job_id_prefix, row["timestamp"], row["log_level"],
                                                     row["category"], row["message"], row["thread_id"], row["fiber_id"],
                                                     row["trace_id"])
    print >>file, line
    

def main():
    description = """
    Grep logs from log tailer dynamic table

    Examples:
        * Find all initial queries from beginning of time till now:
          ./grep_logs.py ch_prestable --log-level info --message-like 'QueryKind: Initial' -w 5s
        * Grep given trace:
          ./grep_logs.py ch_prestable --trace-id 3ce27-02240b70-2a811a92-7b2b4520
        * Find query coordinator:
          ./grep_logs.py ch_prestable --trace-id 3ce27-02240b70-2a811a92-7b2b4520 --message-like 'QueryKind: Initial'
        * Grep only coordinator:
          ./grep_logs.py ch_prestable --trace-id 3ce27-02240b70-2a811a92-7b2b4520 --job-id e4d33a76-24c57246-3fe0384-10284
        * Grep everything from minute to minute:
          ./grep_logs.py ch_prestable --from-ts "12:03" --to-ts "12:05" --window-size 10s
        * Tail mode:
          ./grep_logs.pt ch_prestable --tail --window-size 10s
    """
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("operation_alias", help="Operation alias of clique to grep (either with * or without)")
    parser.add_argument("--message-like", help="Message pattern substring")
    parser.add_argument("--artifact-path", help="Path of artifact directory of clique; by default "
                                                "//home/clickhouse-kolkhoz/<operation_alias>")
    parser.add_argument("--trace-id", help="Trace id (either with dashes or without)")
    parser.add_argument("--log-level", help="Minimum log level to grep, default is debug; case-insensitive, either "
                                            "first char or full level name", default="debug")
    parser.add_argument("--expression", help="Arbitrary boolean condition to append to query")
    parser.add_argument("--limit", help="Leave that many lines", type=int, default=10**9)
    parser.add_argument("--omit-job-id", help="Do not print job-id", action="store_true")
    parser.add_argument("--job-id", help="Job id to filter; implies --omit-job-id")
    parser.add_argument("--from-ts", help="'From' timestamp, empty means the creation time of table; "
                                          "for example 2019-07-12 10:00 or 05:05:42")
    parser.add_argument("--to-ts", help="'To' timestamp, empty means now() + 5min; see --from-ts for examples")
    parser.add_argument("-w", "--window-size", help="Size of time window for batching; for example 10min, 5sec, 1hr, "
                                                    "1day")
    parser.add_argument("--tail", help="Imitate tail -f; from-ts is assumed to be now()", action="store_true")
    parser.add_argument("--raw", help="Print raw dicts rather than formatted log lines", action="store_true")
    parser.add_argument('-v', '--verbose', action='count', default=0, help="Log stuff, more v's for more logging")
    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.debug("Arguments: %s", args)

    log_level = parse_log_level(args.log_level)
    if log_level is None:
        parser.error("Invalid log level: {}".format(log_level))
        sys.exit(1)

    window_size = None
    if args.window_size is not None:
        window_size = parse_timedelta(args.window_size)
        if window_size is None:
            parser.error("Invalid window size: {}".format(args.window_size))
            sys.exit(1)

    from_ts = parse_ts(args.from_ts, default=datetime.datetime.min)
    if from_ts is None:
        parser.error("Invalid from ts: {}".format(args.from_ts))
        sys.exit(1)

    to_ts = parse_ts(args.to_ts, default=get_current_time() + datetime.timedelta(minutes=5))
    if to_ts is None:
        parser.error("Invalid to ts: {}".format(args.to_ts))
        sys.exit(1)

    operation_alias = args.operation_alias
    if operation_alias.startswith("*"):
        operation_alias = operation_alias[1:]
    logger.info("Operation alias: %s", operation_alias)

    artifact_path = args.artifact_path or "//home/clickhouse-kolkhoz/" + operation_alias
    logger.info("Artifact path: %s", artifact_path)

    input_table_path = get_input_table_path(artifact_path, trace_id_present=args.trace_id is not None, log_level=log_level)
    creation_time = parse_ts(yt.get(input_table_path + "/@creation_time"))
    logger.info("Input table: %s, creation_time: %s", input_table_path, format_ts(creation_time))
    from_ts = max(from_ts, creation_time)

    if args.tail:
        from_ts = get_current_time() - datetime.timedelta(seconds=10)

    omit_job_id = args.omit_job_id or args.job_id is not None
    logger.info("Omit job id: %s", omit_job_id)

    conditions = []

    if args.trace_id:
        logger.info("Trace id: %s", args.trace_id)
        conditions.append("trace_id = '{}'".format(args.trace_id))

    if args.message_like:
        conditions.append("is_substr('{}', message)".format(args.message_like))

    suitable_log_levels = get_suitable_log_levels(log_level)
    conditions.append("log_level in {}".format(suitable_log_levels))

    if args.expression:
        conditions.append(args.expression)

    if args.job_id:
        conditions.append("job_id = '{}'".format(args.job_id))

    logger.info("Base query: %s", format_query(input_table_path, conditions))
    logger.info("=" * 80)

    rows = select_rows_batched(input_table_path, conditions, from_ts, to_ts, window_size, args.tail)
    for index, row in enumerate(rows):
        if index >= args.limit:
            break
        print_row(row, omit_job_id=omit_job_id, raw=args.raw, file=sys.stdout)


if __name__ == "__main__":
    main()
