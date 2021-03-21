#!/usr/bin/python2.7 

import argparse
import json
import dateutil.parser
import datetime
import sys

import logging

import grep_logs

logger = logging.getLogger(__name__)

RUN_URL = "https://charts.yandex-team.ru/api/run"

class QueryInvocation(object):
    def __init__(self, request_id, start_time, timings):
        self.request_id = request_id
        self.start_time = start_time
        self.timings = timings
        self.first_log_row = None
        self.last_log_row = None
        self.chyt_start_time = None
        self.chyt_finish_time = None

    def get_start_time(self):
        return self.start_time

    def get_finish_time(self):
        return sum([datetime.timedelta(milliseconds=ms) for ms in self.timings.itervalues()], self.start_time)

    def get_request_id(self):
        return self.request_id

    def on_log_row(self, row):
        assert self.request_id in row["message"]
        if self.first_log_row is None:
            self.first_log_row = row
            self.chyt_start_time = grep_logs.MST.localize(dateutil.parser.parse(row["timestamp"]))
        self.last_log_row = row
        self.chyt_finish_time = grep_logs.MST.localize(dateutil.parser.parse(row["timestamp"]))

    def get_boundary_log_rows(self):
        return (self.first_log_row, self.last_log_row)

    def get_relative_timings(self):
        moments = [
            self.start_time,
            self.start_time + datetime.timedelta(milliseconds=self.timings["blocked"] + self.timings["send"]),
            self.chyt_start_time,
            self.chyt_finish_time,
            self.get_finish_time(),
        ]
        return [next - current for current, next in zip(moments[:-1], moments[1:])]


def find_header(headers, name):
    value = None
    for header in headers:
        if header["name"].lower() == name.lower():
            assert value is None
            value = header["value"]
    return value

def trim_timings(timings):
    return {key: value for key, value in timings.iteritems() if key in \
        ("blocked", "send", "wait", "receive")}

def parse_invocation(entry):
    request_id = find_header(entry["request"]["headers"], "X-Request-ID").encode("ascii")
    logger.debug("Request id is %s", request_id)
    start_time = dateutil.parser.parse(entry["startedDateTime"]).astimezone(grep_logs.MST)
    logger.debug("Start time is %s", start_time)
    timings = trim_timings(entry["timings"])
    logger.debug("Timings are %s", timings)

    return QueryInvocation(request_id, start_time, timings)


def is_query_invocation(request):
    return request["method"] == "POST" and request["url"] == RUN_URL and \
        find_header(request["headers"], "X-Request-ID") is not None

def parse_harfile(harfile_path):
    harfile = json.load(open(harfile_path, "r"))
    entries = harfile["log"]["entries"]
    logger.debug("There are %d entries in harfile", len(entries))
    invocations = []
    for i, entry in enumerate(entries):
        if is_query_invocation(entry["request"]):
            logger.debug("Entry %d looks like a query invocation", i)
            try:
                invocation = parse_invocation(entry)
                invocations.append(invocation)
                logger.debug("Entry %d is an invocation with X-Request-ID %s", i, invocation.request_id)
            except Exception:
                logger.exception("Entry %d is malformed: caught following error while parsing", i)
    return invocations

def setup_logging(verbose):
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s\t%(levelname).1s\t%(module)s:%(lineno)d\t%(message)s")

    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(formatter)
    stderr_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    logging.root.addHandler(stderr_handler)

def main():
    parser = argparse.ArgumentParser(description="Analyze harfile of opening one dashboard")
    parser.add_argument("HARFILE", type=str, help=".har file to analyze")
    parser.add_argument("--clique", type=str, default="ch_datalens", help="Alias of the clique serving the dashboard")
    parser.add_argument("--log-level", type=str, default="info", help="Log level of logs to use")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print lots of debugging information")
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    grep_logs.setup_logging(args.verbose)

    invocations = parse_harfile(args.HARFILE)

    min_start_time = min(invocation.get_start_time() for invocation in invocations)
    max_finish_time = max(invocation.get_finish_time() for invocation in invocations)

    logger.info("Parsed %d invocations, min_start_time = %s, max_finish_time = %s", len(invocations), min_start_time, max_finish_time)

    EPS = datetime.timedelta(seconds=10)
    WINDOW_SIZE = datetime.timedelta(minutes=1)

    from_ts = min_start_time - EPS
    to_ts = max_finish_time + EPS

    substr_condition = " OR ".join("is_substr('{}', message)".format(invocation.get_request_id()) 
                                   for invocation in invocations)

    debug_suffix = "" if args.log_level == "info" else ".debug"

    rows = grep_logs.select_rows_batched("//sys/clickhouse/kolkhoz/{}/clickhouse{}.log".format(args.clique, debug_suffix),
                                         [substr_condition], from_ts, to_ts, WINDOW_SIZE, False)

    for row in rows:
        matching_invocation = None
        for invocation in invocations:
            if invocation.get_request_id() in row["message"]:
                assert matching_invocation is None
                matching_invocation = invocation
        assert matching_invocation is not None
        matching_invocation.on_log_row(row)
    
    if args.verbose:
        for invocation in invocations:
            first_row, last_row = invocation.get_boundary_log_rows()
            if first_row is None or last_row is None:
                logger.warn("There are no boundary rows for request %s (start_time = %s, finish_time = %s)", 
                            invocation.get_request_id(), invocation.get_start_time(), invocation.get_finish_time())
                continue
            logger.debug("Boundary rows for request %s (start_time = %s, finish_time = %s) are:", 
                         invocation.get_request_id(), invocation.get_start_time(), invocation.get_finish_time())
            grep_logs.print_row(first_row, file=sys.stderr)
            grep_logs.print_row(last_row, file=sys.stderr)

    for invocation in invocations:
        timings = invocation.get_relative_timings()
        print "%s: start -- %f sec -- sent to run -- %f sec -- received in CHYT -- %f sec -- finished in CHYT -- %f sec -- finish" % \
            (invocation.get_request_id(), timings[0].total_seconds(), timings[1].total_seconds(), timings[2].total_seconds(), timings[3].total_seconds())


if __name__ == "__main__":
    main()
