#!/usr/bin/env python

import yt.packages.requests as requests

import yt.wrapper as yt

import sys
#import time
import socket
import logging
import argparse
from datetime import datetime

class FennelError(Exception):
    pass

logger = logging.getLogger("fennel")

SERVICE_ID = "yt"
EVENT_LOG_PATH = "//sys/scheduler/event_log"

#def _read_event_log(yt_client, table_name, minimal_row_count):
#    with yt_client.Transaction():
#        yt_client.lock(table_name, mode="snapshot")
#        logbroker_row_index = yt_client.get(table_name + "/@logbroker_row_index")
#        row_count = yt_client.get(table_name + "/@row_count")
#
#        if row_count - logbroker_row_index < minimal_row_count:
#            return []
#
#        start_row_index = logbroker_row_index
#        end_row_index = logbroker_row_index + minimal_row_count
#        logging.info("Reading %s[%d:%d]", table_name, start_row_index, end_row_index)
#        table_path = yt.TablePath(table_name, start_index=start_row_index, end_index=start_row_index + row_count, client=yt_client)
#        rows = yt_client.read_table(table_path, format=yt.JsonFormat(), raw=False)
#
#        if len(rows) != end_row_index - start_row_index:
#            raise Exception("Something goes wrong")
#
#        return rows
#
#def _advance_logbroker_row_index(yt_client, table, row_count):
#    with yt_client.Transaction():
#        yt_client.lock(table, mode="exclusive")
#        logbroker_row_index = yt_client.get(table + "/@logbroker_row_index")
#        yt_client.set(table+ "/@logbroker_row_index", logbroker_row_index + row_count)
#
#
#def write_to_logbroker(logbroker, yt_client, table, minimal_row_count):
#    logbroker.start_session()
#    while True:
#        rows = _read_event_log(yt_client, table, minimal_row_count)
#        if not rows:
#            time.sleep(1.0)
#            continue
#        
#        logbroker.write(rows)
#        _advance_logbroker_row_index(yt_client, table, len(rows))
#
#def write(yt_client, session_id, start_row_index, end_row_index, seqno):
#    requests.get
#    """
#    /rt/store
#    """
#    pass

# ================= LOGBROKER HELPERS ==========================================

def get_logbroker_hostname(url):
    #log.info("Getting adviced logbroker endpoint hostname for %s...", logbroker_url)
    headers = {"ClientHost": socket.getfqdn(), "Accept-Encoding": "identity"}
    response = requests.get("http://{0}/advise".format(url), headers=headers)
    if not response.ok:
        #log.warning("Unable to get adviced logbroker endpoint hostname. Response: %s", response.content)
        raise RuntimeError("Unable to get adviced logbroker endpoint hostname")
    host = response.text.strip()

    #log.info("Adviced endpoint hostname: %s", host)
    return host


class LogBroker(object):
    def __init__(self, url, log_type, source_id, chunk_size):
        self._url = url
        self._source_id = source_id
        self._log_type = log_type
        self._chunk_size = chunk_size

        self._cached_url = None

    def _make_source_id(self, session_index, session_count):
        return "{0}_{1}_{2}".format(self._source_id, session_count, session_index + 1)
    
    def get_session_params(self, session_index, session_count):
        return {"ident": SERVICE_ID, "logtype": self._log_type, "sourceid": self._make_source_id()}

    def get_url(self):
        if self._cached_url is None:
            self._cached_url = get_logbroker_hostname(self._url)
        return self._cached_url

    def get_chunk_size(self):
        return self._chunk_size

# ==============================================================================

def make_yt_client(args):
    return yt.YtClient(args.yt_proxy, config=args.yt_config)

def make_logbroker_client(args):
    return LogBroker(args.logbroker_url, args.log_type, args.source_id, args.chunk_size)

# ================= MONITOR ====================================================

def monitor(yt_client, threshold):
    """ threshold in hours """
    try:
        now = datetime.datetime.utcnow()
        last_processed_time_str = yt_client.get(EVENT_LOG_PATH + "/@last_save_ts")
        last_processed_time = datetime.strptime(last_processed_time_str, "%Y-%m-%d %H:%M:%S")
        lag = now - last_processed_time
    except Exception:
        logger.error("Failed to run monitoring. Unhandled exception", exc_info=True)
        sys.stdout.write("2; Internal error\n")
    else:
        if lag.total_seconds() > threshold * 60:
            sys.stdout.write("2; Lag equals to: %s\n" % (lag,))
        else:
            sys.stdout.write("0; Lag equals to: %s\n" % (lag,))

def parse_monitor(args):
    client = make_yt_client(args)
    monitor(client, args.threshold)

def add_monitor_parser(subparsers):
    parser = subparsers.add_parser("monitor", help="Juggler compatible monitor of event_log state")
    parser.add_argument("--threshold", type=int, help="Maximum value of allowed lag in minutes", default=60)
    parser.set_defaults(func=parse_monitor)

# ================= PUSH TO LOGBROKER  =========================================

def convert_rows_to_chunks(rows, chunk_size):
    # TODO
    yield "aaaa", 15

def make_read_tasks(session_count, row_limit):
    # TODO
    seqno = 0
    return [(index, seqno, []) for index in xrange(session_count)]

def pipe_from_yt_to_logbroker(yt_client, logbroker, session_params, seqno, ranges):
    session_rsp = requests.get("http://{}/rt/session".format(logbroker.get_url()), params=session_params, stream=True)

    rows = yt_client.read_table(yt.TablePath(EVENT_LOG_PATH, ranges=ranges))
    for chunk, row_count in convert_rows_to_chunks(rows, logbroker.chunk_size):
        store_rsp = requests.put("http://{}/rt/store".format(logbroker.get_url()), data=chunk)
        store_rsp.raise_for_status()

        while True:
            line = session_rsp.readline()
            if line.startswith("ping"):
                continue
            if any(line.startswith(marker) for marker in ["eof", "error", "dc-off"]):
                raise FennelError("Failed to write chunk: received message %r", line)
            if line.startswith("skip"):
                logger.info("Chunk skipped due to duplication")

            # Add more checks.
            assert line.startswith("chunk")

    session_rsp.close()

def push_to_logbroker(yt_client, logbroker, session_count, task_row_limit, lock_path):
    tasks = make_read_tasks(session_count, task_row_limit)
    if session_count == 1:
        session_index, seqno, ranges = tasks[0]
        pipe_from_yt_to_logbroker(yt_client, logbroker, logbroker.get_session_params(session_index), seqno, ranges)
    else:
        # TODO(operation)
        pass

def parse_push_to_logbroker(args):
    yt_client = make_yt_client(args)
    logbroker = make_logbroker_client(args)
    push_to_logbroker(yt_client, logbroker, session_count=args.session_count, lock_path=args.lock_path)

def add_push_to_logbroker_parser(subparsers):
    parser = subparsers.add_parser("push-to-logbroker",
                                   help="Tail scheduler event log and push data to logbroker")
    parser.add_argument("--session-count", type=int,
                        help="Number of parallel sessions to tail and push", default=1)
    parser.add_argument("--source-id", required=True,
                        help="Source id for log broker. If used more than one session then "
                             "'_NUM' will be added to session id")
    parser.add_argument("--logbroker-url", required=True,
                        help="Url of logbroker")
    parser.add_argument("--log-type", help="Name of log type that used by logbroker.", default="yt-scheduler")
    parser.add_argument("--lock-path", help="Path in cypress to avoid running more than one instance of fennel")
    parser.set_defaults(func=parse_push_to_logbroker)

# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Fennel: tool for processing YT shceduler event log")
    parser.add_argument("--yt-proxy", help="yt proxy")
    parser.add_argument("--yt-config", help="yt config")

    subparsers = parser.add_subparsers(help="Command: monitor or push-to-logbroker", metavar="command")
    add_monitor_parser(subparsers)
    add_push_to_logbroker_parser(subparsers)
    
    args, other = parser.parse_known_args()
    args.func(args)

    # Attributes
    # TODO(ignat): rename
    # archived_row_count
    # last_saved_ts
    # processed_row_count
    #
    # TODO(ignat): add
    # global_processes_row_count

if __name__ == "__main__":
    main()
