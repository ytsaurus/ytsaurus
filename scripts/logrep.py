#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import argparse
import collections
import datetime
import json
import logging
import os
import re
import sys
import subprocess
import gzip
import socket

FQDN = socket.getfqdn()

OPERATION_OBJECT_TYPE = 1000
SCHEDULER_JOB = 900

GUID_RE = re.compile("[a-fA-F0-9]+-[a-fA-F0-9]+-[a-fA-F0-9]+-[a-fA-F0-9]+")
LOG_FILE_NAME_RE = re.compile(r"^(?P<app_and_host>[^.]+)[.]debug[.]log([.](?P<log_index>\d+))?([.]gz)?$")

LogName = collections.namedtuple("LogName", ["application", "host", "log_index"])

logging.basicConfig(format="%(asctime)s\t" + FQDN + "\t%(levelname)s: %(message)s", level=logging.INFO)


class LogrepError(RuntimeError):
    def __init__(self, *args, **kwargs):
        RuntimeError.__init__(self, *args, **kwargs)
        self.epilog = kwargs.get("epilog", "Error occurred, exiting...")


def shell_quote(s):
    return "'" + s.replace("'", "'\\''") + "'"


def item_per_line(items, indent=0):
    indent_str = " " * indent
    return "".join(indent_str + item + "\n" for item in items)


def parse_log_filename(filename):
    m = LOG_FILE_NAME_RE.match(filename)
    if not m:
        return None
    application = m.group("app_and_host")
    hostname = FQDN.split(".")[0]
    expected_suffix = "-" + hostname
    if application.endswith(expected_suffix):
        application = application[:-len(expected_suffix)]
    index = m.group("log_index")
    index = int(index) if index else 0
    return LogName(application, hostname, index)


class Task(object):
    @staticmethod
    def _datetime_to_str(arg):
        if isinstance(arg, datetime.datetime):
            return arg.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(arg, (str, unicode)):
            return arg
        else:
            raise TypeError("arg has type: {}".format(type(arg)))

    def __init__(self, pattern, server, start_time, end_time, application=None):
        self.pattern = pattern
        self.start_time = self._datetime_to_str(start_time)
        self.end_time = self._datetime_to_str(end_time)
        self.application = application
        self.server = server

    @staticmethod
    def try_switch_to_server_task_mode():
        try:
            task_str = os.environ.get("LOGR_TASK", None)
            if task_str is None:
                return None
            task = Task._load(task_str)
            task._local_run_impl()
        except LogrepError as e:
            print >>sys.stderr, str(e)
            exit(1)
        exit(0)

    def remote_run(self):
        with open(__file__, "r") as srcf:
            logging.info("connecting to {}".format(self.server))
            retcode = subprocess.call(
                [
                    "ssh",
                    "-o", "StrictHostKeyChecking=no",
                    self.server,
                    "LOGR_TASK={} python /dev/stdin".format(
                        shell_quote(self._save())
                    )
                ],
                stdin=srcf
            )
            if retcode != 0:
                raise LogrepError("Remote process exited with nonzero exit code.")

    def _local_run_impl(self):
        actual_application = chdir_to_application_logs(self.application)
        file_to_grep_list = find_files_to_grep(actual_application, self.start_time, self.end_time)
        if not file_to_grep_list:
            raise LogrepError("Cannot find log files for time interval: {} - {}".format(self.start_time, self.end_time))

        for f in file_to_grep_list:
            logging.info("would grep {}".format(f))

        cmd = ["zfgrep", self.pattern] + file_to_grep_list
        logging.info("running {}".format(" ".join(map(shell_quote, cmd))))

        subprocess.call(["stdbuf", "-oL"] + cmd)

    def _save(self):
        return json.dumps(
            {
                "pattern": self.pattern,
                "start_time": self.start_time,
                "end_time": self.end_time,
                "application": self.application,
                "server": self.server
            }
        )

    @classmethod
    def _load(cls, s):
        return Task(**json.loads(s))


#
# Searching for logs
#

def chdir_to_application_logs(application):
    # 1. Понять, где лежат логи
    if not os.path.isdir("/yt"):
        raise LogrepError("/yt doesn't exist or not a directory")
    os.chdir("/yt")
    log_dirs = {}
    if os.path.isdir("logs"):
        os.chdir("logs")
        for name in os.listdir("."):
            parsed = parse_log_filename(name)
            if parsed is None:
                continue
            log_dirs[parsed.application] = "."
    else:
        for name in os.listdir("."):
            if name.endswith("-logs") and os.path.isdir(name):
                log_dirs[name[:-len("-logs")]] = name

    if not log_dirs:
        raise LogrepError("Cannot find any application logs on this server.")

    if application is None:
        if len(log_dirs) == 1:
            application = next(log_dirs.keys())
        else:
            raise LogrepError(
                "Please specify application which logs to grep. Applications found on this server:\n"
                "{}".format(item_per_line(sorted(log_dirs), indent=2))
            )
    elif application not in log_dirs:
        raise LogrepError(
            "Application: {application} is not found on server. Available applications on this server:\n"
            "{available_applications}\n".format(
                application=application,
                available_applications=item_per_line(sorted(log_dirs), indent=2)
            )
        )
    os.chdir(log_dirs[application])
    return application


def find_files_to_grep(application, start_time_str, end_time_str):
    def binsearch_first_log_greater_than(log_list, key):
        def first_line(idx):
            if not (0 <= idx < len(log_list)):
                raise IndexError("idx: {} is out of range [0, {})".format(idx, len(log_list)))
            return get_first_file_line(log_list[idx])
        begin = 0
        end = len(log_list)
        if end == begin:
            return -1
        if key <= first_line(begin):
            return -1
        if first_line(end - 1) < key:
            return end
        while True:
            # assert first_line(begin) <= key <= first_line(end - 1)
            if end - begin <= 1:
                return end

            try_idx = begin + (end - begin) / 2
            line = first_line(try_idx)
            c = cmp(line, key)
            if c == 0:
                return try_idx
            elif c == -1:
                assert end - try_idx < end - begin
                begin = try_idx
            else:
                assert try_idx - begin < end - begin
                end = try_idx

    def pick_files(log_list):
        begin_idx = binsearch_first_log_greater_than(log_list, start_time_str)
        end_idx = binsearch_first_log_greater_than(log_list, end_time_str)
        begin_idx -= 1
        if begin_idx < 0:
            begin_idx = 0
        if end_idx < 0:
            end_idx = 0
        return log_list[begin_idx:end_idx]

    log_file_list = []
    if os.path.exists("archive"):
        for filename in os.listdir("archive"):
            if ".debug.log" not in filename:
                continue
            log_file_list.append(os.path.join("archive", filename))
        log_file_list.sort()
    current_logs = []
    for name in os.listdir("."):
        parsed = parse_log_filename(name)
        if parsed is None or parsed.application != application:
            continue
        current_logs.append((parsed.log_index, name))
    log_file_list += (f for _, f in sorted(current_logs, reverse=True))
    return pick_files(log_file_list)


def get_first_file_line(filename):
    if filename.endswith(".gz"):
        open_func = gzip.open
    else:
        open_func = open
    with open_func(filename) as inf:
        return inf.readline()


#
# Time parsing
#

def parse_time(time_str):
    if re.match("\d\d:\d\d:\d\d", time_str):
        now = datetime.datetime.now()
        parsed = datetime.datetime.strptime(time_str, "%H:%M:%S")
        result = now.replace(hour=parsed.hour, minute=parsed.minute, second=parsed.second, microsecond=0)
        if result > now:
            raise LogrepError("Date {0} is in the future".format(result))
        return result
    if re.match("\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d", time_str):
        return datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    if re.match("\d{1,2} \w+ \d\d\d\d \d\d:\d\d:\d\d", time_str):
        return datetime.datetime.strptime(time_str, "%d %b %Y %H:%M:%S")
    if re.match("\w+ \w+ \d{1,2} \d\d:\d\d:\d\d \w+ \d\d\d\d", time_str):
        return datetime.datetime.strptime(time_str, "%a %b %d %H:%M:%S %Z %Y")
    else:
        raise LogrepError("Don't know how to parse time: {0}".format(time_str))


#
# Server resolving
#

def resolve_server(server_str, application):
    if "@" not in server_str:
        return server_str
    import yt.wrapper
    obj, cluster = server_str.split("@", 1)
    client = yt.wrapper.YtClient(cluster)
    if GUID_RE.match(obj):
        t = yt.wrapper.common.object_type_from_uuid(obj)
        if t == OPERATION_OBJECT_TYPE:
            return resolve_server_for_operation(client, operation_id=obj, application=application)
        else:
            raise LogrepError("Cannot recognize guid: {}".format(obj))
    raise LogrepError("Cannot resolve server for: {}".format(server_str))


def resolve_server_for_operation(client, operation_id, application):
    supported_applications = (
        "Supported applications:\n"
        "  scheduler\n"
        "  controller-agent\n"
    )
    if application is None:
        raise LogrepError(
            "You must specify application to autoresolve server for operation-id.\n" + supported_applications
        )
    if application == "scheduler":
        host, _port = client.get("//sys/scheduler/@addresses/default").split(":")
        return host
    elif application == "controller-agent":
        r = client.get_operation(operation_id)
        address = r.get("controller_agent_address", None)
        if address is None:
            raise LogrepError("Cannot resolve controller agent for operation: {}".format(operation_id))
        host, _port = address.split(":")
        return host
    else:
        raise LogrepError("Unsupported application: {}\n".format(application) + supported_applications)


def main():
    Task.try_switch_to_server_task_mode()

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--server", help="server to use", required=True)
    parser.add_argument(
        "-t", "--time", "--start-time",
        help="start of time interval to grep",
        required=True
    )
    parser.add_argument(
        "-e", "--end-time",
        help="end of time interval to grep (equals start interval by default)"
    )
    parser.add_argument(
        "-a", "--application",
        help="application which logs we want to grep (by default logr will try to determine it automatically"
    )
    parser.add_argument("pattern")
    args = parser.parse_args()

    dt = datetime.timedelta(seconds=10)
    start_time = parse_time(args.time)

    if args.end_time is None:
        end_time = start_time
    else:
        end_time = parse_time(args.end_time)

    start_time -= dt
    end_time += dt

    server = resolve_server(args.server, args.application)

    task = Task(
        pattern=args.pattern,
        server=server,
        start_time=start_time,
        end_time=end_time,
        application=args.application
    )

    task.remote_run()


if __name__ == "__main__":
    def run():
        try:
            main()
        except LogrepError as e:
            print >>sys.stderr, str(e)
            if e.epilog:
                print >>sys.stderr, e.epilog
            exit(1)
    run()
