#!/usr/bin/env python2
# -*- encoding: utf-8 -*-

import argparse
import collections
import datetime
import errno
import getpass
import gzip
import hashlib
import json
import logging
import os
import Queue
import re
import signal
import socket
import subprocess
import sys
import threading
import time

# NOTE: we should not import nonstandard libraries here because this script is being uploaded to remote server
# which might miss such library

FQDN = socket.getfqdn()

CHUNK_GUID_TYPE = 100
ERASURE_CHUNK_GUID_TYPE = 102
TABLET_CELL_GUID_TYPE = 700
TABLET_GUID_TYPE = 702
JOB_GUID_TYPE = 900
OPERATION_GUID_TYPE = 1000

GUID_RE = re.compile("^[a-fA-F0-9]+-[a-fA-F0-9]+-[a-fA-F0-9]+-[a-fA-F0-9]+$")
LOG_FILE_NAME_RE = re.compile(r"^(?P<app_and_host>[^.]+)[.]debug[.]log([.](?P<log_index>\d+))?([.]gz)?$")
CLUSTER_SERVICE_INSTANCE_SELECTOR_RE = re.compile(r"^(/[^/]+)+$")

ParsedLogName = collections.namedtuple("ParsedLogName", ["log_prefix", "log_index"])

logging.basicConfig(format="%(asctime)s\t" + FQDN + "\t%(levelname)s: %(message)s", level=logging.INFO)

ServiceInfo = collections.namedtuple("ServiceInfo", [
    "name",
    "list_func_name",
    "application_name",
])

SERVICE_TABLE = [
    ServiceInfo("controller-agent", "get_controller_agent_list", "ytserver-controller-agent"),
    ServiceInfo("master", "get_master_list", "ytserver-master"),
    ServiceInfo("node", "get_node_list", "ytserver-node"),
    ServiceInfo("primary-master", "get_primary_master_list", "ytserver-master"),
    ServiceInfo("proxy", "get_proxy_list", "ytserver-http-proxy"),
    ServiceInfo("rpc-proxy", "get_rpc_proxy_list", "ytserver-proxy"),
    ServiceInfo("scheduler", "get_scheduler_list", "ytserver-scheduler"),
    ServiceInfo("secondary-master", "get_secondary_master_list", "ytserver-master"),
]
SERVICE_MAP = {info.name: info for info in SERVICE_TABLE}

CONTAINER_RE_TEMPLATE = r"^ISS-AGENT.*_yt_.*_{service}_.*/iss_hook_start$"

ApplicationInfo = collections.namedtuple("ApplicationInfo", ["name", "log_dir_name", "log_prefix", "container_re"])
APPLICATION_TABLE = [
    ApplicationInfo("ytserver-controller-agent", "controller-agent", "controller-agent",
                    CONTAINER_RE_TEMPLATE.format(service="controller_agent")),
    ApplicationInfo("ytserver-http-proxy", "proxy", "http-proxy",
                    CONTAINER_RE_TEMPLATE.format(service="proxies")),
    ApplicationInfo("ytserver-master", "master", "master",
                    CONTAINER_RE_TEMPLATE.format(service="masters")),
    ApplicationInfo("ytserver-node", "node", "node",
                    CONTAINER_RE_TEMPLATE.format(service="nodes")),
    ApplicationInfo("ytserver-proxy", "rpc-proxy", "proxy",
                    CONTAINER_RE_TEMPLATE.format(service="rpc_proxies")),
    ApplicationInfo("ytserver-scheduler", "scheduler", "scheduler",
                    CONTAINER_RE_TEMPLATE.format(service="schedulers")),
]
APPLICATION_MAP = {info.name: info for info in APPLICATION_TABLE}

CHYT_PATH_MAP = {
    "master":    "//home/logfeller/logs/yt-raw-master-log",
    "scheduler": "//home/logfeller/logs/yt-raw-scheduler-log",
    "proxy":     "//home/logfeller/logs/yt-raw-http-proxy-log",
}

assert all(service.application_name in APPLICATION_MAP for service in SERVICE_TABLE)

LOG_PREFIX_MAP = {info.log_prefix: info for info in APPLICATION_TABLE}
LOG_DIR_NAME_MAP = {info.log_dir_name : info for info in APPLICATION_TABLE}

SSH_OPTS = [
    "-o", "StrictHostKeyChecking=no",
    "-o", "CheckHostIP=no",
]

CLUSTER_MAP = {
    2: "socrates",
    6: "seneca-sas",
    8: "locke",
    10: "flux",
    11: "pythia",
    12: "freud",
    14: "vanga",
    22: "hahn",
    23: "hume",
    27: "ofd",
    28: "perelman",
    29: "markov",
    30: "ofd-myt",
    31: "seneca-man",
    33: "zeno",
    34: "yp-sas",
    35: "yp-man",
    36: "yp-vla",
    39: "bohr",
    40: "yp-sas-test",
    41: "arnold",
    42: "seneca-vla",
    43: "landau",
    44: "yp-man-pre",
    45: "yp-msk",
    46: "yp-xdc",
}


def list_known_services():
    return [service.name for service in SERVICE_TABLE]


def get_application_by_log_prefix(log_prefix):
    if log_prefix not in LOG_PREFIX_MAP:
        return None
    return LOG_PREFIX_MAP[log_prefix].name


def get_application_by_log_dir_name(log_dir_name):
    for suffix in ["", "-logs", "-logs-archive"]:
        if log_dir_name.endswith(suffix):
            app_name = log_dir_name[:-len(suffix)]
            if app_name in LOG_DIR_NAME_MAP:
                return LOG_DIR_NAME_MAP[app_name].name
    return None


class LogrepError(RuntimeError):
    def __init__(self, *args, **kwargs):
        RuntimeError.__init__(self, *args, **kwargs)
        self.epilog = kwargs.get("epilog", "Error occurred, exiting...")

    def __str__(self):
        res = RuntimeError.__str__(self)
        if res.endswith("\n"):
            res = res[:-1]
        return res


def guid_type_name(guid_type):
    return {
        OPERATION_GUID_TYPE: "OperationId",
        JOB_GUID_TYPE: "JobId",
        TABLET_CELL_GUID_TYPE: "TabletCellId",
        TABLET_GUID_TYPE: "TabletId",
        CHUNK_GUID_TYPE: "ChunkId",
        ERASURE_CHUNK_GUID_TYPE: "ErasureChunkId"
    }[guid_type]


def is_guid_selector(selector):
    return selector.startswith("#") or GUID_RE.match(selector)


def extract_guid(selector):
    if selector.startswith("#"):
        n = selector.strip("#")
        if not GUID_RE.match(n):
            raise LogrepError("Bad guid selector: {}".format(selector))
        selector = n
    return selector


def cell_id_from_guid(guid):
    assert GUID_RE.match(guid)
    part = int(guid.split("-")[2], 16)
    return part >> 16


def is_primary_master_cell_id(cell_id):
    return cell_id / 100 == 10


def shell_quote(s):
    return "'" + s.replace("'", "'\\''") + "'"


def indented_lines(items, indent=0):
    indent_str = " " * indent
    return "".join(indent_str + item + "\n" for item in items)


def parse_log_filename(filename):
    m = LOG_FILE_NAME_RE.match(filename)
    if not m:
        return None
    app_and_host = m.group("app_and_host")
    for app in APPLICATION_TABLE:
        if app_and_host.startswith(app.log_prefix):
            app_and_host = app.log_prefix
            break
    index = m.group("log_index")
    index = int(index) if index else 0
    return ParsedLogName(app_and_host, index)


AsyncTaskOutput = collections.namedtuple("AsyncTaskOutput", ["file", "line"])
AsyncTaskCompleted = collections.namedtuple("AsyncTaskCompleted", ["instance_description", "return_code", "stderr_tail"])


def reset_signals():
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)


class Grepper(object):
    """
    class is able to launch find appropriate log and launch process that will grep them
    """
    def __init__(self, application, pattern, start_time, end_time, stdout=None):
        self.application = application
        self.pattern = pattern
        self.start_time = start_time
        self.end_time = end_time
        self.stdout = stdout

    def call(self):
        return self._run_impl(subprocess.call)

    def popen(self):
        return self._run_impl(subprocess.Popen)
    
    def _run_impl(self, fn):
        log_directories = collect_log_directories(self.application)
        log_prefix = APPLICATION_MAP[self.application].log_prefix
        file_to_grep_list = find_files_to_grep(log_directories, log_prefix, self.start_time, self.end_time)
        if not file_to_grep_list:
            raise LogrepError("Cannot find log files for time interval: {} - {}".format(self.start_time, self.end_time))

        for f in file_to_grep_list:
            first_line = get_first_file_line(f).strip()
            logging.info("Would grep {} ({})".format(f, shorten(first_line, 50)))

        cmd = ["zfgrep", "--text", "--no-filename", self.pattern] + file_to_grep_list
        logging.info("Running {}".format(" ".join(map(shell_quote, cmd))))

        cmd = ["stdbuf", "-oL", "-eL"] + cmd

        return fn(cmd, preexec_fn=reset_signals(), stdout=self.stdout)


class RemoteTask(object):
    def save(self):
        raise NotImplementedError("This method must be implemented in subclass")

    @staticmethod
    def try_switch_to_remote_task_mode():
        remote_task_class = os.environ.get("LOGREP_REMOTE_TASK_CLASS", None)
        if remote_task_class is not None:
            args = json.loads(os.environ["LOGREP_REMOTE_TASK_ARGS"])
            task = globals()[remote_task_class](**args)
            try:
                task.run()
            except LogrepError as e:
                print >>sys.stderr, str(e)
                exit(1)
            exit(0)

    def upload_script(self, host):
        with open(__file__, "r") as source_file:
            hash_value = hashlib.md5(source_file.read()).hexdigest()
        remote_file_name = "/tmp/logrep_{}_{}.py".format(hash_value, getpass.getuser())
        subprocess.check_call(
            ["scp"] + SSH_OPTS + [__file__, "{}:{}".format(host, remote_file_name)]
        )
        return remote_file_name

    def popen_on_remote_machine(self, host, **kwargs):
        logging.info("connecting to {}".format(host))
        remote_file_name = self.upload_script(host)
        return subprocess.Popen(
            ["ssh", host] + SSH_OPTS + [
                "LOGREP_REMOTE_TASK_CLASS={} LOGREP_REMOTE_TASK_ARGS={} stdbuf -oL -eL python {}".format(
                    self.__class__.__name__,
                    shell_quote(self.save()),
                    remote_file_name
                )
            ],
            preexec_fn=reset_signals,
            **kwargs
        )

    def exec_on_remote_machine(self, host, allocate_terminal=False):
        logging.info("copying script to {}".format(host))
        remote_file_name = self.upload_script(host)
        logging.info("ssh to {}".format(host))
        cmd = ["ssh", self.host] + SSH_OPTS
        if allocate_terminal:
            cmd += ["-t"]
        cmd += [
            "LOGREP_REMOTE_TASK_CLASS={} LOGREP_REMOTE_TASK_ARGS={} python {}".format(
                self.__class__.__name__,
                shell_quote(self.save()),
                remote_file_name
            )
        ]
        reset_signals()
        os.execlp(
            "ssh",
            *cmd
        )


class GrepTask(RemoteTask):
    @staticmethod
    def _datetime_to_str(arg):
        if isinstance(arg, datetime.datetime):
            return arg.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(arg, (str, unicode)):
            return arg
        else:
            raise TypeError("arg has type: {}".format(type(arg)))

    def __init__(self, pattern, host, start_time, end_time, application=None):
        self.pattern = pattern
        self.start_time = self._datetime_to_str(start_time)
        self.end_time = self._datetime_to_str(end_time)
        self.application = application
        self.host = host

    def remote_run(self):
        self.exec_on_remote_machine(self.host)

    def async_remote_run(self, queue):
        p = self.popen_on_remote_machine(self.host, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        stderr_tail = collections.deque(maxlen=5)

        def stderr_reader():
            for line in p.stderr:
                line = line.rstrip("\n")
                stderr_tail.append(line)
                line = "{host} stderr: {line}".format(host=self.host, line=line)
                queue.put(AsyncTaskOutput(sys.stderr, line))

        def stdout_reader():
            for line in p.stdout:
                line = "{line} - {application}@{host}".format(
                    application=self.application,
                    host=self.host,
                    line=line.rstrip("\n")
                )
                queue.put(AsyncTaskOutput(sys.stdout, line))
        stdout_thread = threading.Thread(target=stdout_reader)
        stdout_thread.start()
        stderr_thread = threading.Thread(target=stderr_reader)
        stderr_thread.start()

        def process_watch():
            p.wait()
            stdout_thread.join()
            stderr_thread.join()
            description = "{application}@{address}".format(address=self.host, application=self.application)
            queue.put(AsyncTaskCompleted(description, p.returncode, stderr_tail))

        watch_thread = threading.Thread(target=process_watch)
        watch_thread.setDaemon(True)
        watch_thread.start()

    def run(self):
        code = Grepper(self.application, self.pattern, self.start_time, self.end_time).call()
        if code <= 1:
            exit(0)
        exit(code)

    def save(self):
        return json.dumps(
            {
                "pattern": self.pattern,
                "start_time": self.start_time,
                "end_time": self.end_time,
                "application": self.application,
                "host": self.host
            }
        )


class SshTask(RemoteTask):
    def __init__(self, host, application):
        self.host = host
        self.application = application

    def save(self):
        return json.dumps(
            {
                "application": self.application,
                "host": self.host,
            }
        )

    def remote_run(self):
        self.exec_on_remote_machine(self.host, allocate_terminal=True)

    def run(self):
        def exec_default_shell():
            logging.info("Launching default login shell")
            os.execlp("bash", "bash", "-l")
        application_info = APPLICATION_MAP[self.application]
        if application_info.container_re is None:
            exec_default_shell()
        container_re = re.compile(application_info.container_re)

        try:
            active_containers = subprocess.check_output(["portoctl", "list", "-r"])
        except OSError, subprocess.CalledProcessError:
            exec_default_shell()

        matched = []
        for line in active_containers.strip("\n").split("\n"):
            container_id, _ = line.split(None, 1)
            if container_re.match(container_id):
                matched.append((container_id, line))
        if len(matched) == 0:
            logging.warning("Cannot find {application} container".format(application=self.application))
            sys.stdout.write(active_containers)
            sys.stdout.flush()
            exec_default_shell()

        if len(matched) > 1:
            logging.warning("Found multiple containers:")
            for _, line in matched:
                print >>sys.stderr, line
            exec_default_shell()

        (container_id, _), = matched
        logging.info("Launching: sudo portoctcl shell {}".format(container_id))
        os.execlp("sudo", "sudo", "portoctl", "shell", container_id)


class GetJobLogsTask(RemoteTask):
    def __init__(self, host, job_id, time):
        self.job_id = job_id
        self.time = time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(time, datetime.datetime) else time
        self.host = host

    def save(self):
        return json.dumps(
            {
                "job_id": self.job_id,
                "time": self.time,
                "host": self.host
            }
        )

    def remote_run(self):
        self.exec_on_remote_machine(self.host)

    def run(self):
        logging.info("Searching for working directory")
        grep_process = Grepper("ytserver-node", self.job_id, self.time, self.time, stdout=subprocess.PIPE).popen()
        working_directory = None
        for line in grep_process.stdout:
            if "Spawning a job proxy" in line:
                m = re.search(r"WorkingDirectory: ([^,)]*)[,)]", line)
                working_directory = m.group(1)
                break
        else:
            raise LogrepError("Cannot find job working directory")

        logging.info("Found working directory: {}".format(working_directory))

        grep_process.stdout.close()
        grep_process.wait()


        logging.info("Serarch for job-proxy log")
        file_to_grep_list = find_files_to_grep([working_directory], "job-proxy", self.time, self.time)

        logging.info("Searching for job logs")

        # We open all descriptors at the same time to reduce probability that
        # job proxy log rotation will screw things up.
        def open_log(fname):
            return (gzip.open if fname.endswith(".gz") else open)(fname)

        descriptor_list = map(open_log, file_to_grep_list)
        for fname, fd in zip(file_to_grep_list, descriptor_list):
            logging.info("Checking {}".format(fname))

            lineiter = iter(fd)

            unknown_job_head = []
            for line in lineiter:
                if "Logging started" in line:
                    unknown_job_head = [line]
                elif unknown_job_head:
                    unknown_job_head.append(line)
                    if len(unknown_job_head) > 100:
                        raise LogrepError("Job proxy log looks broken")
                    if "JobId: " in line:
                        m = re.search(r"JobId: ([^),]*)[,)]", line)
                        if self.job_id != m.group(1):
                            unknown_job_head = None
                        else:
                            for line in unknown_job_head:
                                sys.stdout.write(line)

                            for line in lineiter:
                                sys.stdout.write(line)
                                if "Logging started:" in line:
                                    sys.stdout.flush()
                                    exit(0)

#
# Searching for logs
#

def collect_log_directories(application):
    YT_DIR = "/yt"
    if not os.path.isdir(YT_DIR):
        raise LogrepError("/yt doesn't exist or not a directory")
    log_dirs = {}
    result = []
    for name in os.listdir(YT_DIR):
        if name == "logs":
            result.append(name)
        else:
            cur_app = get_application_by_log_dir_name(name)
            if cur_app == application:
                result.append(name)

    for i in xrange(len(result)):
        result[i] = os.path.join(YT_DIR, result[i])
        possible_archive = os.path.join(result[i], "archive")
        if os.path.isdir(possible_archive):
            result.append(possible_archive)

    if not result:
        raise LogrepError("Cannot find application logs on this server.")

    return result


def find_files_to_grep(log_directories, expected_log_prefix, start_time_str, end_time_str):
    def binsearch_last_log_smaller_than(log_list, key):
        def first_line(idx):
            if not (0 <= idx < len(log_list)):
                raise IndexError("idx: {} is out of range [0, {})".format(idx, len(log_list)))
            return get_first_file_line(log_list[idx])

        def pprint_idx(idx):
            return "{}[{}]".format(log_list[idx], first_line(idx))

        begin = 0
        end = len(log_list)
        if end == begin:
            return -1
        if key <= first_line(begin):
            return -1
        end -= 1
        if first_line(end) < key:
            return end
        while True:
            if end - begin <= 1:
                return begin
            assert first_line(begin) <= key <= first_line(end), "\n{}\n{}\n{}".format(pprint_idx(begin), key, pprint_idx(end))

            try_idx = begin + (end - begin) / 2
            line = first_line(try_idx)
            c = cmp(line, key)
            if c == 0:
                return try_idx
            elif c == -1:
                # line < key
                begin = try_idx
            else:
                # key < line
                end = try_idx

    def pick_files(log_list):
        begin_idx = binsearch_last_log_smaller_than(log_list, start_time_str)
        end_idx = binsearch_last_log_smaller_than(log_list, end_time_str)
        assert end_idx >= begin_idx
        if end_idx < 0:
            return []
        if begin_idx == -1:
            begin_idx = 0
        return log_list[begin_idx:end_idx + 1]

    def post_filter(log_list):
        files_inside_time_interval = []
        max_file_before = None
        max_file_before_first_line = None
        for fname in log_list:
            line = get_first_file_line(fname)
            if start_time_str <= line <= end_time_str:
                files_inside_time_interval.append((line, fname))

            if line < start_time_str:
                if max_file_before_first_line is None or max_file_before_first_line < line:
                    max_file_before = fname
                    max_file_before_first_line = line

        if max_file_before is not None:
            files_inside_time_interval.append((max_file_before_first_line, max_file_before))

        files_inside_time_interval.sort()
        return [f for _, f in files_inside_time_interval]

    log_file_list = []
    for log_dir in log_directories:
        for filename in os.listdir(log_dir):
            if ".debug.log" not in filename:
                continue
            if not filename.startswith(expected_log_prefix):
                continue
            log_file_list.append(os.path.join(log_dir, filename))

    digit_files = []
    date_files = []
    for log_file in log_file_list:
        fname = os.path.basename(log_file)
        m = re.search("[.]debug[.]log[.](\d\d\d\d-\d\d-\d\d[.]\d\d:\d\d)", fname)
        if m:
            date_files.append((m.group(0), log_file))
            continue
        m = re.search("[.]debug[.]log(?P<number>[.]\d+)?([.]gz)?$", fname)
        if m:
            number = int(m.group("number").strip(".")) if m.group("number") else 0
            digit_files.append((number, log_file))
        else:
            raise LogrepError("Unknown name of log file: {}".format(fname))

    digit_files.sort(reverse=True)
    date_files.sort()
    return post_filter(pick_files([f for _, f in date_files]) + pick_files([f for _, f in digit_files]))


def get_first_file_line(filename):
    if filename.endswith(".gz"):
        open_func = gzip.open
    else:
        open_func = open
    with open_func(filename) as inf:
        return inf.readline()


def shorten(line, num):
    assert num > 3
    if len(line) > num - 3:
        return line[:num - 3] + "..."
    return line


#
# Time parsing
#

def utc_to_local(utc_dt):
    epoch = time.mktime(utc_dt.timetuple())
    offset = datetime.datetime.fromtimestamp(epoch) - datetime.datetime.utcfromtimestamp(epoch)
    return utc_dt + offset

def parse_time(time_str, is_start=True):
    try:
        if time_str == "now":
            return datetime.datetime.now()
        elif re.match("^\d\d:\d\d:\d\d$", time_str) or re.match("\d\d:\d\d", time_str):
            now = datetime.datetime.now()
            if len(time_str) == 5:
                parsed = datetime.datetime.strptime(time_str, "%H:%M")
            else:
                parsed = datetime.datetime.strptime(time_str, "%H:%M:%S")
            result = now.replace(hour=parsed.hour, minute=parsed.minute, second=parsed.second, microsecond=0)
            if result > now:
                raise LogrepError("Date {0} is in the future".format(result))
            return result
        elif re.match("^\d\d\d\d-\d\d-\d\d$", time_str):
            if is_start:
                time_str += "T00:00:00"
            else:
                time_str += "T23:59:59"
            return datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
        if re.match("^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d*Z$", time_str):
            return utc_to_local(datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ"))
        if re.match("^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$", time_str):
            return datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        if re.match("^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d$", time_str):
            return datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
        if re.match("^\d{1,2} \w+ \d\d\d\d \d\d:\d\d:\d\d$", time_str):
            return datetime.datetime.strptime(time_str, "%d %b %Y %H:%M:%S")
        if re.match("^\w+ \w+ \d{1,2} \d\d:\d\d:\d\d \w+ \d\d\d\d$", time_str):
            return datetime.datetime.strptime(time_str, "%a %b %d %H:%M:%S %Z %Y")
        raise LogrepError("Don't know how to parse time: {0}".format(time_str))
    except LogrepError as e:
        raise LogrepError(
            str(e) +
            "\nPlease use one of the known time formats.\n\n" +
            TIME_FORMAT_HELP)


#
# Instance resolving
#

Instance = collections.namedtuple("Instance", ["address", "application", "attributes", "hidden_attributes"])

class Selector(object):
    def __init__(self, selector_str, cluster=None, service=None, instance_list=()):
        self._selector_str = selector_str
        self._cluster = cluster
        self._service = service
        self._instance_list = instance_list

    def get_text(self):
        return self._selector_str

    def get_cluster(self):
        return self._cluster

    def get_service(self):
        return self._service

    def iter_instances(self):
        return iter(self._instance_list)


def get_all_instances(selector_list):
    res = []
    for selector in selector_list:
        res += selector.iter_instances()
    return res

def resolve_cluster(guid):
    cell_tag = cell_id_from_guid(guid)
    cluster_index = cell_tag % 100
    cluster = CLUSTER_MAP.get(cluster_index)
    if not cluster:
        raise LogrepError("Unknown cluster index {} (cell tag {})".format(cluster_index, cell_tag))
    return cluster


def resolve_selector(selector_str):
    from yt.wrapper import YtClient

    def _raise(msg=None):
        if msg:
            raise LogrepError("Cannot resolve selector `{}': {}".format(selector_str, msg))
        else:
            raise LogrepError("Cannot resolve selector `{}'".format(selector_str))

    if CLUSTER_SERVICE_INSTANCE_SELECTOR_RE.match(selector_str):
        components = selector_str.strip("/").split("/")

        def _is_service(component):
            return component in SERVICE_MAP
        def _is_cluster(component):
            return component in CLUSTER_MAP.values()

        cluster = None
        service = None

        while components:
            if _is_cluster(components[0]):
                if cluster:
                    _raise("/<cluster> must be specified at most once")
                cluster = components[0]
            elif _is_service(components[0]):
                if service:
                    _raise("/<service> must be specified exactly once")
                service = components[0]
            else:
                break
            components.pop(0)

        if not cluster:
            guids = [extract_guid(c) for c in components if is_guid_selector(c)]
            if guids:
                clusters = set(resolve_cluster(guid) for guid in guids)
                if len(clusters) != 1:
                    _raise("Guids belong to different clusters: ".format(", ".join(clusters)))
                [cluster] = clusters

        if not service or not cluster:
            _raise("instance must start with /<cluster>/<service> or with /<service> and contain a guid")

        client = YtClient(cluster)
        instance_list = resolve_service(client, service, components)
        return Selector(
            selector_str=selector_str,
            cluster=cluster,
            service=service,
            instance_list=instance_list
        )
    elif selector_str.count("@") == 1:
        service, address = selector_str.split("@")
        return Selector(
            selector_str=selector_str,
            cluster=None,
            service=service,
            instance_list= [Instance(address, service, [], [])],
        )
    else:
        _raise()


def resolve_service(client, service_name, selector_list):
    from yt.wrapper.common import object_type_from_uuid

    if service_name not in SERVICE_MAP:
        raise LogrepError(
            "Unknown service: {service}\n"
            "List of known services:\n"
            "{known_services}\n"
            .format(service=service_name, known_services=indented_lines(list_known_services(), indent=2))
        )
    service_info = SERVICE_MAP[service_name]
    instance_list = globals()[service_info.list_func_name](client)
    application = service_info.application_name

    node_job_id_selector = NodeJobIdSelector(client)
    for selector in selector_list:
        filter_func = None
        not_count = 0
        for c in selector:
            if c != "!":
                break
            not_count += 1
        if not_count:
            selector = selector[not_count:]

        if selector.startswith("@"):
            def filter_func(x):
                return instance_match_attribute(x, selector[1:])
        elif is_guid_selector(selector):
            guid = extract_guid(selector)
            guid_type = object_type_from_uuid(guid)

            if guid_type == OPERATION_GUID_TYPE:
                if application == "ytserver-scheduler":
                    def filter_func(x):
                        return instance_match_attribute(x, "active")
                elif application == "ytserver-controller-agent":
                    filter_func = AddressInSet({get_controller_agent_for_operation(client, guid)})
                elif application == "ytserver-node":
                    filter_func = node_job_id_selector.get_operation_id_filter(guid)

            elif guid_type == JOB_GUID_TYPE:
                if application == "ytserver-scheduler":
                    def filter_func(x):
                        return instance_match_attribute(x, "active")
                elif application == "ytserver-node":
                    filter_func = node_job_id_selector.get_job_id_filter(guid)

            elif guid_type in [CHUNK_GUID_TYPE, ERASURE_CHUNK_GUID_TYPE]:
                if application == "ytserver-master":
                    attribute = ("primary" if is_primary_master_cell_id(cell_id_from_guid(guid))
                        else "secondary:{cell:x}".format(cell=cell_id_from_guid(guid)))

                    def filter_func(x):
                        return instance_match_attribute(instance, attribute)
                elif application == "ytserver-node":
                    filter_func = NodeChunkIdFilter(client, guid)

            elif guid_type == TABLET_CELL_GUID_TYPE:
                if application == "ytserver-node":
                    filter_func = NodeTabletCellIdFilter(client, guid)

            elif guid_type == TABLET_GUID_TYPE:
                if application == "ytserver-master":
                    attribute = ("primary" if is_primary_master_cell_id(cell_id_from_guid(guid))
                        else "secondary:{cell:x}".format(cell=cell_id_from_guid(guid)))

                    def filter_func(x):
                        return instance_match_attribute(instance, attribute)
                elif application == "ytserver-node":
                    filter_func = NodeTabletIdFilter(client, guid)

            else:
                raise LogrepError("Don't know what to do with guid of unknown type: {}".format(guid_type))

            if filter_func is None:
                def filter_func(_x):
                    return True
                logging.warn("{guid_type} selector {guid} has no effect on service {service}".format(
                    guid_type=guid_type_name(guid_type),
                    guid=selector,
                    service=service_name
                ))
        else:
            def filter_func(x):
                return selector in parse_address(x.address).host

        if not_count % 2 == 1:
            old_filter_func = filter_func

            def filter_func(x):
                return not old_filter_func(x)
        instance_list = [instance for instance in instance_list if filter_func(instance)]

    return instance_list


def extract_guids(instance_str):
    if not CLUSTER_SERVICE_INSTANCE_SELECTOR_RE.match(instance_str):
        return []
    return [s for s in instance_str.strip("/").split("/") if GUID_RE.match(s)]


class AddressInSet(object):
    def __init__(self, iterable):
        self.filter_set = frozenset(iterable)

    def __call__(self, x):
        return x.address in self.filter_set


class NodeChunkIdFilter(object):
    def __init__(self, client, chunk_id):
        info = client.get("#{}/@".format(chunk_id), attributes=["stored_replicas", "last_seen_replicas"])
        self.chunk_id = chunk_id
        self.stored_replicas = {str(r): r.attributes.get("medium", "") for r in info["stored_replicas"]}
        self.last_seen_replicas = frozenset(str(r) for r in info["last_seen_replicas"])

    def __call__(self, instance):
        if instance.application != "ytserver-node":
            return instance

        ok = False
        medium = self.stored_replicas.get(instance.address, None)
        if medium is not None:
            instance.attributes.append("{}:stored:{}".format(self.chunk_id, medium))
            ok = True
        if instance.address in self.last_seen_replicas:
            instance.attributes.append("{}:last-seen".format(self.chunk_id))
            ok = True
        return ok


class NodeTabletCellIdFilter(object):
    def __init__(self, client, cell_id):
        self.cell_id = cell_id
        peers = client.get("#{}/@peers".format(self.cell_id))
        self.peers = {p["address"]: p["state"] for p in peers}

    def __call__(self, instance):
        if instance.application != "ytserver-node":
            return instance

        state = self.peers.get(instance.address)
        if state is not None:
            instance.attributes.append("{}:cell-state:{}".format(self.cell_id, state))
            return True

        return False


class NodeTabletIdFilter(object):
    def __init__(self, client, tablet_id):
        info = client.get("#{}/@".format(tablet_id), attributes=["cell_id"])
        self.tablet_id = tablet_id
        cell_id = info.get("cell_id")
        if cell_id:
            self.tablet_cell_id_filter = NodeTabletCellIdFilter(client, cell_id)
        else:
            self.tablet_cell_id_filter = None

    def __call__(self, instance):
        if instance.application != "ytserver-node":
            return instance

        if self.tablet_cell_id_filter:
            return self.tablet_cell_id_filter(instance)
        else:
            return False


class NodeJobIdSelector(object):
    def __init__(self, client):
        self.client = client
        self.operation_id = None

    def get_operation_id_filter(self, operation_id):
        self.operation_id = operation_id
        return lambda x: True

    def get_job_id_filter(self, job_id):
        if self.operation_id is None:
            raise LogrepError(
                "Cannot apply JobId selector `{}' because OperationId selector must be applied before.".format(job_id)
            )
        return AddressInSet({
            get_node_for_job_id(self.client, self.operation_id, job_id)
        })


def instance_match_attribute(instance, attr_pattern):
    result = False
    for attr in instance.attributes:
        if attr_pattern in attr:
            result = True
            break

    write_idx = 0
    for idx in xrange(len(instance.hidden_attributes)):
        attr = instance.hidden_attributes[idx]
        if attr_pattern in attr:
            instance.attributes.append(attr)
            result = True
        else:
            if write_idx != idx:
                instance.hidden_attributes[write_idx] = attr
            write_idx += 1
    if write_idx < len(instance.hidden_attributes):
        del instance.hidden_attributes[write_idx:]

    return result


def parse_state_attribute(address):
    state = address.attributes.get("state", "online")
    if state != "online":
        yield "state:{}".format(state)


def parse_banned_attribute(address):
    banned = address.attributes.get("banned", None)
    if banned and banned != "false":
        ban_message = address.attributes.get("ban_message", "<ban-message-not-specified>")
        yield "banned:{}".format(banned)
        yield "ban-message:\"{}\"".format(ban_message)

def parse_tags_attribute(address):
    tags = address.attributes.get("tags", None)
    if tags:
        for t in tags:
            yield "tag:{}".format(t)

def parse_role_attribute(address):
    role = address.attributes.get("role", None)
    if role:
        yield "role:{}".format(role)

def parse_rack_attribute(address):
    yield "rack:{}".format(address.attributes.get("rack", "<unknown>"))


def get_master_list(client):
    return get_primary_master_list(client) + get_secondary_master_list(client)


def get_primary_master_list(client):
    address_list = client.list("//sys/primary_masters")

    req_map = {}
    for address in address_list:
        req_map[address] = "//sys/primary_masters/{address}/orchid/monitoring/hydra/state".format(address=address)

    rsp_map = batch_get(client, req_map)
    result = []
    for address in address_list:
        attributes = [
            "primary",
            rsp_map[address],
        ]
        result.append(Instance(address, "ytserver-master", attributes, []))
    return result


def get_secondary_master_list(client):
    cell_id_map = client.get("//sys/secondary_masters")

    master_to_cell_id = {}
    req_map = {}
    for cell_id in cell_id_map:
        for address in cell_id_map[cell_id]:
            req_map[address] = (
                "//sys/secondary_masters/{cell_id}/{address}/orchid/monitoring/hydra/state"
                .format(cell_id=cell_id, address=address)
            )
            master_to_cell_id[address] = int(cell_id)

    rsp_map = batch_get(client, req_map)
    result = []
    for address, state in rsp_map.iteritems():
        master_type = "secondary:{:x}".format(master_to_cell_id[address])
        result.append(Instance(address, "ytserver-master", [master_type, state], []))
    return result


def get_scheduler_list(client):
    address_list = client.list("//sys/scheduler/instances")

    req_map = {}
    for address in address_list:
        req_map[address] = (
            "//sys/scheduler/instances/{address}/orchid/scheduler/connected".format(address=address)
        )

    rsp_map = batch_get(client, req_map)
    result = []
    for address, is_connected in rsp_map.iteritems():
        attributes = [
            "active" if is_connected else "standby"
        ]
        result.append(Instance(address, "ytserver-scheduler", attributes, []))
    return result


def get_controller_agent_list(client):
    address_list = client.list("//sys/controller_agents/instances")

    req_map = {}
    for address in address_list:
        req_map[address] = (
            "//sys/controller_agents/instances/{address}/orchid/controller_agent/connected".format(address=address)
        )

    rsp_map = batch_get(client, req_map)
    result = []
    for address, is_connected in rsp_map.iteritems():
        attributes = [
            "active" if is_connected else "standby"
        ]
        result.append(Instance(address, "ytserver-controller-agent", attributes, []))
    return result


def get_node_list(client):
    address_list = client.list("//sys/nodes", attributes=["state", "rack", "banned", "ban_message", "tags"])
    result = []
    for address in address_list:
        attributes = []
        hidden_attributes = []
        attributes += parse_rack_attribute(address)
        attributes += parse_state_attribute(address)
        attributes += parse_banned_attribute(address)
        hidden_attributes += parse_tags_attribute(address)
        result.append(Instance(str(address), "ytserver-node", attributes, hidden_attributes))
    return result


def get_proxy_list(client):
    address_list = client.list("//sys/proxies", attributes=["banned", "ban_message", "role"])
    result = []
    for address in address_list:
        attributes = []
        attributes += parse_role_attribute(address)
        attributes += parse_banned_attribute(address)
        result.append(Instance(str(address), "ytserver-http-proxy", attributes, []))
    return result


def get_rpc_proxy_list(client):
    address_list = client.list("//sys/rpc_proxies")
    result = []
    for address in address_list:
        result.append(Instance(address, "ytserver-proxy", [], []))
    return result


def batch_get(client, req_map):
    batch_client = client.create_batch_client(raise_errors=True)
    rsp_map = {}
    for k, path in req_map.iteritems():
        rsp_map[k] = batch_client.get(path)
    batch_client.commit_batch()

    for k, v in rsp_map.iteritems():
        rsp_map[k] = v.get_result()

    return rsp_map


def get_controller_agent_for_operation(client, operation_id):
    operation_attrs = client.get_operation(operation_id)
    address = operation_attrs.get("controller_agent_address", None)
    if address is None:
        raise LogrepError("Cannot find controller_agent in attributes of operation {}".format(operation_id))
    return address


def get_node_for_job_id(client, operation_id, job_id):
    job_info = client.get_job(operation_id, job_id)
    address = job_info["address"]
    return address


ParsedAddress = collections.namedtuple("ParsedAddress", ["host", "port"])


def parse_address(address):
    if not isinstance(address, str):
        raise TypeError
    if ":" in address:
        return ParsedAddress(*address.rsplit(":", 1))
    return ParsedAddress(address, None)


#
# Subcommands
#

def subcommand_list(selector_list, args):
    instance_list = get_all_instances(selector_list)
    for instance in sorted(instance_list):
        attributes = " ".join(instance.attributes)
        if args.short:
            print parse_address(instance.address).host
        else:
            print "{address}\t{application_name}\t{attributes}".format(
                address=instance.address,
                application_name=instance.application,
                attributes=attributes
            )


def verify_at_least_one_instance(instance_list, exactly):
    if len(instance_list) == 0:
        raise LogrepError(
            "You must select {} one instance. Currently selected no instances.".format(
                "exactly" if exactly else "at least")
        )


def verify_exactly_one_instance(instance_list):
    if len(instance_list) > 1:
        instance_list_str = indented_lines((instance.address for instance in instance_list[:10]), indent=2)
        if len(instance_list) > 10:
            instance_list_str += "  ...\n"
        raise LogrepError(
            "You must select exactly one instance. Currently you selected:\n"
            + instance_list_str
        )
    verify_at_least_one_instance(instance_list, exactly=True)


def get_instance_for_subcommand(instance_list, args):
    if args.any:
        verify_at_least_one_instance(instance_list, exactly=False)
    else:
        verify_exactly_one_instance(instance_list)

    return instance_list[0]


def args_get_time_interval(args, dt=datetime.timedelta(seconds=10)):
    start_time = parse_time(args.time, is_start=True)
    if args.end_time is None:
        args.end_time = args.time
    end_time = parse_time(args.end_time, is_start=False)
    start_time -= dt
    end_time += dt
    return start_time, end_time


def subcommand_grep(selector_list, args):
    instance_list = get_all_instances(selector_list)

    start_time, end_time = args_get_time_interval(args)

    instance = get_instance_for_subcommand(instance_list, args)

    host = parse_address(instance.address).host
    task = GrepTask(
        pattern=args.pattern,
        host=host,
        start_time=start_time,
        end_time=end_time,
        application=instance.application
    )

    task.remote_run()


def subcommand_pgrep(selector_list, args):
    instance_list = get_all_instances(selector_list)

    verify_at_least_one_instance(instance_list, exactly=False)

    if args.instance_limit and len(instance_list) > args.instance_limit:
        raise LogrepError(
            "Too many instances selected.\n"
            "Currently selected: {}\n"
            "Current limit: {}\n"
            .format(len(instance_list), args.instance_limit)
        )

    start_time, end_time = args_get_time_interval(args)

    queue = Queue.Queue()

    process_list = []
    for instance in instance_list:
        host = parse_address(instance.address).host
        task = GrepTask(
            pattern=args.pattern,
            host=host,
            start_time=start_time,
            end_time=end_time,
            application=instance.application,
        )

        process_list.append(task.async_remote_run(queue))

    completed_list = []
    while len(completed_list) != len(instance_list):
        item = queue.get()
        if isinstance(item, AsyncTaskCompleted):
            completed_list.append(item)
        elif isinstance(item, AsyncTaskOutput):
            print >>item.file, item.line
        else:
            raise AssertionError("Unknown item in queue: {}".format(item))

    completed_list.sort()
    ok = 0
    err_list = []
    for item in completed_list:
        if item.return_code == 0:
            ok += 1
            logging.info("Grep task {description} completed successfully".format(description=item.instance_description))
        else:
            err_list.append(
                "Grep task {description} exitted with error code: {code}\n"
                "Stderr tail:\n"
                "{stderr_tail}\n"
                .format(
                    code=item.return_code,
                    description=item.instance_description,
                    stderr_tail=indented_lines(item.stderr_tail, indent=2)
                )
            )
    if err_list:
        raise LogrepError(
            "".join(err_list)
        )
    else:
        logging.info("Successfully grepped {} instances".format(len(completed_list)))


def subcommand_yql_grep(selector_list, args):
    if len(selector_list) != 1:
        raise LogrepError("Multiple selectors specified, but --chyt-grep supports only single selector")

    selector = selector_list[0]

    cluster = selector.get_cluster()
    if cluster is None:
        raise LogrepError("Cannot derive cluster from selector {}".format(selector.selector_str))

    table_prefix = CHYT_PATH_MAP.get(selector.get_service(), None)
    if table_prefix is None:
        raise LogrepError("Logs for {} are not pushed to YT".format(selector.service))

    start_time, end_time = args_get_time_interval(args, dt=datetime.timedelta())

    import yql_logrep
    import yt.wrapper

    logging.info("Gathering tables to grep")
    client = yt.wrapper.YtClient("hahn")
    yql_tables = yql_logrep.find_tables(client, table_prefix, start_time, end_time)
    tables = yql_tables.all_tables
    if len(tables) == 0:
        raise LogrepError("Cannot find tables for specified time")

    logging.info("Found tables: {}".format(" ".join(tables)))
    if not yql_tables.complete:
        logging.warning("Looks like requested interval is not fully covered by selected tables")

    use_chyt = False
    if args.yql_flavour == "chyt":
        use_chyt = True
    elif args.yql_flavour == "yql":
        use_chyt = False
    else:
        assert args.yql_flavour == "auto"
        # 1d tables are to big to use chyt :(
        use_chyt = (len(yql_tables.tables_1d) == 0 and len(tables) <= 5)

    for record in yql_logrep.yql_grep(tables, cluster=cluster, pattern=args.pattern, use_chyt=use_chyt):
        sys.stdout.write(
            "{timestamp}\t{log_level}\t{log_message}\t{source}\n"
            .format(
                timestamp=record.timestamp,
                log_level=record.log_level,
                log_message=record.log_message,
                source=record.node))
        sys.stdout.flush()

def subcommand_ssh(selector_list, args):
    instance_list = get_all_instances(selector_list)

    instance = get_instance_for_subcommand(instance_list, args)
    host = parse_address(instance.address).host
    for fd, name in enumerate(["stdin", "stdout", "stderr"]):
        if not os.isatty(fd):
            raise LogrepError("Cannot launch ssh session, {} is not a tty.".format(name))

    logging.info("ssh to {}".format(host))
    SshTask(host, instance.application).remote_run()


def subcommand_get_job_log(selector_list, args):
    instance_list = get_all_instances(selector_list)

    from yt.wrapper import YtClient
    from yt.wrapper.common import object_type_from_uuid

    instance = get_instance_for_subcommand(instance_list, args)

    # Get job ids and operation ids from selector
    job_ids = []
    operation_ids = []
    for selector in selector_list:
        for guid in extract_guids(selector.get_text()):
            guid_type = object_type_from_uuid(guid)
            if guid_type == JOB_GUID_TYPE:
                job_ids.append(guid)
            elif guid_type == OPERATION_GUID_TYPE:
                operation_ids.append(guid)

    # Check that we have exactly one job-id/operation id
    def ensure_single_element(lst, element_description):
        if not lst:
            raise LogrepError("Cannot find {name} in instance selector".format(name=element_description))
        if len(lst) > 1:
            raise LogrepError(
                "Expected to have exactly one {name} in instance selector but found multiple:\n"
                "{lst}"
                .format(
                    name=element_description,
                    lst=indented_lines(lst)
                )
            )
        return lst[0]

    job_id = ensure_single_element(job_ids, "job id")
    operation_id = ensure_single_element(operation_ids, "job id")

    # Find approximate time that job was running
    logging.info("Resolving operation_id: {} job_id: {}".format(operation_id, job_id))
    cluster = resolve_cluster(operation_id)
    client = YtClient(cluster)
    job_info = client.get_job(operation_id, job_id)

    time = None
    for event in job_info["events"]:
        if event.get("state", None) == "running":
            time = utc_to_local(datetime.datetime.strptime(event["time"], "%Y-%m-%dT%H:%M:%S.%fZ"))
            break
    else:
        raise LogrepError("Cannot get the time when job was running")

    host = parse_address(instance.address).host
    task = GetJobLogsTask(host, job_id, time)
    task.remote_run()


SELECTOR_HELP = """\
INSTANCE SELECTORS
 Instance selector must start with /<cluster>/<service> or /<service>. In the latter case a guid selector
 must be present and the cluster will be detected. Check RECOGNIZED SERVICES for the list of accepted services.
 
 /@<attr-filter>
 Select instances that have attribute that match <attr-filter> (substring matching is used).
 
 /<guid>
 Select instances responsible for object. Each service treats object differently. If service doesn't know about object
 no filter is applied.
 
   /<operation-id> scheduler
   Select active scheduler
   
   /<operation-id> controller-agent
   Select controller agent responsible for operation
   
   /<operation-id>/<job-id> node
   Select node responsible for this job id.

   /<chunk-id> master
   Select master responsible for this chunk.

   /<chunk-id> node
   Select nodes responsible for this chunk.
   This selector also adds attributes
     - <chunk-id>:last-seen
     - <chunk-id>:stored:<medium>
   for selected nodes.

   /<tablet-id> master
   Select master responsible for this tablet.

   /<tablet-id> node
   Select nodes responsible for this tablet cell.
   This selector also adds attribute
     - <tablet-cell-id>:cell-state:<peer-state>
   for selected instances.

   /<tablet-cell-id> node
   Select nodes responsible for this tablet cell.
   This selector also adds attribute
     - <tablet-cell-id>:cell-state:<peer-state>
   for selected instances.

 /!<selector>
 Invert selector.

RECOGNIZED SERVICES
{list_of_known_services}
"""

TIME_FORMAT_HELP = """
TIME FORMATS
 logrep supports multiple ways of specifying time
    now - use current moment (current log file will be grepped)
    HH:MM (e.g. 14:30) - today's date is used
    HH:MM:SS (e.g. 12:23:00) - today's date is used
    YYYY-mm-DD (e.g. 2018-11-09)
    YYYY-mm-DD HH:MM:SS' (e.g. 2018-11-09 05:10:43)
    YYYY-mm-DDTHH:MM:SS' (e.g. 2019-12-14T00:10:43)
    DD MMM YYYY HH:MM:SS (e.g. 16 Nov 2018 13:56:14)
    YYYY-mm-DDTHH:MM:SSZ (e.g. 2019-09-19T11:46:04.848360Z)
""".format(
    list_of_known_services=indented_lines((info.name for info in SERVICE_TABLE), indent=2)
)

EPILOG = """\
Check https://ya.cc/4Waoc for examples of usage.
For help about instance selectors and time formats use `--help-more`
"""

def main():
    RemoteTask.try_switch_to_remote_task_mode()

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, epilog=EPILOG)

    class MoreHelpAction(argparse.Action):
        def __init__(self, option_strings, dest, nargs=None, **kwargs):
            if nargs != None:
                raise ValueError("nargs not allowed")
            argparse.Action.__init__(self, option_strings, dest, nargs=0, **kwargs)

        def __call__(self, parser, namespace, values, option_string):
            sys.stdout.write(SELECTOR_HELP)
            sys.stdout.write(TIME_FORMAT_HELP)
            sys.stdout.flush()
            sys.exit(0)
    parser.add_argument(
        "--help-more", action=MoreHelpAction,
        help="Show help about instance selectors and time format then exit")

    parser.set_defaults(subcommand=subcommand_list)
    parser.add_argument("selector", nargs="+", help="selector of instances to work with")

    parser.add_argument("--any", action="store_true", default=False, help="select arbitrary instance")

    action_group = parser.add_mutually_exclusive_group()

    action_group.add_argument(
        "--ssh",
        help="ssh to selected instance (single instance must be selected or `--any' flag used)",
        action="store_const",
        dest="subcommand",
        const=subcommand_ssh
    )

    action_group.add_argument(
        "--get-job-log",
        help="find and get logs of a job proxy that executed job",
        action="store_const",
        dest="subcommand",
        const=subcommand_get_job_log,
    )

    class GrepAction(argparse.Action):
        def __init__(self, option_strings, dest, subcommand=None, **kwargs):
            argparse.Action.__init__(self, option_strings, dest, metavar="PATTERN", **kwargs)
            self.subcommand = subcommand

        def __call__(self, parser, namespace, values, option_string):
            setattr(namespace, "pattern", values)
            setattr(namespace, "subcommand", self.subcommand)

    action_group.add_argument(
        "--grep",
        help="grep logs on selected instance (single instance must be selected or `--any' flag used)",
        action=GrepAction,
        subcommand=subcommand_grep,
    )

    action_group.add_argument(
        "--pgrep",
        help="grep logs on several selected instances in parallel",
        action=GrepAction,
        subcommand=subcommand_pgrep,
    )

    action_group.add_argument(
        "--yql-grep",
        help="grep logs using YQL",
        action=GrepAction,
        subcommand=subcommand_yql_grep,
    )

    list_group = parser.add_argument_group("list instances arguments")

    list_group.add_argument(
        "--short",
        help="print only host names when listing instances",
        action="store_true",
        default=False,
    )

    grep_pgrep_group = parser.add_argument_group("grep / pgrep arguments")

    grep_pgrep_group.add_argument(
        "-t", "--time", "--start-time",
        default="now",
        help="start of time interval to grep (check TIME FORMATS below)"
    )
    grep_pgrep_group.add_argument(
        "-e", "--end-time",
        help="end of time interval to grep, equals start interval by default (check TIME FORMATS below)"
    )

    pgrep_group = parser.add_argument_group("pgrep only arguments")
    pgrep_group.add_argument(
        "--instance-limit",
        type=int,
        default=10,
        help=(
            "limit for the number of selected instances "
            "(pgrep will fail if number of selected instance exeeds this limit, default limit: 10)"
        )
    )
    
    yql_grep_group = parser.add_argument_group("yql-grep arguments")
    yql_grep_group.add_argument(
        "--yql-flavour",
        choices=["auto", "chyt", "yql"],
        default="auto",
        help=(
            "which flavour of YQL query we should use (default:auto)"
        )
    )

    args = parser.parse_args()

    selector_list = []
    for selector_str in args.selector:
        selector_list.append(resolve_selector(selector_str))

    args.subcommand(selector_list, args)


if __name__ == "__main__":
    def run():
        try:
            main()
        except LogrepError as e:
            print >>sys.stderr, str(e)
            if e.epilog:
                print >>sys.stderr, e.epilog
            exit(1)
        except IOError as e:
            raise
            if e.errno != errno.EPIPE:
                raise
    run()
