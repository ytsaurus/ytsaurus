#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import argparse
import collections
import datetime
import errno
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

# NOTE: we should not import nonstandard libraries here because this script is being uploaded to remote server
# which might miss such library

FQDN = socket.getfqdn()

CHUNK_GUID_TYPE = 100
ERASURE_CHUNK_GUID_TYPE = 102
JOB_GUID_TYPE = 900
OPERATION_GUID_TYPE = 1000

GUID_RE = re.compile("^[a-fA-F0-9]+-[a-fA-F0-9]+-[a-fA-F0-9]+-[a-fA-F0-9]+$")
LOG_FILE_NAME_RE = re.compile(r"^(?P<app_and_host>[^.]+)[.]debug[.]log([.](?P<log_index>\d+))?([.]gz)?$")

ParsedLogName = collections.namedtuple("ParsedLogName", ["log_prefix", "host", "log_index"])

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

assert all(service.application_name in APPLICATION_MAP for service in SERVICE_TABLE)

LOG_PREFIX_MAP = {info.log_prefix: info for info in APPLICATION_TABLE}
LOG_DIR_NAME_MAP = {info.log_dir_name : info for info in APPLICATION_TABLE}

SSH_OPTS = [
    "-o", "StrictHostKeyChecking=no",
]


def list_known_services():
    return [service.name for service in SERVICE_TABLE]


def get_application_by_log_prefix(log_prefix):
    if log_prefix not in LOG_PREFIX_MAP:
        return None
    return LOG_PREFIX_MAP[log_prefix].name


def get_application_by_log_dir_name(log_dir_name):
    suffix = "-logs"
    if log_dir_name.endswith(suffix):
        log_dir_name = log_dir_name[:-len(suffix)]
    if log_dir_name not in LOG_DIR_NAME_MAP:
        return None
    return LOG_DIR_NAME_MAP[log_dir_name].name


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
        CHUNK_GUID_TYPE: "ChunkId",
        ERASURE_CHUNK_GUID_TYPE: "ErasureChunkId"
    }[guid_type]


def cell_id_from_guid(guid):
    assert GUID_RE.match(guid)
    part = int(guid.split("-")[2], 16)
    return part >> 16


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
    hostname = FQDN.split(".")[0]
    expected_suffix = "-" + hostname
    if app_and_host.endswith(expected_suffix):
        app_and_host = app_and_host[:-len(expected_suffix)]
    index = m.group("log_index")
    index = int(index) if index else 0
    return ParsedLogName(app_and_host, hostname, index)


AsyncTaskOutput = collections.namedtuple("AsyncTaskOutput", ["file", "line"])
AsyncTaskCompleted = collections.namedtuple("AsyncTaskCompleted", ["instance_description", "return_code", "stderr_tail"])


def reset_signals():
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)


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

    def popen_on_remote_machine(self, host, **kwargs):
        logging.info("connecting to {}".format(host))
        with open(__file__) as source_file:
            return subprocess.Popen(
                ["ssh", host] + SSH_OPTS + [
                    "LOGREP_REMOTE_TASK_CLASS={} LOGREP_REMOTE_TASK_ARGS={} stdbuf -oL -eL python -".format(
                        self.__class__.__name__,
                        shell_quote(self.save())
                    )
                ],
                stdin=source_file,
                preexec_fn=reset_signals,
                **kwargs
            )

    def exec_on_remote_machine(self, host, allocate_terminal=False):
        logging.info("copying script to {}".format(host))
        with open(__file__, "r") as source_file:
            hash_value = hashlib.md5(source_file.read()).hexdigest()
        remote_file_name = "/tmp/logrep_{}.py".format(hash_value)
        subprocess.check_call(
            ["scp"] + SSH_OPTS + [__file__, "{}:{}".format(host, remote_file_name)]
        )
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
        chdir_to_application_logs(self.application)
        file_to_grep_list = find_files_to_grep(self.application, self.start_time, self.end_time)
        if not file_to_grep_list:
            raise LogrepError("Cannot find log files for time interval: {} - {}".format(self.start_time, self.end_time))

        for f in file_to_grep_list:
            logging.info("would grep {}".format(f))

        cmd = ["zfgrep", self.pattern] + file_to_grep_list
        logging.info("running {}".format(" ".join(map(shell_quote, cmd))))

        cmd = ["stdbuf", "-oL", "-eL"] + cmd

        code = subprocess.call(cmd, preexec_fn=reset_signals())
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
            cur_app = get_application_by_log_prefix(parsed.log_prefix)
            if cur_app is not None:
                log_dirs[cur_app] = "."
    else:
        for name in os.listdir("."):
            if name.endswith("-logs") and os.path.isdir(name):
                cur_app = get_application_by_log_dir_name(name)
                if cur_app is not None:
                    log_dirs[cur_app] = name

    if not log_dirs:
        raise LogrepError("Cannot find any application logs on this server.")

    if application not in log_dirs:
        raise LogrepError(
            "Service: {application} is not found on server. Available applications on this server:\n"
            "{available_applications}\n".format(
                application=application,
                available_applications=indented_lines(sorted(log_dirs), indent=2)
            )
        )
    os.chdir(log_dirs[application])


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

    expected_log_prefix = APPLICATION_MAP[application].log_prefix
    log_file_list = []
    if os.path.exists("archive"):
        for filename in os.listdir("archive"):
            if ".debug.log" not in filename:
                continue
            if not filename.startswith(expected_log_prefix):
                continue
            log_file_list.append(os.path.join("archive", filename))
        log_file_list.sort()
    current_logs = []
    for filename in os.listdir("."):
        if not filename.startswith(expected_log_prefix):
            continue
        parsed = parse_log_filename(filename)
        if parsed is None:
            continue
        current_logs.append((parsed.log_index, filename))
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
    if time_str == "now":
        return datetime.datetime.now()
    elif re.match("\d\d:\d\d:\d\d", time_str):
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
# Instance resolving
#

Instance = collections.namedtuple("Instance", ["address", "application", "attributes"])


def resolve_instance(instance_str):
    from yt.wrapper import YtClient

    if re.match(r"^(/[^/]+)+$", instance_str):
        components = instance_str.strip("/").split("/")
        if len(components) < 2:
            raise LogrepError(
                "Cannot resolve instance `{}': instance must start with /<cluster>/<service>".format(instance_str)
            )
        cluster, service = components[:2]
        client = YtClient(cluster)
        return resolve_service(client, service, components[2:])
    elif instance_str.count("@") == 1:
        application, address = instance_str.split("@")
        return [Instance(address, application, [])]
    else:
        raise LogrepError("Cannot resolve instance `{}'".format(instance_str))


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
        if selector.startswith("@"):
            def filter_func(x):
                return instance_match_attribute(x, selector[1:])
        elif selector.startswith("#") or GUID_RE.match(selector):
            if selector.startswith("#"):
                n = selector.strip("#")
                if not GUID_RE.match(n):
                    raise LogrepError("Bad guid selector: {}".format(selector))
                selector = n
            guid = selector
            guid_type = object_type_from_uuid(guid)
            if guid_type not in [OPERATION_GUID_TYPE, JOB_GUID_TYPE, CHUNK_GUID_TYPE, ERASURE_CHUNK_GUID_TYPE]:
                raise LogrepError("Don't know what to do with guid of unknown type: {}".format(guid_type))
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
                    attribute = "secondary:{cell:x}".format(cell=cell_id_from_guid(guid))

                    def filter_func(x):
                        return instance_match_attribute(instance, attribute)
                elif application == "ytserver-node":
                    filter_func = NodeChunkIdFilter(client, guid)
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

        instance_list = [instance for instance in instance_list if filter_func(instance)]

    return instance_list


class AddressInSet(object):
    def __init__(self, iterable):
        self.filter_set = frozenset(iterable)

    def __call__(self, x):
        return x.address in self.filter_set


class NodeChunkIdFilter(object):
    def __init__(self, client, chunk_id):
        info = client.get("#{}/@".format(chunk_id), attributes=["stored_replicas", "last_seen_replicas"])
        self.stored_replicas = {str(r): r.attributes.get("medium", "") for r in info["stored_replicas"]}
        self.last_seen_replicas = frozenset(str(r) for r in info["last_seen_replicas"])

    def __call__(self, instance):
        if instance.application != "ytserver-node":
            return instance

        ok = False
        medium = self.stored_replicas.get(instance.address, None)
        if medium is not None:
            instance.attributes.append("stored:{}".format(medium))
            ok = True
        if instance.address in self.last_seen_replicas:
            instance.attributes.append("last-seen")
            ok = True
        return ok


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
    return any(attr_pattern in attr for attr in instance.attributes)


def get_master_list(client):
    return get_primary_master_list(client) + get_secondary_master_list(client)


def get_primary_master_list(client):
    address_list = client.list("//sys/primary_masters")

    req_map = {}
    for address in address_list:
        req_map[address] = "//sys/primary_masters/{address}/orchid/monitoring/hydra/state".format(address=address)

    rsp_map = batch_get(client, req_map)
    result = []
    for address, hydra_state in rsp_map.iteritems():
        result.append(Instance(address, "ytserver-master", [hydra_state, "primary"]))
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
        result.append(Instance(address, "ytserver-master", [state, master_type]))
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
        active_attr = "active" if is_connected else "standby"
        result.append(Instance(address, "ytserver-scheduler", [active_attr]))
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
        active_attr = "active" if is_connected else "standby"
        result.append(Instance(address, "ytserver-controller-agent", [active_attr]))
    return result


def get_node_list(client):
    node_attributes = ["state", "rack"]
    address_list = client.list("//sys/nodes", attributes=node_attributes)
    result = []
    for address in address_list:
        attrs = [
            "{}:{}".format(attr, address.attributes.get(attr, "<unknown>"))
            for attr in node_attributes
        ]
        result.append(Instance(str(address), "ytserver-node", attrs))
    return result


def get_proxy_list(client):
    address_list = client.list("//sys/proxies")
    result = []
    for address in address_list:
        result.append(Instance(address, "ytserver-http-proxy", []))
    return result


def get_rpc_proxy_list(client):
    address_list = client.list("//sys/rpc_proxies")
    result = []
    for address in address_list:
        result.append(Instance(address, "ytserver-proxy", []))
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

def subcommand_list(instance_list, args):
    for instance in sorted(instance_list):
        attributes = " ".join(instance.attributes)
        print "{address}\t{application_name}\t{attributes}".format(
            address=instance.address,
            application_name=instance.application,
            attributes=attributes
        )


def verify_instance_is_single(instance_list):
    if len(instance_list) > 1:
        instance_list_str = indented_lines((instance.address for instance in instance_list[:10]), indent=2)
        if len(instance_list) > 10:
            instance_list_str += "  ...\n"
        raise LogrepError(
            "You must select exactly one instance. Currently you selected:\n"
            + instance_list_str
        )
    if len(instance_list) == 0:
        raise LogrepError(
            "You must select exactly one instance. Currently selected no instance."
        )


def args_get_time_interval(args):
    start_time = parse_time(args.time)
    if args.end_time is None:
        end_time = start_time
    else:
        end_time = parse_time(args.end_time)

    dt = datetime.timedelta(seconds=10)
    start_time -= dt
    end_time += dt
    return start_time, end_time


def subcommand_grep(instance_list, args):
    start_time, end_time = args_get_time_interval(args)

    verify_instance_is_single(instance_list)
    instance, = instance_list

    host = parse_address(instance.address).host
    task = GrepTask(
        pattern=args.pattern,
        host=host,
        start_time=start_time,
        end_time=end_time,
        application=instance.application
    )

    task.remote_run()


def subcommand_pgrep(instance_list, args):
    if not instance_list:
        LogrepError("No no instance is selected")

    if args.instance_limit and len(instance_list) > args.instance_limit:
        raise LogrepError(
            "Too many instances is selected.\n"
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


def subcommand_ssh(instance_list, _args):
    verify_instance_is_single(instance_list)
    instance, = instance_list
    host = parse_address(instance.address).host
    for fd, name in enumerate(["stdin", "stdout", "stderr"]):
        if not os.isatty(fd):
            raise LogrepError("Cannot launch ssh session, {} is not a tty.".format(name))

    logging.info("ssh to {}".format(host))
    SshTask(host, instance.application).remote_run()


EPILOG = """\
INSTANCE SELECTORS
 Instance selector must start with /<cluster>/<service>. Check RECOGNIZED SERVICES for the list of accepted services.
 
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

RECOGNIZED SERVICES
{list_of_known_services}

TIME FORMATS
 logrep supports multiple ways of specifying time
    now - use current moment (current log file will be grepped)
    HH:MM:SS (e.g. 12:23:00) - today's date is used
    YYYY-MM-DD HH:MM:SS' (e.g. 2018-11-09 05:10:43)
    DD MMM YYYY HH:MM:SS (e.g. 16 Nov 2018 13:56:14)
""".format(
    list_of_known_services=indented_lines((info.name for info in SERVICE_TABLE), indent=2)
)


def main():
    RemoteTask.try_switch_to_remote_task_mode()

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, epilog=EPILOG)
    parser.set_defaults(subcommand=subcommand_list)
    parser.add_argument("instance", nargs="+", help="instances to work with")

    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument(
        "--ssh",
        help="ssh to selected instance",
        action="store_const",
        dest="subcommand",
        const=subcommand_ssh
    )

    class GrepAction(argparse.Action):
        def __init__(self, option_strings, dest, subcommand=None, **kwargs):
            argparse.Action.__init__(self, option_strings, dest, **kwargs)
            self.subcommand = subcommand

        def __call__(self, parser, namespace, values, option_string):
            setattr(namespace, "pattern", values)
            setattr(namespace, "subcommand", self.subcommand)

    grep_group = action_group.add_argument_group()
    grep_group.add_argument(
        "--grep",
        help="grep logs on selected instance",
        action=GrepAction,
        subcommand=subcommand_grep,
    )

    pgrep_group = action_group.add_argument_group()
    pgrep_group.add_argument(
        "--pgrep",
        help="grep logs on several selected instances in parallel",
        action=GrepAction,
        subcommand=subcommand_pgrep,
    )

    parser.add_argument(
        "-t", "--time", "--start-time",
        default="now",
        help="start of time interval to grep (check TIME FORMATS below)"
    )
    parser.add_argument(
        "-e", "--end-time",
        help="end of time interval to grep, equals start interval by default (check TIME FORMATS below)"
    )

    pgrep_group.add_argument(
        "--instance-limit",
        type=int,
        default=10,
        help="limit for the number of selected instances"
    )

    args = parser.parse_args()

    instance_list = []
    for instance_description in args.instance:
        instance_list += resolve_instance(instance_description)

    args.subcommand(instance_list, args)


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
            if e.errno != errno.EPIPE:
                raise
    run()
