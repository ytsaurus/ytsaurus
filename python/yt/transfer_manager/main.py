#!/usr/bin/env python

import yt.logger as logger
from yt.tools.mr import Mr
from yt.wrapper.client import Yt
from yt.wrapper.common import generate_uuid
import yt.wrapper as yt

from flask import Flask, request, jsonify, Response, make_response

import os
import json
import time
import signal
import argparse
from copy import deepcopy
from datetime import datetime
from collections import defaultdict

from threading import RLock, Thread
from multiprocessing import Process, Array, Queue

def now():
    return str(datetime.utcnow().isoformat() + "Z")

def export_to_mr(yt_client, mr_client, src, dst, mr_user, token, title):
    yt_client = deepcopy(yt_client)
    mr_client = deepcopy(mr_client)

    mr_client.mr_user = mr_user
    if not mr_client.is_empty(dst):
        mr_client.drop(dst)

    yt_client.token = token

    record_count = yt_client.records_count(src)

    spec={"data_size_per_job": 2 * 1024 * yt.config.MB}
    if yt_client._export_pool is not None:
        spec["pool"] = yt_client._export_pool
    spec["title"] = title

    write_command = mr_client.get_write_command(dst)
    logger.info("Running map '%s'", write_command)
    yt_client.run_map(write_command, src, yt_client.create_temp_table(),
                      files=mr_client.binary,
                      format=yt.YamrFormat(has_subkey=True, lenval=True),
                      memory_limit=2500 * yt.config.MB,
                      spec=spec)

    result_record_count = mr_client.records_count(dst)
    if record_count != result_record_count:
        mr_client.drop(dst)
        error = "Incorrect record count (expected: %d, actual: %d)" % (record_count, result_record_count)
        logger.error(error)
        raise yt.YtError(error)

def import_from_mr(yt_client, mr_client, src, dst, mr_user, token, title):
    yt_client = deepcopy(yt_client)
    mr_client = deepcopy(mr_client)

    mr_client.mr_user = mr_user
    yt_client.token = token
    portion_size = 1024 ** 3

    proxies = mr_client.proxies
    if not proxies:
        proxies = [mr_client.server]

    record_count = mr_client.records_count(src, allow_cache=True)
    sorted = mr_client.is_sorted(src, allow_cache=True)

    logger.info("Importing table '%s' (row count: %d, sorted: %d)", src, record_count, sorted)

    yt_client.create_table(dst, recursive=True, ignore_existing=True)

    ranges = []
    record_threshold = max(1, record_count * portion_size / mr_client.data_size(src))
    for i in xrange((record_count - 1) / record_threshold + 1):
        server = proxies[i % len(proxies)]
        start = i * record_threshold
        end = min(record_count, (i + 1) * record_threshold)
        ranges.append((server, start, end))

    temp_table = yt_client.create_temp_table(prefix=os.path.basename(src))
    yt_client.write_table(temp_table,
                          ["\t".join(map(str, range)) + "\n" for range in ranges],
                          format=yt.YamrFormat(lenval=False, has_subkey=True))

    spec = {"data_size_per_job": 1}
    if yt_client._import_pool is not None:
        spec["pool"] = yt_client._import_pool
    spec["title"] = title

    temp_yamr_table = "tmp/yt/" + generate_uuid()
    mr_client.copy(src, temp_yamr_table)
    src = temp_yamr_table

    read_command = mr_client.get_read_range_command(src)
    command = 'while true; do '\
                  'IFS="\t" read -r server start end; '\
                  'if [ "$?" != "0" ]; then break; fi; '\
                  'set -e; '\
                  '{0}; '\
                  'set +e; '\
              'done;'\
                  .format(read_command)
    logger.info("Pull import: run map '%s' with spec '%s'", command, repr(spec))
    try:
        yt_client.run_map(
            command,
            temp_table,
            dst,
            input_format=yt.YamrFormat(lenval=False, has_subkey=True),
            output_format=yt.YamrFormat(lenval=True, has_subkey=True),
            files=mr_client.binary,
            memory_limit = 2500 * yt.config.MB,
            spec=spec)
    finally:
        mr_client.drop(temp_yamr_table)

class Task(object):
    def __init__(self, source_cluster, source_table, destination_cluster, destination_table, creation_time, id, state, token="", user="unknown", mr_user="tmp", error=""):
        self.source_cluster = source_cluster
        self.source_table = source_table
        self.destination_cluster = destination_cluster
        self.destination_table = destination_table

        self.creation_time = creation_time
        self.state = state
        self.id = id
        self.user = user
        self.mr_user = mr_user
        self.error = error
        self.token = token
        #self.detailed_state = ...

    def get_queue_id(self):
        return self.source_cluster, self.destination_cluster

    def dict(self):
        result = deepcopy(self.__dict__)
        del result["token"]
        return result

class Application(object):
    ERROR_BUFFER_SIZE = 2 ** 16

    def __init__(self, config):
        self._daemon = Flask(__name__)

        self._config = config
        self._mutex = RLock()
        self._yt = Yt(config["proxy"])
        self._yt.token = config["token"]
        
        message_queue = Queue()
        self._lock_path = config["lock_path"]
        self._lock_thread = Process(target=self._take_lock, args=(message_queue,))
        self._lock_thread.start()
        if not message_queue.get(timeout=5.0):
            raise yt.YtError("Cannot take lock " + self._lock_path)

        self._load_config(config)

        self._add_rule("/", 'main', methods=["GET"])
        self._add_rule("/add/", 'add', methods=["POST"])
        self._add_rule("/abort/<id>/", 'abort', methods=["POST"])
        self._add_rule("/restart/<id>/", 'restart', methods=["POST"])
        self._add_rule("/get/<id>/", 'get_task', methods=["GET"])
        self._add_rule("/get/tasks/", 'get_tasks', methods=["GET"])
        self._add_rule("/config/", 'config', methods=["GET"])

        self._task_processes = {}

        self._execution_thread = Thread(target=self._execute_tasks)
        self._execution_thread.daemon = True
        self._execution_thread.start()

    def _add_rule(self, rule, endpoint, methods):
        methods.append("OPTIONS")
        self._daemon.add_url_rule(rule, endpoint, self._process_cors(Application.__dict__[endpoint], methods), methods=methods)

    def _process_cors(self, func, methods):
        def decorator(*args, **kwargs):
            if request.method == "OPTIONS":
                rsp = self._daemon.make_default_options_response()
                rsp.headers["Access-Control-Allow-Origin"] = "*"
                rsp.headers["Access-Control-Allow-Methods"] = ", ".join(methods)
                rsp.headers["Access-Control-Allow-Headers"] = ", ".join(["Authorization", "Origin", "Content-Type", "Accept"])
                rsp.headers["Access-Control-Max-Age"] = 3600
                return rsp
            else:
                rsp = make_response(func(self, *args, **kwargs))
                rsp.headers["Access-Control-Allow-Origin"] = "*"
                return rsp
        
        return decorator

    def _take_lock(self, message_queue):
        with self._yt.PingableTransaction():
            try:
                self._yt.create("map_node", self._lock_path, ignore_existing=True)
                self._yt.lock(self._lock_path)
                message_queue.put(True)
            except Exception as err:
                logger.exception(err)
                message_queue.put(True)
                return

            # Sleep infinitely long
            time.sleep(2 ** 60)

    def _load_config(self, config):
        self._clusters = {}

        for name, cluster_description in config["clusters"].iteritems():
            type = cluster_description["type"]
            options = cluster_description["options"]

            if type == "yt":
                self._clusters[name] = Yt(token=config["token"], **options)
                self._clusters[name]._export_pool = cluster_description.get("mr_export_pool")
                self._clusters[name]._import_pool = cluster_description.get("mr_import_pool")
                self._clusters[name]._network = cluster_description.get("remote_copy_network")
            elif type == "mr":
                self._clusters[name] = Mr(**options)
            else:
                raise yt.YtError("Incorrect cluster type " + options["type"])

            # Hacky :(
            self._clusters[name]._type = type

        for name in config["availability_graph"]:
            if name not in self._clusters:
                raise yt.YtError("Incorrect availability graph, cluster {} is missing".format(name))
            for neighbour in config["availability_graph"][name]:
                if neighbour not in self._clusters:
                    raise yt.YtError("Incorrect availability graph, cluster {} is missing".format(neighbour))

        self._availability_graph = config["availability_graph"]

        self._load_tasks(config["tasks_path"])

    def _load_tasks(self, tasks_path): #, archived_tasks_path):
        self._tasks_path = tasks_path
        #self._archived_tasks_path = archived_tasks_path

        # From id to task description
        self._tasks = {}

        # From ... to task ids
        self._running_task_queues = defaultdict(lambda: [])

        # List of tasks sorted by creation time
        self._pending_tasks = []

        for id, options in self._yt.get(tasks_path).iteritems():
            task = Task(**options)
            self._tasks[id] = task
            if task.state == "running":
                self._change_task_state(id, "pending")
                task.state = "pending"
            if task.state == "pending":
                self._pending_tasks.append(task.id)

        self._pending_tasks.sort(key=lambda id: self._tasks[id].creation_time)

    def _change_task_state(self, id, new_state):
        with self._mutex:
            self._tasks[id].state = new_state
            self._yt.set(os.path.join(self._tasks_path, id), self._tasks[id].__dict__)

    def _get_token(self, authorization_header):
        words = authorization_header.split()
        if len(words) != 2 or words[0].lower() != "oauth":
            return None
        return words[1]

    def _precheck(self, task):
        if task.source_cluster not in self._clusters:
            raise yt.YtError("Unknown cluster " + task.source_cluster)
        if task.destination_cluster not in self._clusters:
            raise yt.YtError("Unknown cluster " + task.destination_cluster)
        if task.source_cluster not in self._availability_graph or task.destination_cluster not in self._availability_graph[task.source_cluster]:
            raise yt.YtError("Cluster {} not available from {}".format(task.destination_cluster, task.source_cluster))

        source_client = self._clusters[task.source_cluster]
        destination_client = self._clusters[task.destination_cluster]
        if source_client._type == "mr" and source_client.is_empty(task.source_table) or \
           source_client._type == "yt" and (not source_client.exists(task.source_table) or source_client.get_attribute(task.source_table, "row_count") == 0):
            raise yt.YtError("Source table {} is empty".format(task.source_table))

        if source_client._type == "yt" and destination_client._type == "mr":
            keys = list(source_client.read_table(yt.TablePath(task.source_table, end_index=1), format=yt.JsonFormat(), raw=False).next())
            if set(keys + ["subkey"]) != set(["key", "subkey", "value"]):
                raise yt.YtError("Keys in the source table must be a subset of ('key', 'subkey', 'value')")
        
        if destination_client._type == "yt" and \
           destination_client.check_permission(task.user, "write", os.path.dirname(task.destination_table))["action"] != "allow":
            raise yt.YtError("There is no permission to write to " + task.destination_table)


    def _can_run(self, task):
        return not self._running_task_queues[task.get_queue_id()]

    def _execute_tasks(self):
        while True:
            with self._mutex:
                self._pending_tasks = filter(lambda id: self._tasks[id].state == "pending", self._pending_tasks)

                for id, (process, error) in self._task_processes.items():
                    if not process.is_alive():
                        if process.aborted:
                            pass
                        elif not error.value:
                            self._change_task_state(id, "completed")
                        else:
                            self._change_task_state(id, "failed")
                            self._tasks[id].error = error.value

                        self._running_task_queues[self._tasks[id].get_queue_id()].remove(id)
                        del self._task_processes[id]

                for id in self._pending_tasks:
                    if not self._can_run(self._tasks[id]):
                        continue
                    self._running_task_queues[self._tasks[id].get_queue_id()].append(id)
                    self._change_task_state(id, "running")
                    task_error = Array('c', Application.ERROR_BUFFER_SIZE)
                    task_process = Process(target=lambda: self._execute_task(self._tasks[id], task_error))
                    task_process.aborted = False
                    task_process.start()
                    self._task_processes[id] = (task_process, task_error)

            time.sleep(1.0)

    def _execute_task(self, task, error):
        logger.info("Executing task %s", task.id)
        try:
            self._precheck(task)

            title = "Supervised by transfer task " + task.id

            if self._clusters[task.source_cluster]._type == "yt" and self._clusters[task.destination_cluster]._type == "yt":
                client = deepcopy(self._clusters[task.destination_cluster])
                client.token = task.token
                client.run_remote_copy(task.source_table, task.destination_table, cluster_name=task.source_cluster, network_name=self._clusters[task.source_cluster]._network, spec={"title": title})
            if self._clusters[task.source_cluster]._type == "yt" and self._clusters[task.destination_cluster]._type == "mr":
                export_to_mr(self._clusters[task.source_cluster], self._clusters[task.destination_cluster], task.source_table, task.destination_table, task.mr_user, task.token, title=title)
            if self._clusters[task.source_cluster]._type == "mr" and self._clusters[task.destination_cluster]._type == "yt":
                import_from_mr(self._clusters[task.destination_cluster], self._clusters[task.source_cluster], task.source_table, task.destination_table, task.mr_user, task.token, title=title)
            logger.info("Task %s completed", task.id)
        except KeyboardInterrupt:
            pass
        except Exception as err:
            logger.exception(err)
            logger.info("Task %s failed with error '%s'", task.id, err.message)
            error.value = err.message[:Application.ERROR_BUFFER_SIZE]


    # Public interface
    def run(self, *args, **kwargs):
        self._daemon.run(*args, **kwargs)


    # Url handlers
    def main(self):
        return "This is YT import/export daemon"

    def add(self):
        try:
            try:
                params = json.loads(request.data)
            except ValueError:
                return "Cannot parse json from body '{}'".format(request.data), 400

            required_parameters = set(["source_cluster", "source_table", "destination_cluster", "destination_table"])
            if not set(params) >= required_parameters:
                return "All required parameters ({}) must be presented Incorrect parameters".format(", ".join(required_parameters)), 400

            token = self._get_token(request.headers.get("Authorization", ""))
            if token is None or token == "undefined":
                user = "guest"
                token = ""
            else:
                user = self._yt.get_user_name(token)
                if not user:
                    return "Authorization token is incorrect: " + token, 400

            try:
                task = Task(id=generate_uuid(), creation_time=now(), user=user, token=token, state="pending", **params)
            except TypeError:
                return "Cannot create task", 400

            try:
                self._precheck(task)
            except yt.YtError as error:
                return "Precheck failed: " + error.message, 400

            if not request.args.get("dry_run", False):
                with self._mutex:
                    self._tasks[task.id] = task
                    self._pending_tasks.append(task.id)

                self._yt.set(os.path.join(self._tasks_path, task.id), task.__dict__)

        except Exception as error:
            return "Unknown error: " + error.message, 502

        return task.id

    def abort(self, id):
        if id not in self._tasks:
            return "Unknown task " + id, 400

        if id not in self._task_processes:
            return "Taks {0} is not running ".format(id), 400

        process, _ = self._task_processes[id]
        process.aborted = True

        os.kill(process.pid, signal.SIGINT)
        time.sleep(0.5)
        if process.is_alive():
            process.terminate()

        with self._mutex:
            self._change_task_state(id, "aborted")

        return "OK"

    def restart(self, id):
        if id not in self._tasks:
            return "Unknown task " + id, 400
        if self._tasks[id].state not in ["completed", "aborted", "failed"]:
            return "Cannot restart task in state " + self._tasks[id].state, 400

        self._tasks[id].state = "pending"
        self._tasks[id].creation_time = now()
        self._tasks[id].error = ""
        self._pending_tasks.append(id)

        return "OK"

    def get_task(self, id):
        if id not in self._tasks:
            return "Unknown task " + id, 400
        return jsonify(**self._tasks[id].dict())

    def get_tasks(self):
        user = request.args.get("user")
        tasks = self._tasks.values()
        if user is not None:
            tasks = [task.user == user for task in tasks]

        return Response(json.dumps(map(lambda task: task.dict(), tasks)), mimetype='application/json')

    def config(self):
        return jsonify(self._config)

DEFAULT_CONFIG = {
    "clusters": {
        "kant": {
            "type": "yt",
            "options": {"proxy": "kant.yt.yandex.net"}
        },
        "smith": {
            "type": "yt",
            "options": {"proxy": "smith.yt.yandex.net"}
        },
        "plato": {
            "type": "yt",
            "options": {"proxy": "plato.yt.yandex.net"}
        },
        "cedar": {
            "type": "mr",
            "options": {
                "server": "cedar.search.yandex.net",
                "opts": "MR_NET_TABLE=ipv6",
                "binary": "/opt/cron/tools/mapreduce",
                "server_port": 8013,
                "http_port": 13013,
                "fastbone": True
            }
        }
    },
    "availability_graph": {
        "kant": ["cedar", "smith", "plato"],
        "smith": ["cedar", "kant", "plato"],
        "plato": ["cedar", "kant", "smith"],
        "cedar": ["kant", "smith", "plato"]
    },
    "tasks_path": "//home/ignat/tasks",
    "lock_path": "//home/ignat/tasks_lock",
    "proxy": "kant.yt.yandex.net",
    "token": "93b4cacc08aa4538a79a76c21e99c0fb"}
    
def main():
    parser = argparse.ArgumentParser(description="Transfer manager.")
    parser.add_argument("--config")
    args = parser.parse_args()

    if args.config is not None:
        config = json.load(open(args.config))
    else:
        config = DEFAULT_CONFIG

    app = Application(config)
    app.run(host="localhost", port=5000, debug=True, use_reloader=False)

if __name__ == "__main__":
    main()
