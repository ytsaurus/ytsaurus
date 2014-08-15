#!/usr/bin/env python

import yt.logger as logger
from yt.tools.yamr import Yamr
from yt.tools.remote_copy_tools import \
    copy_yamr_to_yt_pull, \
    copy_yt_to_yamr_pull, \
    copy_yt_to_yamr_push, \
    run_operation_and_notify
from yt.wrapper.client import Yt
from yt.wrapper.common import generate_uuid
import yt.wrapper as yt

from flask import Flask, request, jsonify, Response, make_response

import os
import json
import time
import signal
import socket
import argparse
from copy import deepcopy
from datetime import datetime
from collections import defaultdict

from threading import RLock, Thread
from multiprocessing import Process, Queue

class RequestFailed(yt.YtError):
    pass

class IncorrectTokenError(RequestFailed):
    pass

def now():
    return str(datetime.utcnow().isoformat() + "Z")

def create_pool(yt_client, destination_cluster_name):
    pool_name = "transfer_" + destination_cluster_name
    pool_path = "//sys/pools/transfer_manager/" + pool_name
    if not yt_client.exists(pool_path):
        yt_client.create("map_node", pool_path, recursive=True, ignore_existing=True)
    yt_client.set(pool_path + "/@resource_limits", {"user_slots": 200})
    yt_client.set(pool_path + "/@mode", "fifo")
    return pool_path

def get_import_pool(mr_client, yt_client):
    if yt_client._import_pool is None:
        return create_pool(yt_client, mr_client._name)
    else:
        return yt_client._import_pool

class Task(object):
    def __init__(self, source_cluster, source_table, destination_cluster, destination_table, creation_time, id, state,
                 token="", user="unknown", mr_user=None, error=None, finish_time=None, progress=None):
        self.source_cluster = source_cluster
        self.source_table = source_table
        self.destination_cluster = destination_cluster
        self.destination_table = destination_table

        self.creation_time = creation_time
        self.finish_time = finish_time
        self.state = state
        self.id = id
        self.user = user
        self.mr_user = mr_user
        self.error = error
        self.token = token
        self.progress = progress

    def get_queue_id(self):
        return self.source_cluster, self.destination_cluster

    def dict(self, hide_token=False):
        result = deepcopy(self.__dict__)
        if hide_token:
            del result["token"]
        for key in result.keys():
            if result[key] is None:
                del result[key]
        return result

class Application(object):
    ERROR_BUFFER_SIZE = 2 ** 16

    def __init__(self, config):
        self._daemon = Flask(__name__)

        self._config = config
        self._mutex = RLock()
        self._yt = Yt(config["proxy"])
        self._yt.token = config["token"]

        self._load_config(config)

        message_queue = Queue()
        self._lock_path = os.path.join(config["path"], "lock")
        self._yt.create("map_node", self._lock_path, ignore_existing=True)
        self._lock_thread = Process(target=self._take_lock, args=(message_queue,))
        self._lock_thread.start()
        if not message_queue.get(timeout=5.0):
            raise yt.YtError("Cannot take lock " + self._lock_path)
        self._yt.set_attribute(config["path"], "address", socket.getfqdn())

        self._acl_path = os.path.join(config["path"], "acl")
        self._yt.create("map_node", self._acl_path, ignore_existing=True)

        self._add_rule("/", 'main', methods=["GET"])
        self._add_rule("/tasks/", 'get_tasks', methods=["GET"])
        self._add_rule("/tasks/", 'add', methods=["POST"])
        self._add_rule("/tasks/<id>/", 'get_task', methods=["GET"])
        self._add_rule("/tasks/<id>/abort/", 'abort', methods=["POST"])
        self._add_rule("/tasks/<id>/restart/", 'restart', methods=["POST"])
        self._add_rule("/config/", 'config', methods=["GET"])

        self._task_processes = {}

        self._execution_thread = Thread(target=self._execute_tasks)
        self._execution_thread.daemon = True
        self._execution_thread.start()

    def _add_rule(self, rule, endpoint, methods):
        methods.append("OPTIONS")
        self._daemon.add_url_rule(rule, endpoint, self._process_exception(self._process_cors(Application.__dict__[endpoint], methods)), methods=methods)

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

    def _process_exception(self, func):
        def decorator(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except RequestFailed as error:
                logger.exception(yt.errors.format_error(error))
                return yt.errors.format_error(error), 400
            except Exception as error:
                logger.exception(error)
                return "Unknown error: " + error.message, 502

        return decorator

    def _take_lock(self, message_queue):
        try:
            with self._yt.PingableTransaction():
                try:
                    self._yt.lock(self._lock_path)
                    message_queue.put(True)
                except Exception as err:
                    logger.exception(yt.errors.format_error(err))
                    message_queue.put(True)
                    return

                # Sleep infinitely long
                time.sleep(2 ** 60)
        except KeyboardInterrupt:
            # Do not print backtrace in case of SIGINT
            pass

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
            elif type == "yamr":
                if "viewer" in options:
                    del options["viewer"]
                self._clusters[name] = Yamr(**options)
            else:
                raise yt.YtError("Incorrect cluster type " + options["type"])

            self._clusters[name]._name = name
            self._clusters[name]._type = type

        for name in config["availability_graph"]:
            if name not in self._clusters:
                raise yt.YtError("Incorrect availability graph, cluster {} is missing".format(name))
            for neighbour in config["availability_graph"][name]:
                if neighbour not in self._clusters:
                    raise yt.YtError("Incorrect availability graph, cluster {} is missing".format(neighbour))

        self._availability_graph = config["availability_graph"]

        self._load_tasks(os.path.join(config["path"], "tasks"))

    def _load_tasks(self, tasks_path): #, archived_tasks_path):
        self._tasks_path = tasks_path
        if not self._yt.exists(self._tasks_path):
            self._yt.create("map_node", self._tasks_path)
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
            self._yt.set(os.path.join(self._tasks_path, id), self._tasks[id].dict())

    def _get_token(self, authorization_header):
        words = authorization_header.split()
        if len(words) != 2 or words[0].lower() != "oauth":
            return None
        return words[1]

    def _get_token_and_user(self, authorization_header):
        token = self._get_token(request.headers.get("Authorization", ""))
        if token is None or token == "undefined":
            user = "guest"
            token = ""
        else:
            user = self._yt.get_user_name(token)
            if not user:
                raise IncorrectTokenError("Authorization token is incorrect: " + token)
        return token, user

    def _precheck(self, task):
        if task.source_cluster not in self._clusters:
            raise yt.YtError("Unknown cluster " + task.source_cluster)
        if task.destination_cluster not in self._clusters:
            raise yt.YtError("Unknown cluster " + task.destination_cluster)
        if task.source_cluster not in self._availability_graph or task.destination_cluster not in self._availability_graph[task.source_cluster]:
            raise yt.YtError("Cluster {} not available from {}".format(task.destination_cluster, task.source_cluster))

        source_client = self._clusters[task.source_cluster]
        destination_client = self._clusters[task.destination_cluster]
        if source_client._type == "yamr" and source_client.is_empty(task.source_table) or \
           source_client._type == "yt" and (not source_client.exists(task.source_table) or source_client.get_attribute(task.source_table, "row_count") == 0):
            raise yt.YtError("Source table {} is empty".format(task.source_table))

        if source_client._type == "yt" and destination_client._type == "yamr":
            keys = list(source_client.read_table(yt.TablePath(task.source_table, end_index=1, simplify=False), format=yt.JsonFormat(), raw=False).next())
            if set(keys + ["subkey"]) != set(["key", "subkey", "value"]):
                raise yt.YtError("Keys in the source table must be a subset of ('key', 'subkey', 'value')")

        if destination_client._type == "yt":
            destination_dir = os.path.dirname(task.destination_table)
            if not os.path.exists(destination_dir):
                raise yt.YtError("Directory {} should exist".format(destination_dir))
            if destination_client.check_permission(task.user, "write", destination_dir)["action"] != "allow":
                raise yt.YtError("There is no permission to write to {}. Please log in.".format(task.destination_table))


    def _can_run(self, task):
        return not self._running_task_queues[task.get_queue_id()]

    def _execute_tasks(self):
        while True:
            with self._mutex:
                self._pending_tasks = filter(lambda id: self._tasks[id].state == "pending", self._pending_tasks)

                for id, (process, message_queue) in self._task_processes.items():
                    error = None
                    while not message_queue.empty():
                        message = None
                        try:
                            message = message_queue.get()
                        except Queue.Empty:
                            break
                        if message["type"] == "error":
                            assert not process.is_alive()
                            error = message["error"]
                        elif message["type"] == "operation_started":
                            self._tasks[id].progress["operations"].append(message["operation"])
                        else:
                            assert False, "Incorrect message type: " + message["type"]

                    if not process.is_alive():
                        self._tasks[id].finish_time = now()
                        if process.aborted:
                            pass
                        elif error is None:
                            self._change_task_state(id, "completed")
                        else:
                            self._change_task_state(id, "failed")
                            self._tasks[id].error = error

                        self._running_task_queues[self._tasks[id].get_queue_id()].remove(id)
                        del self._task_processes[id]

                for id in self._pending_tasks:
                    if not self._can_run(self._tasks[id]):
                        continue
                    self._running_task_queues[self._tasks[id].get_queue_id()].append(id)
                    self._change_task_state(id, "running")
                    self._tasks[id].progress = {"operations": []}
                    queue = Queue()
                    task_process = Process(target=lambda: self._execute_task(self._tasks[id], queue))
                    task_process.aborted = False
                    task_process.start()
                    self._task_processes[id] = (task_process, queue)

            time.sleep(1.0)

    def _execute_task(self, task, message_queue):
        logger.info("Executing task %s", task.id)
        try:
            logger.info("Making precheck")
            self._precheck(task)

            title = "Supervised by transfer task " + task.id
            task_spec = {"title": title, "transfer_task_id": task.id}

            if self._clusters[task.source_cluster]._type == "yt" and self._clusters[task.destination_cluster]._type == "yt":
                client = deepcopy(self._clusters[task.destination_cluster])
                client.token = task.token
                logger.info("Running YT -> YT remote copy operation")
                run_operation_and_notify(
                    message_queue,
                    client,
                    lambda client, strategy:
                        client.run_remote_copy(
                            task.source_table,
                            task.destination_table,
                            cluster_name=task.source_cluster,
                            network_name=self._clusters[task.source_cluster]._network,
                            spec=task_spec,
                            strategy=strategy))
            elif self._clusters[task.source_cluster]._type == "yt" and self._clusters[task.destination_cluster]._type == "yamr":
                if task.mr_user is None:
                    task.mr_user = "tmp"
                logger.info("Running YT -> YAMR remote copy")
                #copy_yt_to_yamr_pull(
                #    self._clusters[task.source_cluster],
                #    self._clusters[task.destination_cluster],
                #    task.source_table,
                #    task.destination_table,
                #    mr_user=task.mr_user,
                #    message_queue=message_queue)
                copy_yt_to_yamr_push(
                    self._clusters[task.source_cluster],
                    self._clusters[task.destination_cluster],
                    task.source_table,
                    task.destination_table,
                    mr_user=task.mr_user,
                    spec_template=task_spec,
                    token=task.token,
                    message_queue=message_queue)
            elif self._clusters[task.source_cluster]._type == "yamr" and self._clusters[task.destination_cluster]._type == "yt":
                if task.mr_user is None:
                    task.mr_user = "tmp"
                task_spec["pool"] = get_import_pool(self._clusters[task.source_cluster], self._clusters[task.destination_cluster])
                logger.info("Running YAMR -> YT remote copy")
                copy_yamr_to_yt_pull(
                    self._clusters[task.source_cluster],
                    self._clusters[task.destination_cluster],
                    task.source_table,
                    task.destination_table,
                    token=task.token,
                    mr_user=task.mr_user,
                    spec_template=task_spec,
                    message_queue=message_queue)
            elif self._clusters[task.source_cluster]._type == "yamr" and self._clusters[task.destination_cluster]._type == "yamr":
                if task.mr_user is None:
                    task.mr_user = "tmp"
                self._clusters[task.destination_cluster].remote_copy(
                    self._clusters[task.source_cluster].server,
                    task.source_table,
                    task.destination_table,
                    task.mr_user)
            else:
                raise Exception("Incorrect cluster types: {} source and {} destination".format(
                                self._clusters[task.source_cluster]._type,
                                self._clusters[task.destination_cluster]._type))
            logger.info("Task %s completed", task.id)
        except KeyboardInterrupt:
            pass
        except yt.YtError as error:
            logger.exception("Task {} failed with error {}".format(task.id, yt.errors.format_error(error)))
            message_queue.put({
                "type": "error",
                "error": error.simplify()
            })
        except Exception as error:
            logger.exception("Task {} failed with error {}".format(task.id, error.message))
            message_queue.put({
                "type": "error",
                "error": {
                    "message": error.message,
                    "code": 1
                }
            })

    def _get_task_description(self, task):
        task_description = task.dict(hide_token=True)
        queue_index = 1
        with self._mutex:
            for id in self._pending_tasks:
                if id == task.id:
                    task_description["queue_index"] = queue_index
                if self._tasks[id].get_queue_id() == task.get_queue_id():
                    queue_index += 1
        return task_description

    # Public interface
    def run(self, *args, **kwargs):
        self._daemon.run(*args, **kwargs)


    # Url handlers
    def main(self):
        return "This is YT transfer manager"

    def add(self):
        try:
            params = json.loads(request.data)
        except ValueError as error:
            raise RequestFailed("Cannot parse json from body '{}'".format(request.data), inner_errors=[yt.YtError(error.message)])

        required_parameters = set(["source_cluster", "source_table", "destination_cluster", "destination_table"])
        if not set(params) >= required_parameters:
            raise RequestFailed("All required parameters ({}) must be presented Incorrect parameters".format(", ".join(required_parameters)))

        token, user = self._get_token_and_user(request.headers.get("Authorization", ""))

        try:
            task = Task(id=generate_uuid(), creation_time=now(), user=user, token=token, state="pending", **params)
        except TypeError as error:
            raise RequestFailed("Cannot create task", inner_errors=[yt.YtError(error.message)])

        try:
            self._precheck(task)
        except yt.YtError as error:
            raise RequestFailed("Precheck failed", inner_errors=[error])

        if not request.args.get("dry_run", False):
            with self._mutex:
                self._tasks[task.id] = task
                self._pending_tasks.append(task.id)

            self._yt.set(os.path.join(self._tasks_path, task.id), task.dict())

        return task.id

    def abort(self, id):
        if id not in self._tasks:
            raise RequestFailed("Unknown task " + id)

        _, user = self._get_token_and_user(request.headers.get("Authorization", ""))
        if self._tasks[id].user != user and \
           self._yt.check_permission(user, "administer", self._acl_path)["action"] != "allow":
            raise RequestFailed("There is no permission to abort task.")

        if id in self._task_processes:
            process, _ = self._task_processes[id]
            process.aborted = True

            os.kill(process.pid, signal.SIGINT)
            time.sleep(0.5)
            if process.is_alive():
                process.terminate()

        if self._tasks[id].state not in ["aborted", "completed", "failed"]:
            with self._mutex:
                self._change_task_state(id, "aborted")

        return ""

    def restart(self, id):
        if id not in self._tasks:
            raise RequestFailed("Unknown task " + id)

        _, user = self._get_token_and_user(request.headers.get("Authorization", ""))
        if self._tasks[id].user != user and \
           self._yt.check_permission(user, "administer", self._acl_path)["action"] != "allow":
            raise RequestFailed("There is no permission to abort task.")

        if self._tasks[id].state not in ["completed", "aborted", "failed"]:
            raise RequestFailed("Cannot restart task in state " + self._tasks[id].state)

        self._tasks[id].state = "pending"
        self._tasks[id].creation_time = now()
        self._tasks[id].error = None
        self._pending_tasks.append(id)

        return ""

    def get_task(self, id):
        if id not in self._tasks:
            return "Unknown task " + id, 400

        return jsonify(**self._get_task_description(self._tasks[id]))

    def get_tasks(self):
        user = request.args.get("user")
        tasks = self._tasks.values()
        if user is not None:
            tasks = [task.user == user for task in tasks]

        return Response(json.dumps(map(self._get_task_description, tasks)), mimetype='application/json')

    def config(self):
        return jsonify(self._config)

DEFAULT_CONFIG = {
    "clusters": {
        "kant": {
            "type": "yt",
            "options": {
                "proxy": "kant.yt.yandex.net",
            },
            "remote_copy_network": "fastbone",
            "mr_import_pool": "import_restricted"
        },
        "smith": {
            "type": "yt",
            "options": {
                "proxy": "smith.yt.yandex.net",
            },
            # TODO: support it by client, move to options.
            "remote_copy_network": "fastbone",
            "mr_import_pool": "import_restricted"
        },
        "plato": {
            "type": "yt",
            "options": {
                "proxy": "plato.yt.yandex.net"
            },
            "remote_copy_network": "fastbone",
            "mr_import_pool": "import_restricted"
        },
        "cedar": {
            "type": "yamr",
            "options": {
                "server": "cedar00.search.yandex.net",
                "opts": "MR_NET_TABLE=ipv6",
                "binary": "/opt/cron/tools/mapreduce",
                "server_port": 8013,
                "http_port": 13013,
                "fastbone": True,
                "viewer": "https://specto.yandex.ru/cedar-viewer/"
            }
        },
        "betula": {
            "type": "yamr",
            "options": {
                "server": "betula00.yandex.ru",
                "binary": "/opt/cron/tools/betula/mapreduce",
                "server_port": 8013,
                "http_port": 13013,
                "fastbone": True,
                "viewer": "https://specto.yandex.ru/betula-viewer/"
            }
        }
    },
    "availability_graph": {
        "kant": ["cedar", "betula", "smith", "plato"],
        "smith": ["cedar", "betula", "kant", "plato"],
        "plato": ["cedar", "betula", "kant", "smith"],
        "cedar": ["betula", "kant", "smith", "plato"],
        "betula": ["cedar", "kant", "smith", "plato"]
    },
    "path": "//home/ignat/transfer_manager_test",
    "proxy": "kant.yt.yandex.net",
    "token": "93b4cacc08aa4538a79a76c21e99c0fb",
    "port": 5010
}

def main():
    parser = argparse.ArgumentParser(description="Transfer manager.")
    parser.add_argument("--config")
    args = parser.parse_args()

    if args.config is not None:
        config = json.load(open(args.config))
    else:
        config = DEFAULT_CONFIG

    app = Application(config)
    app.run(host="localhost", port=config["port"], debug=True, use_reloader=False)

if __name__ == "__main__":
    main()
