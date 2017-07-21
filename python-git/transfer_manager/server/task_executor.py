import yt.packages.requests as requests

import yt.json as json

from cherrypy import wsgiserver
from flask import Flask, request

import os
import sys
import prctl
import signal
import logging
import traceback
import subprocess
from collections import defaultdict, deque
from multiprocessing import Process
import cPickle as pickle

logger = logging.getLogger("TM.executor_process")

class TaskExecutorApplication(object):
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._daemon = Flask(__name__)
        self._processes = {}
        self._messages = defaultdict(deque)
        self._add_rule("/start/", "start_task", methods=["POST"])
        self._add_rule("/poll/<task_id>/", "poll_task", methods=["GET"])
        self._add_rule("/remove/<task_id>/", "remove_task", methods=["POST"])
        self._add_rule("/get_messages/<task_id>/", "get_messages_of_task", methods=["GET"])
        self._add_rule("/put_message/<task_id>/", "put_message_from_task", methods=["POST"])

    def _add_rule(self, rule, endpoint, methods):
        func = lambda *args, **kwargs: TaskExecutorApplication.__dict__[endpoint](self, *args, **kwargs)
        decorated_func = self._process_exception(func)
        self._daemon.add_url_rule(rule, endpoint, decorated_func, methods=methods)

    def _process_exception(self, func):
        def decorator(*args, **kwargs):
            logger.info("Received request %s (%r, %r)", request.url, args, kwargs)
            try:
                return func(*args, **kwargs)
            except Exception:
                logger.exception("Error while processing request")
                return json.dumps({"code": 1, "message": "Unknown error: " + traceback.format_exc()}), 404

        return decorator

    def start_task(self):
        params = json.loads(request.data)

        module_dir_name = os.path.dirname(os.path.realpath(__file__))
        executor_path = os.path.join(module_dir_name, "execute_task_script.py")

        proc = subprocess.Popen([
                                    sys.executable, executor_path,
                                    "--config-path", params["config_path"],
                                    "--task-executor-address", params["task_executor_address"]
                                ],
                                stdin=subprocess.PIPE,
                                stderr=sys.stderr,
                                preexec_fn=lambda: prctl.set_pdeathsig(signal.SIGINT),
                                close_fds=True)
        proc.stdin.write(params["input"])
        proc.stdin.close()

        self._processes[params["task_id"]] = proc

        return str(proc.pid)

    def poll_task(self, task_id):
        return json.dumps(self._processes[task_id].poll())

    def remove_task(self, task_id):
        del self._processes[task_id]
        return ""

    def get_messages_of_task(self, task_id):
        messages = self._messages[task_id]
        result = []
        while messages:
            result.append(messages.popleft())
        return pickle.dumps(result)

    def put_message_from_task(self, task_id):
        self._messages[task_id].append(pickle.loads(request.data))
        return ""

    def run(self, *args, **kwargs):
        # Debug version
        #self._daemon.run(host=self._host, port=self._port, debug=True)
        dispatcher = wsgiserver.WSGIPathInfoDispatcher({'/': self._daemon})
        # NB: TaskExecutorApplication has shared state, all methods should be implemented in thread-safe manner.
        server = wsgiserver.CherryPyWSGIServer((self._host, self._port), dispatcher, numthreads=5)
        try:
           server.start()
        except KeyboardInterrupt:
           server.stop()


class TaskExecutorProcess(Process):
    def __init__(self, config):
        super(TaskExecutorProcess, self).__init__()
        self._config = config
        self.address = "http://localhost:{0}/".format(self._config["port"])

    def run(self):
        self._app = TaskExecutorApplication(host="localhost", port=self._config["port"])
        self._app.run()

class TaskProcessClient(object):
    def __init__(self, address, logger):
        self._address = address
        self._logger = logger

    def start(self, task_id, config_path, input):
        params = {"task_id": task_id, "config_path": config_path, "task_executor_address": self._address, "input": input}
        rsp = requests.post(self._address + "start/", data=json.dumps(params))
        self._raise_for_status(rsp)
        self.pid = int(rsp.content)
        self.task_id = task_id

    def poll(self):
        rsp = requests.get(self._address + "poll/{0}/".format(self.task_id))
        self._raise_for_status(rsp)
        return rsp.json()

    def remove(self):
        rsp = requests.post(self._address + "remove/{0}/".format(self.task_id))
        self._raise_for_status(rsp)

    def get_messages(self):
        rsp = requests.get(self._address + "get_messages/{0}/".format(self.task_id))
        self._raise_for_status(rsp)
        return pickle.loads(rsp.content)

    def _raise_for_status(self, rsp):
        if rsp.status_code / 100 != 2:
            if rsp.status_code == 404:
                self._logger.error("Failed response from task executor: %s", rsp.content)
            rsp.raise_for_status()

class MessageWriter(object):
    def __init__(self, address, task_id):
        self.address = address
        self.task_id = task_id

    def put(self, obj):
        rsp = requests.post(self.address + "put_message/{0}/".format(self.task_id), data=pickle.dumps(obj))
        rsp.raise_for_status()

