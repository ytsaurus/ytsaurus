from yt.common import YtError
from yt.wrapper.common import get_value, require, update, run_with_retries, generate_uuid, bool_to_string
from yt.wrapper.http import get_retriable_errors, get_token
import yt.logger as logger

import yt.packages.requests as requests
import yt.packages.simplejson as json

import time
from copy import deepcopy

TM_BACKEND_URL = "http://transfer-manager.yt.yandex.net/api/v1"
TM_TASK_URL_PATTERN = "https://transfer-manager.yt.yandex-team.ru/task?id={id}&tab=details&backend={backend_tag}"

TM_HEADERS = {
    "Accept-Type": "application/json",
    "Content-Type": "application/json"
}

class YtTransferManagerUnavailableError(YtError):
    pass

def _raise_for_status(response):
    if response.status_code == 200:
        return

    if response.status_code == 500:
        if response.content:
            message = "Transfer Manager is not available: {0}".format(response.content)
        else:
            message = "Transfer manager is not available"

        raise YtTransferManagerUnavailableError(message)

    raise YtError(**response.json())

class TransferManager(object):
    def __init__(self, url=None, token=None, http_request_timeout=10000,
                 enable_retries=True, retry_count=6):
        backend_url = get_value(url, TM_BACKEND_URL)

        # Backend url can be specified in short form.
        if backend_url.startswith("http://"):
            self.backend_url = backend_url
        else:
            self.backend_url = "http://{0}".format(backend_url)

        self.token = get_value(token, get_token())

        self.http_request_timeout = http_request_timeout
        self.enable_retries = enable_retries
        self.retry_count = retry_count

        self._backend_config = self.get_backend_config()

    def add_task(self, source_cluster, source_table, destination_cluster, destination_table=None, params=None,
                 sync=False, poll_period=None, attached=False):
        params = get_value(params, {})
        poll_period = get_value(poll_period, 5)

        data = {
            "source_cluster": source_cluster,
            "source_table": source_table,
            "destination_cluster": destination_cluster,
        }
        if destination_table is not None:
            data["destination_table"] = destination_table
        if attached:
            params["lease_timeout"] = max(120, 2 * poll_period)

        update(data, params)

        task_id = self._make_request(
            "POST",
            self.backend_url + "/tasks/",
            is_mutating=True,
            data=json.dumps(data)).content

        logger.info("Transfer task started: %s", TM_TASK_URL_PATTERN.format(
            id=task_id, backend_tag=self._backend_config["backend_tag"]))

        if sync:
            self._wait_for_tasks([task_id], poll_period)

        return task_id

    def add_tasks(self, source_cluster, source_pattern, destination_cluster, destination_pattern, **kwargs):
        src_dst_pairs = self.match_src_dst_pattern(source_cluster, source_pattern,
                                                   destination_cluster, destination_pattern)

        sync = kwargs.pop("sync", False)
        poll_period = get_value(kwargs.pop("poll_period", None), 5)

        tasks = []
        for source_table, destination_table in src_dst_pairs:
            task = self.add_task(source_cluster, source_table, destination_cluster, destination_table,
                                 sync=False, **kwargs)
            tasks.append(task)

        if sync:
            self._wait_for_tasks(tasks, poll_period)

        return tasks

    def abort_task(self, task_id):
        self._make_request(
            "POST",
            "{0}/tasks/{1}/abort/".format(self.backend_url, task_id),
            is_mutating=True)

    def restart_task(self, task_id):
        self._make_request(
            "POST",
            "{0}/tasks/{1}/restart/".format(self.backend_url, task_id),
            is_mutating=True)

    def get_task_info(self, task_id):
        return self._make_request("GET", "{0}/tasks/{1}/".format(self.backend_url, task_id)).json()

    def get_tasks(self, user=None, fields=None):
        params = {}
        if user is not None:
            params["user"] = user
        if fields is not None:
            params["fields[]"] = deepcopy(fields)

        return self._make_request("GET", "{0}/tasks/".format(self.backend_url), params=params).json()

    def get_backend_config(self):
        return self._make_request("GET", "{0}/config/".format(self.backend_url)).json()

    def match_src_dst_pattern(self, source_cluster, source_table, destination_cluster, destination_table):
        data = {
            "source_cluster": source_cluster,
            "source_pattern": source_table,
            "destination_cluster": destination_cluster,
            "destination_pattern": destination_table
        }

        return self._make_request(
            "POST",
            self.backend_url + "/match/",
            is_mutating=False,
            data=json.dumps(data)).json()

    def _make_request(self, method, url, is_mutating=False, **kwargs):
        headers = kwargs.get("headers", {})
        update(headers, TM_HEADERS)

        if method == "POST":
            require(self.token is not None, YtError("YT token is not specified"))
            headers["Authorization"] = "OAuth " + self.token

        params = {}
        if is_mutating:
            params["mutation_id"] = generate_uuid()
            params["retry"] = bool_to_string(False)

        def except_action():
            if is_mutating:
                params["retry"] = bool_to_string(True)

        def make_request():
            update(headers, {"X-TM-Parameters": json.dumps(params)})
            response = requests.request(
                method,
                url,
                headers=headers,
                timeout=self.http_request_timeout / 1000.0,
                **kwargs)

            _raise_for_status(response)
            return response

        if self.enable_retries:
            retriable_errors = get_retriable_errors() + (YtTransferManagerUnavailableError,)
            return run_with_retries(make_request, self.retry_count, exceptions=retriable_errors,
                                    except_action=except_action)

        else:
            return make_request()

    def _wait_for_tasks(self, tasks, poll_period):
        remaining_tasks = deepcopy(tasks)
        aborted_task_count = 0
        failed_task_count = 0

        while True:
            tasks_to_remove = []
            logger.info("Waiting for tasks...")
            for task in remaining_tasks:
                state = self.get_task_info(task)["state"]
                if state == "completed":
                    logger.info("Task %s completed", task)
                elif state == "skipped":
                    logger.info("Task %s skipped", task)
                elif state == "aborted":
                    logger.warning("Task {0} was aborted".format(task))
                    aborted_task_count += 1
                elif state == "failed":
                    logger.warning("Task {0} failed. Use get_task_info for more info".format(task))
                    failed_task_count += 1
                else:
                    continue

                tasks_to_remove.append(task)

            for task in tasks_to_remove:
                remaining_tasks.remove(task)

            if not remaining_tasks:
                break

            time.sleep(poll_period)

        if aborted_task_count or failed_task_count:
            raise YtError("All tasks done but there are {0} failed and {1} aborted tasks"
                          .format(failed_task_count, aborted_task_count))
