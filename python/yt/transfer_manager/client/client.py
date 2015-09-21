from yt.common import YtError
from yt.wrapper.common import get_value, require, update, run_with_retries
from yt.wrapper.http import RETRIABLE_ERRORS, get_token
import yt.logger as logger

import yt.packages.requests as requests
import yt.packages.simplejson as json

import time

TM_BACKEND_URL = "http://transfer-manager.yt.yandex.net/api/v1"

TM_HEADERS = {
    "Accept-Type": "application/json",
    "Content-Type": "application/json"
}

def _raise_for_status(response):
    if response.status_code == 200:
        return

    raise YtError(**response.json())

class TransferManager(object):
    def __init__(self, url=None, token=None, http_request_timeout=10000,
                 enable_read_retries=True, read_retry_count=5):
        self.backend_url = get_value(url, TM_BACKEND_URL)
        self.token = get_value(token, get_token())

        self.http_request_timeout = http_request_timeout

        self.enable_read_retries = enable_read_retries
        self.read_retry_count = read_retry_count

    def add_task(self, source_cluster, source_table, destination_cluster, destination_table=None, params=None,
                 sync=False, poll_period=None):
        params = get_value(params, {})
        poll_period = get_value(poll_period, 5)

        data = {
            "source_cluster": source_cluster,
            "source_table": source_table,
            "destination_cluster": destination_cluster,
        }
        if destination_table is not None:
            data["destination_table"] = destination_table
        update(data, params)

        task_id = self._make_post_request(self.backend_url + "/tasks/", data=json.dumps(data)).content

        logger.info("Transfer task started: %s", task_id)

        if sync:
            self._wait_for_tasks([task_id], poll_period)

        return task_id

    def add_tasks(self, source_cluster, source_pattern, destination_cluster, destination_pattern, **kwargs):
        src_dst_pairs = self._get_src_dst_pairs(source_cluster, source_pattern,
                                                destination_cluster, destination_pattern)

        sync = kwargs.pop("sync", False)
        poll_period = kwargs.pop("poll_period", 5)

        tasks = []
        for source_table, destination_table in src_dst_pairs:
            task = self.add_task(source_cluster, source_table, destination_cluster, destination_table,
                                 sync=False, **kwargs)
            tasks.append(task)

        if sync:
            self._wait_for_tasks(tasks, poll_period)

        return tasks

    def abort_task(self, task_id):
        self._make_post_request("{0}/tasks/{1}/abort/".format(self.backend_url, task_id))

    def restart_task(self, task_id):
        self._make_post_request("{0}/tasks/{1}/restart/".format(self.backend_url, task_id))

    def get_task_info(self, task_id):
        return self._make_get_request("{0}/tasks/{1}/".format(self.backend_url, task_id)).json()

    def get_tasks(self):
        return self._make_get_request("{0}/tasks/".format(self.backend_url)).json()

    def get_backend_config(self):
        return self._make_get_request("{0}/config/".format(self.backend_url)).json()

    def _get_src_dst_pairs(self, source_cluster, source_table, destination_cluster, destination_table):
        data = {
            "source_cluster": source_cluster,
            "source_pattern": source_table,
            "destination_cluster": destination_cluster,
            "destination_pattern": destination_table
        }

        return self._make_post_request(self.backend_url + "/match/", data=json.dumps(data)).json()

    def _make_get_request(self, url):
        def make_request():
            return requests.get(url, headers=TM_HEADERS, timeout=self.http_request_timeout)

        if not self.enable_read_retries:
            response = make_request()
        else:
            response = run_with_retries(make_request,
                                        exceptions=RETRIABLE_ERRORS)

        _raise_for_status(response)
        return response

    def _make_post_request(self, url, **kwargs):
        require(self.token is not None, YtError("YT token is not specified"))

        post_headers = update(TM_HEADERS, {"Authorization": "OAuth " + self.token})
        response = requests.post(url, headers=post_headers, timeout=self.http_request_timeout, **kwargs)

        _raise_for_status(response)
        return response

    def _wait_for_tasks(self, tasks, poll_period):
        for task in tasks:
            while True:
                logger.info("Waiting for task %s", task)
                state = self.get_task_info(task)["state"]
                if state == "completed":
                    logger.info("Task %s completed", task)
                    break
                if state == "aborted":
                    raise YtError("Task {0} was aborted".format(task))
                if state == "failed":
                    raise YtError("Task {0} failed. Use get_task_info for more info".format(task))

                time.sleep(poll_period)

