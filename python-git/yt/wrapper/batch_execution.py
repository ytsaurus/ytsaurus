from .batch_client import BatchClient
from .batch_response import BatchResponse
from .common import chunk_iter_list, get_value
from .config import get_config, get_client_state
from .errors import YtError
from .etc_commands import execute_batch

import yt.yson as yson
from yt.packages.six.moves import zip as izip

from copy import deepcopy

class YtBatchRequestFailedError(YtError):
    pass

class BatchExecutor(object):
    def __init__(self, raise_errors=False, max_batch_size=None, client=None):
        self._client = client
        self._tasks = []
        self._responses = []
        self._batch_client = None
        self._raise_errors = raise_errors
        self._max_batch_size = get_value(max_batch_size, get_config(self._client)["max_batch_size"])

    def get_client(self):
        config = deepcopy(get_config(self._client))
        self._batch_client = BatchClient(self, client_state=get_client_state(self._client), config=config)
        return self._batch_client

    def add_task(self, command, parameters, input=None):
        task = {"command": command, "parameters": parameters}

        if input is not None:
            task["input"] = yson.loads(input)

        self._tasks.append(task)
        self._responses.append(BatchResponse())
        return self._responses[-1]

    def commit_batch(self):
        for tasks, responses in izip(chunk_iter_list(self._tasks, self._max_batch_size),
                                     chunk_iter_list(self._responses, self._max_batch_size)):
            results = execute_batch(tasks, client=self._client)
            for result, response in izip(results, responses):
                response._output = result.get("output")
                response._error = result.get("error")
                response._executed = True

        if self._raise_errors:
            errors = []
            for response in self._responses:
                if response.get_error():
                    errors.append(response.get_error())
            if errors:
                self._clear_tasks()
                raise YtBatchRequestFailedError("Batch request failed", inner_errors=errors)

        self._clear_tasks()

    def _clear_tasks(self):
        self._tasks = []
        self._responses = []

def create_batch_client(raise_errors=False, max_batch_size=None, client=None):
    """Creates client which supports batch executions."""
    batch_executor = BatchExecutor(raise_errors, max_batch_size, client)
    return batch_executor.get_client()
