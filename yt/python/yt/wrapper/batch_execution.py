import weakref
from .batch_client import BatchClient
from .batch_response import BatchResponse
from .common import chunk_iter_list, get_value
from .config import get_config, get_option, get_client_state
from .driver import get_api_version
from .errors import YtError, YtResponseError
from .etc_commands import execute_batch
from .format import create_format
from .http_helpers import get_retriable_errors
from .retries import Retrier, default_chaos_monkey

from copy import deepcopy


class YtBatchRequestFailedError(YtError):
    """Batch request failed error.
       Can be raised if at least one request in batch failed."""
    pass


class BatchRequestRetrier(Retrier):
    def __init__(self, tasks, responses, max_batch_size, concurrency, client=None):
        retry_config = get_config(client)["batch_requests_retries"]
        request_timeout = get_config(client)["proxy"]["request_timeout"]
        chaos_monkey_enable = get_option("_ENABLE_HEAVY_REQUEST_CHAOS_MONKEY", client)
        super(BatchRequestRetrier, self).__init__(retry_config=retry_config,
                                                  timeout=request_timeout,
                                                  exceptions=get_retriable_errors() + (YtBatchRequestFailedError,),
                                                  chaos_monkey=default_chaos_monkey(chaos_monkey_enable))
        self._tasks = tasks
        self._responses = responses
        self._max_batch_size = max_batch_size
        self._concurrency = concurrency
        self._client = client

    def action(self):
        for tasks, responses in zip(chunk_iter_list(self._tasks, self._max_batch_size),
                                    chunk_iter_list(self._responses, self._max_batch_size)):
            results = execute_batch(tasks, concurrency=self._concurrency, client=self._client)
            if get_api_version(self._client) == "v4":
                results = results["results"]
            for result, response in zip(results, responses):
                response.set_result(result)

        tasks = []
        responses = []
        for task, response in zip(self._tasks, self._responses):
            if not response.is_ok():
                error = YtResponseError(response.get_error())
                if isinstance(error, get_retriable_errors()):
                    tasks.append(task)
                    responses.append(response)

        self._tasks = tasks
        self._responses = responses
        if tasks:
            raise YtBatchRequestFailedError()


class BatchExecutor(object):
    def __init__(self, raise_errors=False, max_batch_size=None, concurrency=None, client=None):
        self._client = client
        self._tasks = []
        self._responses = []
        self._batch_client = None
        self._raise_errors = raise_errors
        self._max_batch_size = get_value(max_batch_size, get_config(self._client)["max_batch_size"])
        self._concurrency = get_value(concurrency, get_config(self._client)["execute_batch_concurrency"])

    def get_client(self):
        config = deepcopy(get_config(self._client))
        batch_client = BatchClient(self, client_state=get_client_state(self._client), config=config)
        # circular reference: BatchClient._batch_executor <-> BatchExecutor._batch_client
        # object can live up to 1 minute (and hold opened socket) after "destruction". So make ref weak.
        self._batch_client = weakref.proxy(batch_client)
        return batch_client

    def add_task(self, command, parameters, input=None):
        task = {"command": command, "parameters": parameters}

        if input is not None:
            if "input_format" in parameters:
                # COMPAT(ignat): compatibility with older versions of driver.
                # We want to check for structured input here, but we have no such information at this point.
                if command not in ("get_in_sync_replicas",):
                    task["input"] = create_format(parameters["input_format"]).loads_node(input)
                    del parameters["input_format"]
                else:
                    task["input"] = input
            else:
                # Expects yson string if input format is not specified.
                # TODO(ignat): fix usages and remove this case.
                format = create_format("yson")
                task["input"] = format.loads_node(input)

        self._tasks.append(task)
        self._responses.append(BatchResponse())
        return self._responses[-1]

    def commit_batch(self):
        retrier = BatchRequestRetrier(tasks=self._tasks,
                                      responses=self._responses,
                                      max_batch_size=self._max_batch_size,
                                      concurrency=self._concurrency,
                                      client=self._client)
        try:
            retrier.run()
        except YtBatchRequestFailedError:
            pass

        if self._raise_errors:
            errors = []
            for response in self._responses:
                if not response.is_ok():
                    errors.append(response.get_error())
            if errors:
                raise YtBatchRequestFailedError("Batch request failed", inner_errors=errors)

        self._clear_tasks()

    def _clear_tasks(self):
        self._tasks = []
        self._responses = []
