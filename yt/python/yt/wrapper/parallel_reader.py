from .common import YtError, require, get_value
from .config import get_config, get_option
from .errors import YtChunkUnavailable
from .format import YtFormatReadError
from .heavy_commands import process_read_exception, _get_read_response
from .http_helpers import get_retriable_errors
from .lock_commands import lock
from .response_stream import ResponseStreamWithReadRow, EmptyResponseStream
from .retries import Retrier, default_chaos_monkey
from .transaction import Transaction, null_transaction_id, add_transaction_to_abort
from .thread_pool import ThreadPool
from .ypath import TablePath

import yt.logger as logger

from yt.common import join_exceptions

import builtins
import copy
import threading
import typing


DEFAULT_DATA_SIZE_PER_THREAD = 8 * 1024 * 1024
DEFAULT_SINGLE_CHUNK_SPLIT = 3


class ParallelReadRetrier(Retrier):
    def __init__(self, command_name, transaction_id, client):
        chaos_monkey_enabled = get_option("_ENABLE_READ_TABLE_CHAOS_MONKEY", client)
        retriable_errors = join_exceptions(get_retriable_errors(), YtChunkUnavailable, YtFormatReadError)
        retry_config = get_config(client)["read_retries"]
        timeout = get_config(client)["proxy"]["heavy_request_timeout"]

        super(ParallelReadRetrier, self).__init__(retry_config=retry_config,
                                                  timeout=timeout,
                                                  exceptions=retriable_errors,
                                                  chaos_monkey=default_chaos_monkey(chaos_monkey_enabled))
        self._command_name = command_name
        self._transaction_id = transaction_id
        self._client = client
        self._params = None

    def action(self):
        response = _get_read_response(self._command_name, self._params, self._transaction_id, self._client)
        response._process_error(response._get_response())
        return response.read()

    def except_action(self, exception, attempt):
        process_read_exception(exception)

    def run_read(self, params):
        self._params = params
        return self.run()


class ParallelReader(object):
    def __init__(self, command_name, transaction, params, prepare_params_func, prepare_meta_func, unordered, thread_count, client):
        self._command_name = command_name
        self._transaction = transaction
        self._prepare_params_func = prepare_params_func
        self._prepare_meta_func = prepare_meta_func
        self._unordered = unordered

        self._thread_data = {}
        self._pool = ThreadPool(thread_count, self.init_thread, (get_config(client), params),
                                max_queue_size=thread_count)

    def init_thread(self, client_config, params):
        from .client import YtClient
        ident = threading.current_thread().ident

        transaction_id = null_transaction_id if not self._transaction else self._transaction.transaction_id
        client = YtClient(config=client_config)
        self._thread_data[ident] = {"client": client,
                                    "params": copy.deepcopy(params),
                                    "retrier": ParallelReadRetrier(self._command_name, transaction_id, client)}

    def read_range(self, range):
        if self._transaction and not self._transaction.is_pinger_alive():
            raise YtError("Transaction pinger failed, read interrupted")

        ident = threading.current_thread().ident

        retrier = self._thread_data[ident]["retrier"]
        params = self._thread_data[ident]["params"]
        params = self._prepare_params_func(params, range)
        if "meta" in range:
            block = retrier.run_read(params)
            return self._prepare_meta_func(range["meta"]) + (len(block)).to_bytes(4, 'little') + block
        else:
            return retrier.run_read(params)

    def _read_iterator(self, ranges):
        if self._unordered:
            return self._pool.imap_unordered(self.read_range, ranges)
        return self._pool.imap(self.read_range, ranges)

    def read(self, ranges):
        for data in self._read_iterator(ranges):
            yield data

    def close(self, from_delete=False):
        self._pool.close()
        self._pool = None
        if self._transaction:
            if from_delete:
                add_transaction_to_abort(self._transaction)
                self._transaction = None
            else:
                self._transaction.abort()


def make_read_parallel_request(command_name, path, ranges, params, prepare_params_func,
                               prepare_meta_func, max_thread_count, unordered, response_parameters, client):
    if not ranges:
        return ResponseStreamWithReadRow(
            get_response=lambda: None,
            iter_content=iter(EmptyResponseStream()),
            close=lambda from_delete: None,
            process_error=lambda response: None,
            get_response_parameters=lambda: None)

    title = "Python wrapper: read {0}".format(str(TablePath(path, client=client)))
    transaction = None
    if get_config(client)["read_retries"]["create_transaction_and_take_snapshot_lock"]:
        transaction = Transaction(attributes={"title": title}, interrupt_on_failed=False, client=client)
    if response_parameters is None:
        response_parameters = {}

    thread_count = min(len(ranges), max_thread_count)
    try:
        if transaction:
            with Transaction(transaction_id=transaction.transaction_id, attributes={"title": title}, client=client):
                lock(path, mode="snapshot", client=client)

        reader = ParallelReader(command_name, transaction, params, prepare_params_func, prepare_meta_func, unordered, thread_count, client)
        iterator = reader.read(ranges)
        return ResponseStreamWithReadRow(
            get_response=lambda: None,
            iter_content=iterator,
            close=lambda from_delete: reader.close(from_delete),
            process_error=lambda response: None,
            get_response_parameters=lambda: response_parameters)
    except:  # noqa
        if transaction:
            transaction.abort()
        raise


def _prepare_ranges_for_parallel_read(
    offset: int,
    length: int,
    data_size: int,
    data_size_per_thread: int,
) -> typing.List[typing.Dict[str, typing.Tuple[int, int]]]:
    if not data_size_per_thread:
        data_size_per_thread = DEFAULT_DATA_SIZE_PER_THREAD

    offset = get_value(offset, 0)
    offset = min(offset, data_size)

    length = get_value(length, data_size)
    length = min(length, data_size - offset)

    result = []
    while offset < data_size and length > 0:
        range_size = min(data_size_per_thread, length)
        result.append({"range" : (offset, range_size)})
        offset += range_size
        length -= range_size

    return result


def _slice_row_ranges_for_parallel_read(
    ranges: typing.List[typing.Mapping],
    row_count: int,
    chunk_count: int,
    data_size: int,
    replication_factor: int,
    data_size_per_thread: typing.Union[int, None],
) -> typing.Tuple[typing.List[typing.Dict[str, typing.Tuple[int, int]]], int]:
    def _get_ranges(ranges, rows_per_task):
        result = []
        for range in ranges:
            if "exact" in range:
                require("row_index" in range["exact"], lambda: YtError('Invalid YPath: "row_index" not found'))
                lower_limit = range["exact"]["row_index"]
                upper_limit = lower_limit + 1
            else:
                if "lower_limit" in range:
                    require("row_index" in range["lower_limit"], lambda: YtError('Invalid YPath: "row_index" not found'))
                if "upper_limit" in range:
                    require("row_index" in range["upper_limit"], lambda: YtError('Invalid YPath: "row_index" not found'))

                lower_limit = 0 if "lower_limit" not in range else range["lower_limit"]["row_index"]
                upper_limit = row_count if "upper_limit" not in range else range["upper_limit"]["row_index"]

            for start in builtins.range(lower_limit, upper_limit, rows_per_task):
                end = min(start + rows_per_task, upper_limit)
                result.append({"range" : (start, end)})
        return result

    if row_count > 0:
        row_size = data_size / float(row_count)
    else:
        row_size = 1

    if data_size_per_thread:
        rows_per_task = max(
            int(data_size_per_thread / row_size),
            1,
        )
        result = _get_ranges(ranges, rows_per_task)
        logger.debug(f"Parallel read tasks count: {len(result)}, {row_size=}, {rows_per_task=}")
        return result, rows_per_task * row_size
    else:
        if chunk_count > 0 and replication_factor >= 0:
            data_size_per_thread = int(data_size / (chunk_count * replication_factor))
        else:
            data_size_per_thread = data_size
        rows_per_task_by_table = int(data_size_per_thread / row_size)
        rows_per_task_by_default = int(DEFAULT_DATA_SIZE_PER_THREAD / row_size)
        rows_per_task = max(
            rows_per_task_by_default,
            rows_per_task_by_table,
            1,
        )
        result = _get_ranges(ranges, rows_per_task)
        if len(result) == 1 and rows_per_task > DEFAULT_SINGLE_CHUNK_SPLIT:
            rows_per_task //= DEFAULT_SINGLE_CHUNK_SPLIT
            result = _get_ranges(ranges, rows_per_task)
        logger.debug(f"Parallel read tasks count: {len(result)}, {row_size=}, {rows_per_task=} ({rows_per_task_by_default}, {rows_per_task_by_table})")
        return result, rows_per_task * row_size
