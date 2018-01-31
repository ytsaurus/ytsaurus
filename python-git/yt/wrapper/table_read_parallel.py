from .common import update, get_value, remove_nones_from_dict, YtError, require
from .config import get_config, get_option, set_option
from .errors import YtChunkUnavailable
from .format import YtFormatReadError
from .heavy_commands import process_read_exception, _get_read_response
from .http_helpers import get_retriable_errors
from .lock_commands import lock
from .response_stream import ResponseStreamWithReadRow, EmptyResponseStream
from .retries import Retrier
from .transaction import Transaction, null_transaction_id
from .thread_pool import ThreadPool
from .ypath import TablePath

from yt.packages.six.moves import xrange

import copy
import threading

class ParallelReadRetrier(Retrier):
    def __init__(self, transaction_id, client):
        chaos_monkey_enabled = get_option("_ENABLE_READ_TABLE_CHAOS_MONKEY", client)
        retriable_errors = tuple(list(get_retriable_errors()) + [YtChunkUnavailable, YtFormatReadError])
        retry_config = {
            "count": get_config(client)["read_retries"]["retry_count"],
            "backoff": get_config(client)["retry_backoff"],
        }
        retry_config = update(copy.deepcopy(get_config(client)["read_retries"]), remove_nones_from_dict(retry_config))
        timeout = get_value(get_config(client)["proxy"]["heavy_request_retry_timeout"],
                            get_config(client)["proxy"]["heavy_request_timeout"])

        super(ParallelReadRetrier, self).__init__(retry_config=retry_config,
                                                  timeout=timeout,
                                                  exceptions=retriable_errors,
                                                  chaos_monkey_enable=chaos_monkey_enabled)
        self._transaction_id = transaction_id
        self._client = client
        self._params = None

    def action(self):
        response = _get_read_response("read_table", self._params, self._transaction_id, self._client)
        response._process_error(response._get_response())
        return response.read()

    def except_action(self, exception, attempt):
        process_read_exception(exception)

    def run_read(self, params):
        self._params = params
        return self.run()

class TableReader(object):
    def __init__(self, transaction, params, unordered, thread_count, client):
        self._thread_data = {}
        self._unordered = unordered
        self._transaction = transaction
        self._pool = ThreadPool(thread_count, self.init_thread, (get_config(client), params),
                                max_queue_size=thread_count)

    def init_thread(self, client_config, params):
        from .client import YtClient
        ident = threading.current_thread().ident

        transaction_id = null_transaction_id if not self._transaction else self._transaction.transaction_id
        client = YtClient(config=client_config)
        self._thread_data[ident] = {"client": client,
                                    "params": copy.deepcopy(params),
                                    "retrier": ParallelReadRetrier(transaction_id, client)}

    def read_table_range(self, range):
        if self._transaction and not self._transaction.is_pinger_alive():
            raise YtError("Transaction pinger failed, read interrupted")

        ident = threading.current_thread().ident

        retrier = self._thread_data[ident]["retrier"]
        params = self._thread_data[ident]["params"]
        params["path"].attributes["ranges"] = [{"lower_limit": {"row_index": range[0]},
                                                "upper_limit": {"row_index": range[1]}}]
        return retrier.run_read(params)

    def _read_iterator(self, ranges):
        if self._unordered:
            return self._pool.imap_unordered(self.read_table_range, ranges)
        return self._pool.imap(self.read_table_range, ranges)

    def read(self, ranges):
        for data in self._read_iterator(ranges):
            yield data

    def close(self):
        self._pool.close()
        if self._transaction:
            self._transaction.abort()

def _slice_row_ranges(ranges, row_count, data_size, data_size_per_thread):
    result = []
    if row_count > 0:
        row_size = data_size / float(row_count)
    else:
        row_size = 1

    rows_per_thread = max(int(data_size_per_thread / row_size), 1)
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

        for start in xrange(lower_limit, upper_limit, rows_per_thread):
            end = min(start + rows_per_thread, upper_limit)
            result.append((start, end))

    return result

def make_read_parallel_request(path, attributes, params, unordered, response_parameters, client):
    row_count = attributes["row_count"]
    data_size = attributes["uncompressed_data_size"]
    if "ranges" not in path.attributes:
        path.attributes["ranges"] = [{"lower_limit": {"row_index": 0},
                                      "upper_limit": {"row_index": row_count}}]

    title = "Python wrapper: read {0}".format(str(TablePath(path, client=client)))
    transaction = None
    if get_config(client)["read_retries"]["create_transaction_and_take_snapshot_lock"]:
        transaction = Transaction(attributes={"title": title}, interrupt_on_failed=False, client=client)

    ranges = _slice_row_ranges(path.attributes["ranges"],
                               row_count,
                               data_size,
                               get_config(client)["read_parallel"]["data_size_per_thread"])

    if response_parameters is None:
        response_parameters = {}

    if not ranges:
        response_parameters["start_row_index"] = 0
        response_parameters["approximate_row_count"] = 0
        return ResponseStreamWithReadRow(
            get_response=lambda: None,
            iter_content=iter(EmptyResponseStream()),
            close=lambda: None,
            process_error=lambda response: None,
            get_response_parameters=lambda: None)

    response_parameters["start_row_index"] = ranges[0][0]
    response_parameters["approximate_row_count"] = sum(range[1] - range[0] for range in ranges)

    thread_count = min(len(ranges), get_config(client)["read_parallel"]["max_thread_count"])
    try:
        if transaction:
            with Transaction(transaction_id=transaction.transaction_id, attributes={"title": title}, client=client):
                lock(path, mode="snapshot", client=client)

        reader = TableReader(transaction, params, unordered, thread_count, client)
        iterator = reader.read(ranges)
        return ResponseStreamWithReadRow(
            get_response=lambda: None,
            iter_content=iterator,
            close=lambda: reader.close(),
            process_error=lambda response: None,
            get_response_parameters=lambda: response_parameters)

    except:
        if transaction:
            transaction.abort()
        raise
